(* Effect-based scheduler for Lwt-style promises (POC).

   See lwt_effects.mli for the rationale. Key points of the implementation:

   - A promise ['a t] is a mutable cell, either resolved or pending. A pending
     promise carries its waiters and an optional cancel action.
   - [bind] is direct-style: on a resolved promise it is a plain application; on
     a pending one it performs an [Await] effect, so the fiber's continuation is
     captured by the scheduler instead of being allocated as a callback closure.
   - The scheduler is a single run queue of ready thunks. When the queue is
     empty and the event loop still has registered events (timers or fds), one
     blocking iteration of {!Lwt_engine} is run; its callbacks resolve promises
     and re-fill the queue. A waiter never resumes a continuation directly: it
     only enqueues it, keeping resolution flat (no deep recursion). *)

type 'a t = { mutable st : 'a state }

and 'a state =
  | Return of 'a
  | Fail of exn
  | Pending of 'a pending

and 'a pending = {
  mutable waiters : (('a, exn) result -> unit) list;
    (* Most-recently-added first; each waiter runs once and only enqueues. *)
  mutable on_cancel : unit -> unit;
    (* Run when the promise is cancelled while pending (e.g. to stop an engine
       event). Defaults to doing nothing. *)
}

exception Canceled

(* ------------------------------------------------------------------ *)
(* Promise primitives                                                 *)
(* ------------------------------------------------------------------ *)

let new_pending () = { st = Pending { waiters = []; on_cancel = ignore } }

let fill (type a) (p : a t) (r : (a, exn) result) : unit =
  match p.st with
  | Pending pe ->
    p.st <- (match r with Ok v -> Return v | Error e -> Fail e);
    List.iter (fun w -> w r) (List.rev pe.waiters)
  | Return _ | Fail _ -> ()

let add_waiter (type a) (p : a t) (w : (a, exn) result -> unit) : unit =
  match p.st with
  | Pending pe -> pe.waiters <- w :: pe.waiters
  | Return v -> w (Ok v)
  | Fail e -> w (Error e)

let set_on_cancel (type a) (p : a t) (f : unit -> unit) : unit =
  match p.st with Pending pe -> pe.on_cancel <- f | Return _ | Fail _ -> ()

let cancel (type a) (p : a t) : unit =
  match p.st with
  | Pending pe ->
    pe.on_cancel ();
    fill p (Error Canceled)
  | Return _ | Fail _ -> ()

(* ------------------------------------------------------------------ *)
(* Scheduler state                                                    *)
(* ------------------------------------------------------------------ *)

let run_queue : (unit -> unit) Queue.t = Queue.create ()
let enqueue (f : unit -> unit) : unit = Queue.push f run_queue

(* Number of engine events (timers, fd waits) this scheduler is currently
   waiting for. We track it ourselves rather than reading Lwt_engine's counts:
   once Lwt_main has run, the engine permanently holds Lwt's own notification
   descriptor, which would otherwise make the scheduler block forever. *)
let outstanding = ref 0

(* Register a one-shot engine event for the pending promise [p]. When the event
   fires (or [p] is cancelled) the event is stopped, the outstanding count is
   decremented, and [p] is resolved with [result] (on a normal fire). *)
let setup_event (type a) (p : a t)
    (register : (Lwt_engine.event -> unit) -> Lwt_engine.event)
    (result : (a, exn) result) : unit =
  incr outstanding;
  let event = ref Lwt_engine.fake_event in
  let finished = ref false in
  let finish () =
    if not !finished then begin
      finished := true;
      decr outstanding;
      Lwt_engine.stop_event !event
    end
  in
  event := register (fun _ev -> finish (); fill p result);
  set_on_cancel p finish

(* ------------------------------------------------------------------ *)
(* Effects                                                            *)
(* ------------------------------------------------------------------ *)

type _ Effect.t += Await : 'a t -> ('a, exn) result Effect.t

(* Perform [Await] only when actually pending: resolved cases stay
   allocation-free and never touch the scheduler. *)
let await_result (type a) (p : a t) : (a, exn) result =
  match p.st with
  | Return v -> Ok v
  | Fail e -> Error e
  | Pending _ -> Effect.perform (Await p)

let await (type a) (p : a t) : a =
  match await_result p with Ok v -> v | Error e -> raise e

(* ------------------------------------------------------------------ *)
(* Constructors and combinators                                       *)
(* ------------------------------------------------------------------ *)

let return v = { st = Return v }
let fail e = { st = Fail e }
let return_unit = return ()

(* Apply the continuation of a bind, turning a synchronous exception into a
   rejected promise. This catch-all mirrors Lwt's monadic semantics: a [bind]
   never lets [f] raise into the scheduler. *)
let apply (f : 'a -> 'b t) (v : 'a) : 'b t =
  try f v with e -> { st = Fail e }

let bind (type a) (p : a t) (f : a -> 'b t) : 'b t =
  match p.st with
  | Return v -> apply f v
  | Fail e -> { st = Fail e }
  | Pending _ -> (
    match Effect.perform (Await p) with
    | Ok v -> apply f v
    | Error e -> { st = Fail e })

let map f p = bind p (fun v -> return (f v))
let ( >>= ) = bind
let ( >|= ) p f = map f p

(* [catch]/[try_bind] await the result of [f ()] and dispatch on success or
   rejection. A synchronous exception raised by [f] itself is also routed to the
   handler, mirroring Lwt. *)
let try_bind (f : unit -> 'a t) (g : 'a -> 'b t) (h : exn -> 'b t) : 'b t =
  let r = try await_result (f ()) with e -> Error e in
  match r with Ok v -> apply g v | Error e -> apply h e

let catch (f : unit -> 'a t) (h : exn -> 'a t) : 'a t =
  let r = try await_result (f ()) with e -> Error e in
  match r with Ok v -> return v | Error e -> apply h e

(* ------------------------------------------------------------------ *)
(* Running fibers                                                     *)
(* ------------------------------------------------------------------ *)

(* The single effect handler shared by every fiber. On [Await] it suspends the
   fiber and registers a waiter that re-enqueues the continuation once the
   awaited promise resolves. *)
let handler : (unit, unit) Effect.Deep.handler =
  let retc () = () in
  let exnc e = raise e in
  let effc : type b.
      b Effect.t -> ((b, unit) Effect.Deep.continuation -> unit) option =
    function
    | Await p ->
      Some
        (fun k ->
          add_waiter p (fun r -> enqueue (fun () -> Effect.Deep.continue k r)))
    | _ -> None
  in
  { retc; exnc; effc }

(* Start [body] as a fresh fiber under the shared handler. *)
let spawn (body : unit -> unit) : unit =
  enqueue (fun () -> Effect.Deep.match_with body () handler)

let async (f : unit -> 'a t) : 'a t =
  let p = new_pending () in
  spawn (fun () ->
    let r = try await_result (f ()) with e -> Error e in
    fill p r);
  p

let both a b = bind a (fun x -> bind b (fun y -> return (x, y)))

let choose (ps : 'a t list) : 'a t =
  let result = new_pending () in
  List.iter (fun p -> add_waiter p (fun r -> fill result r)) ps;
  result

let pick (ps : 'a t list) : 'a t =
  let result = new_pending () in
  List.iter
    (fun p ->
      add_waiter p (fun r ->
        match result.st with
        | Pending _ ->
          fill result r;
          List.iter (fun q -> if q != p then cancel q) ps
        | Return _ | Fail _ -> ()))
    ps;
  result

(* ------------------------------------------------------------------ *)
(* Yielding and timers                                                *)
(* ------------------------------------------------------------------ *)

let pause () : unit t =
  let p = new_pending () in
  enqueue (fun () -> fill p (Ok ()));
  p

let sleep (d : float) : unit t =
  if d <= 0. then pause ()
  else begin
    let p = new_pending () in
    setup_event p (fun cb -> Lwt_engine.on_timer d false cb) (Ok ());
    p
  end

(* ------------------------------------------------------------------ *)
(* The scheduler loop                                                 *)
(* ------------------------------------------------------------------ *)

let rec run_scheduler () : unit =
  match Queue.take_opt run_queue with
  | Some thunk ->
    thunk ();
    run_scheduler ()
  | None ->
    if !outstanding > 0 then begin
      Lwt_engine.iter true;
      run_scheduler ()
    end
(* else: nothing ready and no events outstanding: the scheduler is idle/done. *)

let run (type a) (main : unit -> a t) : a =
  let outcome = ref None in
  spawn (fun () ->
    let r = try await_result (main ()) with e -> Error e in
    outcome := Some r);
  run_scheduler ();
  match !outcome with
  | Some (Ok v) -> v
  | Some (Error e) -> raise e
  | None -> failwith "Lwt_effects.run: scheduler stalled before main resolved"

module Syntax = struct
  let ( let* ) = bind
  let ( let+ ) p f = map f p
  let ( and* ) = both
  let ( and+ ) = both
end

(* ------------------------------------------------------------------ *)
(* Non-blocking I/O on raw file descriptors                           *)
(* ------------------------------------------------------------------ *)

module Io = struct
  (* Suspend the current fiber until [fd] is readable / writable, using the
     engine. The promise's cancel action stops the engine event so a cancelled
     wait leaks nothing. *)
  let wait fd register =
    let p = new_pending () in
    setup_event p (fun cb -> register fd cb) (Ok ());
    await p

  let wait_readable fd = wait fd Lwt_engine.on_readable
  let wait_writable fd = wait fd Lwt_engine.on_writable

  let retry_eagain op fd wait_ready =
    let rec loop () =
      match op () with
      | result -> result
      | exception
          Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _)
        ->
        wait_ready fd;
        loop ()
    in
    loop ()

  let read fd buf off len =
    retry_eagain (fun () -> Unix.read fd buf off len) fd wait_readable

  let write fd buf off len =
    retry_eagain (fun () -> Unix.write fd buf off len) fd wait_writable

  let accept fd =
    retry_eagain (fun () -> Unix.accept fd) fd wait_readable

  let connect fd addr =
    match Unix.connect fd addr with
    | () -> ()
    | exception
        Unix.Unix_error ((Unix.EINPROGRESS | Unix.EWOULDBLOCK), _, _) -> (
      wait_writable fd;
      match Unix.getsockopt_error fd with
      | None -> ()
      | Some err -> raise (Unix.Unix_error (err, "connect", "")))
end
