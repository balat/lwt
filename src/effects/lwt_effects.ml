(* Effect-based scheduler for Lwt-style promises (POC).

   See lwt_effects.mli for the rationale. The key points of the implementation:

   - A promise ['a t] is a mutable cell, either resolved or pending with a list
     of waiters.
   - [bind] is direct-style: on a resolved promise it is a plain application; on
     a pending one it performs an [Await] effect, so the fiber's continuation is
     captured by the scheduler instead of being allocated as a callback closure.
   - The scheduler is a single run queue of ready thunks plus a timer list. A
     waiter never resumes a continuation directly: it only enqueues it, which
     keeps resolution flat (no deep recursion through callback chains). *)

type 'a t = { mutable st : 'a state }

and 'a state =
  | Return of 'a
  | Fail of exn
  | Pending of (('a, exn) result -> unit) list
    (* Waiters, most-recently-added first. Each waiter is run exactly once when
       the promise resolves; by convention a waiter only enqueues work. *)

(* ------------------------------------------------------------------ *)
(* Scheduler state                                                    *)
(* ------------------------------------------------------------------ *)

let run_queue : (unit -> unit) Queue.t = Queue.create ()

let enqueue (f : unit -> unit) : unit = Queue.push f run_queue

(* Pending timers, kept sorted by deadline (earliest first). *)
let timers : (float * (unit -> unit)) list ref = ref []

let add_timer (deadline : float) (action : unit -> unit) : unit =
  let rec insert = function
    | (d, _) :: _ as l when deadline < d -> (deadline, action) :: l
    | x :: l -> x :: insert l
    | [] -> [ (deadline, action) ]
  in
  timers := insert !timers

(* ------------------------------------------------------------------ *)
(* Resolution                                                         *)
(* ------------------------------------------------------------------ *)

(* [fill p r] resolves the pending promise [p] with the outcome [r] and runs its
   waiters. Resolving an already-resolved promise is a no-op (mirrors Lwt, where
   resolving a resolved resolver is an error we simply ignore here). *)
let fill (type a) (p : a t) (r : (a, exn) result) : unit =
  match p.st with
  | Pending waiters ->
    p.st <- (match r with Ok v -> Return v | Error e -> Fail e);
    (* Waiters were accumulated most-recent-first; run them oldest-first. *)
    List.iter (fun w -> w r) (List.rev waiters)
  | Return _ | Fail _ -> ()

let add_waiter (type a) (p : a t) (w : (a, exn) result -> unit) : unit =
  match p.st with
  | Pending waiters -> p.st <- Pending (w :: waiters)
  | Return v -> w (Ok v)
  | Fail e -> w (Error e)

(* ------------------------------------------------------------------ *)
(* Effects                                                            *)
(* ------------------------------------------------------------------ *)

type _ Effect.t += Await : 'a t -> ('a, exn) result Effect.t

(* Perform [Await] only when actually pending: the resolved cases stay
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
  let effc : type b. b Effect.t -> ((b, unit) Effect.Deep.continuation -> unit) option =
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
  let p = { st = Pending [] } in
  spawn (fun () ->
    let r = try await_result (f ()) with e -> Error e in
    fill p r);
  p

let both a b = bind a (fun x -> bind b (fun y -> return (x, y)))

let choose (ps : 'a t list) : 'a t =
  let result = { st = Pending [] } in
  List.iter (fun p -> add_waiter p (fun r -> fill result r)) ps;
  result

(* ------------------------------------------------------------------ *)
(* Yielding and timers                                                *)
(* ------------------------------------------------------------------ *)

let pause () : unit t =
  let p = { st = Pending [] } in
  enqueue (fun () -> fill p (Ok ()));
  p

let sleep (d : float) : unit t =
  if d <= 0. then pause ()
  else begin
    let p = { st = Pending [] } in
    add_timer (Unix.gettimeofday () +. d) (fun () -> fill p (Ok ()));
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
  | None -> (
    match !timers with
    | [] -> () (* nothing ready and no timers pending: scheduler is idle *)
    | (deadline, _) :: _ ->
      let delay = deadline -. Unix.gettimeofday () in
      if delay > 0. then Unix.sleepf delay;
      let now = Unix.gettimeofday () in
      let due, later =
        List.partition (fun (d, _) -> d <= now) !timers
      in
      timers := later;
      List.iter (fun (_, action) -> action ()) due;
      run_scheduler ())

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
