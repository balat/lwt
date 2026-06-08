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

type 'a t = { mutable st : 'a promise_state }

and 'a promise_state =
  | Fulfilled of 'a
  | Rejected of exn
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
    p.st <- (match r with Ok v -> Fulfilled v | Error e -> Rejected e);
    List.iter (fun w -> w r) (List.rev pe.waiters)
  | Fulfilled _ | Rejected _ -> ()

let add_waiter (type a) (p : a t) (w : (a, exn) result -> unit) : unit =
  match p.st with
  | Pending pe -> pe.waiters <- w :: pe.waiters
  | Fulfilled v -> w (Ok v)
  | Rejected e -> w (Error e)

let set_on_cancel (type a) (p : a t) (f : unit -> unit) : unit =
  match p.st with Pending pe -> pe.on_cancel <- f | Fulfilled _ | Rejected _ -> ()

let cancel (type a) (p : a t) : unit =
  match p.st with
  | Pending pe ->
    pe.on_cancel ();
    fill p (Error Canceled)
  | Fulfilled _ | Rejected _ -> ()

(* ------------------------------------------------------------------ *)
(* Scheduler state                                                    *)
(* ------------------------------------------------------------------ *)

(* A ready unit of work in the run queue. [Resume] avoids allocating a
   [fun () -> continue k r] closure for every suspension: the waiter pushes the
   continuation and its result directly. *)
type task =
  | Thunk of (unit -> unit)
  | Resume : 'b * ('b, unit) Effect.Deep.continuation -> task

(* Growable ring buffer used as the run queue. Stdlib.Queue allocates a list
   cell on every push; this allocates only when it has to grow. Capacity is kept
   a power of two so indexing uses [land] instead of [mod]. A sentinel fills
   freed slots so consumed continuations are not retained. *)
module Run_queue = struct
  let sentinel : task = Thunk ignore

  type t = { mutable a : task array; mutable head : int; mutable len : int }

  let create () = { a = Array.make 16 sentinel; head = 0; len = 0 }
  let is_empty q = q.len = 0

  let grow q =
    let cap = Array.length q.a in
    let a' = Array.make (2 * cap) sentinel in
    for i = 0 to q.len - 1 do
      a'.(i) <- q.a.((q.head + i) land (cap - 1))
    done;
    q.a <- a';
    q.head <- 0

  let push q x =
    if q.len = Array.length q.a then grow q;
    q.a.((q.head + q.len) land (Array.length q.a - 1)) <- x;
    q.len <- q.len + 1

  (* Caller must ensure [not (is_empty q)]; avoids allocating an option. *)
  let pop q =
    let x = q.a.(q.head) in
    q.a.(q.head) <- sentinel;
    q.head <- (q.head + 1) land (Array.length q.a - 1);
    q.len <- q.len - 1;
    x
end

let run_queue : Run_queue.t = Run_queue.create ()
let enqueue (f : unit -> unit) : unit = Run_queue.push run_queue (Thunk f)

(* Shared [Ok ()] outcome: events and [pause] all resolve unit promises, so
   there is no need to allocate a fresh [Ok ()] each time. *)
let ok_unit : (unit, exn) result = Ok ()

(* Number of engine events (timers, fd waits) this scheduler is currently
   waiting for. We track it ourselves rather than reading Lwt_engine's counts:
   once Lwt_main has run, the engine permanently holds Lwt's own notification
   descriptor, which would otherwise make the scheduler block forever. *)
let outstanding = ref 0

(* Register a one-shot engine event resolving the pending unit promise [p]. The
   fire callback uses its own [ev] argument to stop itself (no ref cell), and is
   guarded by [p]'s state so a double fire cannot double-decrement. Cancellation
   stops the captured event and decrements the count. *)
let setup_event (p : unit t)
    (register : (Lwt_engine.event -> unit) -> Lwt_engine.event) : unit =
  incr outstanding;
  let fire ev =
    match p.st with
    | Pending _ ->
      decr outstanding;
      Lwt_engine.stop_event ev;
      fill p ok_unit
    | Fulfilled _ | Rejected _ -> ()
  in
  let event = register fire in
  set_on_cancel p (fun () ->
    decr outstanding;
    Lwt_engine.stop_event event)

(* ------------------------------------------------------------------ *)
(* Effects                                                            *)
(* ------------------------------------------------------------------ *)

type _ Effect.t +=
  | Await : 'a t -> ('a, exn) result Effect.t
  | Yield : unit Effect.t

(* Reschedule the current fiber behind the others, without allocating a
   promise (unlike [await (pause ())]). *)
let yield () : unit = Effect.perform Yield

(* Perform [Await] only when actually pending: resolved cases stay
   allocation-free and never touch the scheduler. *)
let await_result (type a) (p : a t) : (a, exn) result =
  match p.st with
  | Fulfilled v -> Ok v
  | Rejected e -> Error e
  | Pending _ -> Effect.perform (Await p)

let await (type a) (p : a t) : a =
  match await_result p with Ok v -> v | Error e -> raise e

(* ------------------------------------------------------------------ *)
(* Constructors and combinators                                       *)
(* ------------------------------------------------------------------ *)

let return v = { st = Fulfilled v }
let fail e = { st = Rejected e }
let return_unit = return ()

(* Apply the continuation of a bind, turning a synchronous exception into a
   rejected promise. This catch-all mirrors Lwt's monadic semantics: a [bind]
   never lets [f] raise into the scheduler. *)
let apply (f : 'a -> 'b t) (v : 'a) : 'b t =
  try f v with e -> { st = Rejected e }

let bind (type a) (p : a t) (f : a -> 'b t) : 'b t =
  match p.st with
  | Fulfilled v -> apply f v
  | Rejected e -> { st = Rejected e }
  | Pending _ -> (
    match Effect.perform (Await p) with
    | Ok v -> apply f v
    | Error e -> { st = Rejected e })

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
          add_waiter p (fun r -> Run_queue.push run_queue (Resume (r, k))))
    | Yield -> Some (fun k -> Run_queue.push run_queue (Resume ((), k)))
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
        | Fulfilled _ | Rejected _ -> ()))
    ps;
  result

(* ------------------------------------------------------------------ *)
(* Yielding and timers                                                *)
(* ------------------------------------------------------------------ *)

let pause () : unit t =
  let p = new_pending () in
  enqueue (fun () -> fill p ok_unit);
  p

let sleep (d : float) : unit t =
  if d <= 0. then pause ()
  else begin
    let p = new_pending () in
    setup_event p (fun cb -> Lwt_engine.on_timer d false cb);
    p
  end

(* ------------------------------------------------------------------ *)
(* The scheduler loop                                                 *)
(* ------------------------------------------------------------------ *)

(* Idle-wait hook: called when the run queue is empty, to block until external
   work arrives. Returns [true] if it may have produced new work (the loop
   continues), [false] if there is nothing left to wait for (scheduler is done).
   A backend (the default Lwt_engine one, or io_uring) installs its own. *)
(* Block in the Lwt event loop while cooperating with Lwt itself: fulfil Lwt's
   paused promises and, when some are pending, poll without blocking (as
   Lwt_main does). This lets fibers [await] real [Lwt.t] promises — including
   Lwt_unix I/O — through {!of_lwt}. *)
let default_idle () : bool =
  if !outstanding > 0 then begin
    Lwt.wakeup_paused ();
    (* Servicing Lwt's paused promises may have produced ready work (a fiber
       resumed via [of_lwt]); only block in the loop if nothing is ready, and
       then only if no Lwt pause is still pending. *)
    if Run_queue.is_empty run_queue then
      Lwt_engine.iter (Lwt.paused_count () = 0);
    true
  end
  else false

(* Run at the start of [run] to reset back-end state (e.g. the I/O readiness
   table) that must not leak across independent scheduler runs. *)
let on_reset : (unit -> unit) ref = ref ignore

let idle_hook : (unit -> bool) ref = ref default_idle
let set_idle (f : unit -> bool) : unit = idle_hook := f

let rec run_scheduler () : unit =
  if Run_queue.is_empty run_queue then begin
    if !idle_hook () then run_scheduler ()
  end
  else begin
    (match Run_queue.pop run_queue with
    | Thunk f -> f ()
    | Resume (v, k) -> Effect.Deep.continue k v);
    run_scheduler ()
  end

let run (type a) (main : unit -> a t) : a =
  !on_reset ();
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
(* Interoperability with Lwt                                          *)
(* ------------------------------------------------------------------ *)

(* Both Lwt and this scheduler use the same {!Lwt_engine}, and [run]'s default
   idle hook drives Lwt's paused queue and event loop, so a real [Lwt.t] makes
   progress while a fiber waits on it. *)

let of_lwt (type a) (lwt : a Lwt.t) : a t =
  match Lwt.state lwt with
  | Lwt.Return v -> { st = Fulfilled v }
  | Lwt.Fail e -> { st = Rejected e }
  | Lwt.Sleep ->
    let p = new_pending () in
    incr outstanding;
    let finish r =
      match p.st with
      | Pending _ ->
        decr outstanding;
        fill p r
      | Fulfilled _ | Rejected _ -> ()
    in
    Lwt.on_any lwt (fun v -> finish (Ok v)) (fun e -> finish (Error e));
    set_on_cancel p (fun () ->
      decr outstanding;
      Lwt.cancel lwt);
    p

let await_lwt lwt = await (of_lwt lwt)

let to_lwt (type a) (p : a t) : a Lwt.t =
  match p.st with
  | Fulfilled v -> Lwt.return v
  | Rejected e -> Lwt.fail e
  | Pending _ ->
    let lwt, u = Lwt.wait () in
    add_waiter p (function
      | Ok v -> Lwt.wakeup u v
      | Error e -> Lwt.wakeup_exn u e);
    lwt

(* ------------------------------------------------------------------ *)
(* Lwt-compatibility layer                                            *)
(* ------------------------------------------------------------------ *)

(* Enough of Lwt's public API to compile code written against Lwt (modulo the
   implicit-concurrency semantics of [bind], which is fundamentally different —
   see the .mli). Combinators that Lwt makes non-blocking are implemented with
   [async] so they also return immediately. *)

type 'a state = Return of 'a | Fail of exn | Sleep
type 'a u = 'a t

let wait () =
  let p = new_pending () in
  (p, p)

let task () = wait ()
let wakeup (u : 'a u) v = fill u (Ok v)
let wakeup_exn (u : 'a u) e = fill u (Error e)
let wakeup_later = wakeup
let wakeup_later_exn = wakeup_exn

let state (type a) (p : a t) : a state =
  match p.st with
  | Fulfilled v -> Return v
  | Rejected e -> Fail e
  | Pending _ -> Sleep

let is_sleeping p = match p.st with Pending _ -> true | Fulfilled _ | Rejected _ -> false
let poll p =
  match p.st with Fulfilled v -> Some v | Rejected e -> raise e | Pending _ -> None
let of_result = function Ok v -> return v | Error e -> fail e

let fail_with msg = fail (Failure msg)
let fail_invalid_arg msg = fail (Invalid_argument msg)
let return_some x = return (Some x)
let return_ok x = return (Ok x)
let return_error e = return (Error e)
let return_true = return true
let return_false = return false

let wrap f = try return (f ()) with e -> fail e

let finalize f g =
  try_bind f
    (fun x -> bind (g ()) (fun () -> return x))
    (fun e -> bind (g ()) (fun () -> fail e))

(* Non-blocking like Lwt's: the awaiting happens in a spawned fiber. *)
let join (ps : unit t list) : unit t =
  async (fun () ->
    let err = ref None in
    List.iter
      (fun p ->
        match await_result p with
        | Ok () -> ()
        | Error e -> if !err = None then err := Some e)
      ps;
    match !err with None -> return_unit | Some e -> fail e)

let all (ps : 'a t list) : 'a list t = async (fun () -> return (List.map await ps))

let ready_values ps =
  List.filter_map
    (fun p -> match p.st with Fulfilled v -> Some v | Rejected _ | Pending _ -> None)
    ps

let nchoose (ps : 'a t list) : 'a list t =
  async (fun () ->
    ignore (await (choose ps));
    return (ready_values ps))

let npick (ps : 'a t list) : 'a list t =
  async (fun () ->
    ignore (await (choose ps));
    let res = ready_values ps in
    List.iter (fun p -> if is_sleeping p then cancel p) ps;
    return res)

let on_any p f g = add_waiter p (function Ok v -> f v | Error e -> g e)
let on_success p f = add_waiter p (function Ok v -> f v | Error _ -> ())
let on_failure p f = add_waiter p (function Ok _ -> () | Error e -> f e)
let on_termination p f = add_waiter p (fun _ -> f ())

let on_cancel p f =
  match p.st with
  | Pending pe ->
    let prev = pe.on_cancel in
    pe.on_cancel <- (fun () -> prev (); (try f () with _ -> ()))
  | Fulfilled _ | Rejected _ -> ()

let async_exception_hook =
  ref (fun exn ->
    prerr_string "Fatal error: exception ";
    prerr_string (Printexc.to_string exn);
    prerr_newline ();
    exit 2)

let dont_wait f handler =
  ignore
    (async (fun () ->
       try_bind f (fun () -> return_unit) (fun e -> handler e; return_unit)))

let ignore_result p = on_any p ignore (fun e -> !async_exception_hook e)

(* Cancellation-isolation: approximations for API coverage. *)
let no_cancel p = p
let protected p = p

module Infix = struct
  let ( >>= ) = bind
  let ( =<< ) f p = bind p f
  let ( >|= ) p f = map f p
  let ( =|< ) f p = map f p
end

(* ------------------------------------------------------------------ *)
(* Non-blocking I/O on raw file descriptors                           *)
(* ------------------------------------------------------------------ *)

module Io = struct
  (* Readiness watchers are kept registered per (fd, direction) across calls,
     rather than created one-shot per blocking syscall. Re-registering a libev
     watcher on every read costs an [epoll_ctl] ADD+DEL each time; keeping it
     alive (as Lwt_unix does) reduces that to one registration per fd.

     A [waitset] holds the live engine event (if any) and the promises of the
     fibers waiting for that readiness. When the fd becomes ready, all current
     waiters are resolved; they retry their syscall and, on [EAGAIN], re-arm the
     same still-registered watcher. A watcher is stopped only when it fires with
     no waiters left — using the event the engine hands to the callback, so
     there is no stop/re-register race. In a tight loop the waiter is always
     re-armed before the fd is ready again, so the watcher persists and no
     [epoll_ctl] is issued after the first registration. *)
  type waitset = {
    mutable event : Lwt_engine.event option;
    mutable waiters : unit t list;
    register : (Lwt_engine.event -> unit) -> Lwt_engine.event;
  }

  type entry = { rd : waitset; wr : waitset }

  let table : (Unix.file_descr, entry) Hashtbl.t = Hashtbl.create 64

  let entry_of fd =
    match Hashtbl.find_opt table fd with
    | Some e -> e
    | None ->
      let e =
        {
          rd = { event = None; waiters = []; register = Lwt_engine.on_readable fd };
          wr = { event = None; waiters = []; register = Lwt_engine.on_writable fd };
        }
      in
      Hashtbl.add table fd e;
      e

  let fire ws ev =
    match ws.waiters with
    | [] ->
      (* The fd is ready but nobody is waiting: stop the (level-triggered)
         watcher to avoid spinning. A later wait re-registers it. *)
      ws.event <- None;
      Lwt_engine.stop_event ev
    | waiters ->
      ws.waiters <- [];
      List.iter
        (fun p ->
          match p.st with
          | Pending _ ->
            decr outstanding;
            fill p ok_unit
          | Fulfilled _ | Rejected _ -> ())
        waiters

  let wait_on ws =
    let p = new_pending () in
    incr outstanding;
    ws.waiters <- p :: ws.waiters;
    (match ws.event with
    | Some _ -> ()
    | None -> ws.event <- Some (ws.register (fire ws)));
    (* A cancelled waiter is left in the list and skipped on the next fire. *)
    set_on_cancel p (fun () -> decr outstanding);
    await p

  let wait_readable fd = wait_on (entry_of fd).rd
  let wait_writable fd = wait_on (entry_of fd).wr

  (* Drop all readiness watchers (e.g. between independent [run]s, since
     descriptor numbers may be reused). Installed as the scheduler reset hook. *)
  let reset () =
    let stop ws =
      match ws.event with
      | Some ev ->
        ws.event <- None;
        (try Lwt_engine.stop_event ev with _ -> ())
      | None -> ()
    in
    Hashtbl.iter (fun _ e -> stop e.rd; stop e.wr) table;
    Hashtbl.clear table

  let () = on_reset := reset

  (* Specialised retry loops (rather than a generic [retry_eagain op]) so the
     hot path allocates no per-call operation closure. *)
  let rec read fd buf off len =
    match Unix.read fd buf off len with
    | n -> n
    | exception
        Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) ->
      wait_readable fd;
      read fd buf off len

  let rec write fd buf off len =
    match Unix.write fd buf off len with
    | n -> n
    | exception
        Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) ->
      wait_writable fd;
      write fd buf off len

  let rec accept fd =
    match Unix.accept fd with
    | res -> res
    | exception
        Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) ->
      wait_readable fd;
      accept fd

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

module Private = struct
  let enqueue = enqueue
  let outstanding = outstanding
  let set_idle = set_idle
  let default_idle = default_idle
  let new_pending = new_pending
  let fill = fill
  let set_on_cancel = set_on_cancel
end
