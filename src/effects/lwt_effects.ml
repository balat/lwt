(* Effect-based scheduler for Lwt-style promises (POC).

   This module is the event-engine layer over [Lwt_effects_core] (the
   engine-free promise machinery, resolution loop, run queue and combinators):
   timers and fd-readiness events on {!Lwt_engine}, the blocking idle hook that
   drives Lwt's event loop, interop with the real Lwt, and the non-blocking
   I/O modules. The split mirrors Lwt's own layering (core below, engine in
   lwt.unix) — a requirement for the in-place core swap. See lwt_effects.mli
   for the rationale. *)

include Lwt_effects_core

(* ------------------------------------------------------------------ *)
(* Engine events                                                      *)
(* ------------------------------------------------------------------ *)

(* Register a one-shot engine event resolving the pending unit promise [p]. The
   fire callback uses its own [ev] argument to stop itself (no ref cell), and is
   guarded by [p]'s state so a double fire cannot double-decrement. Cancellation
   stops the captured event and decrements the count. *)
let setup_event (p : unit t)
    (register : (Lwt_engine.event -> unit) -> Lwt_engine.event) : unit =
  incr outstanding;
  let fire ev =
    match (prj p).st with
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

let sleep (d : float) : unit t =
  if d <= 0. then pause ()
  else begin
    let p = new_pending () in
    setup_event p (fun cb -> Lwt_engine.on_timer d false cb);
    p
  end

(* ------------------------------------------------------------------ *)
(* The engine idle hook                                               *)
(* ------------------------------------------------------------------ *)

(* Block in the Lwt event loop while cooperating with Lwt itself: fulfil Lwt's
   paused promises and, when some are pending, poll without blocking (as
   Lwt_main does). This lets fibers [await] real [Lwt.t] promises — including
   Lwt_unix I/O — through {!of_lwt}. Installed below as the scheduler's idle
   hook (the bare core has none). *)
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

let () = set_idle default_idle

(* ------------------------------------------------------------------ *)
(* Interoperability with Lwt                                          *)
(* ------------------------------------------------------------------ *)

(* Both Lwt and this scheduler use the same {!Lwt_engine}, and [run]'s default
   idle hook drives Lwt's paused queue and event loop, so a real [Lwt.t] makes
   progress while a fiber waits on it. *)

let of_lwt (type a) (lwt : a Lwt.t) : a t =
  match Lwt.state lwt with
  | Lwt.Return v -> inj { st = Fulfilled v }
  | Lwt.Fail e -> inj { st = Rejected e }
  | Lwt.Sleep ->
    let p = new_pending () in
    incr outstanding;
    let finish r =
      match (prj p).st with
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
  match (prj p).st with
  | Fulfilled v -> Lwt.return v
  | Rejected e -> Lwt.fail e
  | Pending _ ->
    let lwt, u = Lwt.wait () in
    add_waiter p (function
      | Ok v -> Lwt.wakeup u v
      | Error e -> Lwt.wakeup_exn u e);
    lwt

(* ------------------------------------------------------------------ *)
(* Non-blocking I/O on raw file descriptors                           *)
(* ------------------------------------------------------------------ *)

module Io_impl = struct
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
          match (prj p).st with
          | Pending _ ->
            decr outstanding;
            fill p ok_unit
          | Fulfilled _ | Rejected _ -> ())
        waiters

  let wait_on ws : unit =
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

  (* Register [k] to run (once) when the fd is ready, without suspending the
     caller — used by the monadic I/O below. *)
  let when_ready ws k =
    let p = new_pending () in
    incr outstanding;
    ws.waiters <- p :: ws.waiters;
    (match ws.event with
    | Some _ -> ()
    | None -> ws.event <- Some (ws.register (fire ws)));
    set_on_cancel p (fun () -> decr outstanding);
    add_waiter p (fun _ -> k ())

  (* Monadic, non-blocking I/O (returns a promise, resolved by a callback when
     the fd is ready — like Lwt_unix, no fiber). Composes with the Compat bind
     to express Lwt-style code. *)
  let read_m fd buf off len : int t =
    let result = new_pending () in
    let rec attempt () =
      match Unix.read fd buf off len with
      | n -> fill result (Ok n)
      | exception
          Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) ->
        when_ready (entry_of fd).rd attempt
      | exception e -> fill result (Error e)
    in
    attempt ();
    result

  let write_m fd buf off len : int t =
    let result = new_pending () in
    let rec attempt () =
      match Unix.write fd buf off len with
      | n -> fill result (Ok n)
      | exception
          Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) ->
        when_ready (entry_of fd).wr attempt
      | exception e -> fill result (Error e)
    in
    attempt ();
    result

  let accept_m fd : (Unix.file_descr * Unix.sockaddr) t =
    let result = new_pending () in
    let rec attempt () =
      match Unix.accept fd with
      | res -> fill result (Ok res)
      | exception
          Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) ->
        when_ready (entry_of fd).rd attempt
      | exception e -> fill result (Error e)
    in
    attempt ();
    result

  let connect_m fd addr : unit t =
    match Unix.connect fd addr with
    | () -> return_unit
    | exception Unix.Unix_error ((Unix.EINPROGRESS | Unix.EWOULDBLOCK), _, _) ->
      let result = new_pending () in
      when_ready (entry_of fd).wr (fun () ->
        match Unix.getsockopt_error fd with
        | None -> fill result ok_unit
        | Some e -> fill result (Error (Unix.Unix_error (e, "connect", ""))));
      result
    | exception e -> fail e

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

(* Public monadic I/O: every interruptible call returns a promise ([_ t]), so the
   async type is kept and the call composes with [bind]. *)
module Io = struct
  let read = Io_impl.read_m
  let write = Io_impl.write_m
  let accept = Io_impl.accept_m
  let connect = Io_impl.connect_m
end

(* Direct-style escape hatch (like [Lwt_direct]): turn a promise into a plain
   value with [await], [yield] to the scheduler, and plain-value I/O. These give
   up the [_ t] async typing and suspend the current fiber, so they must run
   inside [run]/[async] — never in a monadic ([bind]) continuation. *)
module Direct = struct
  let await = await
  let yield = yield

  module Io = struct
    let read = Io_impl.read
    let write = Io_impl.write
    let accept = Io_impl.accept
    let connect = Io_impl.connect
    let wait_readable = Io_impl.wait_readable
    let wait_writable = Io_impl.wait_writable
  end
end

module Private = struct
  let enqueue = enqueue
  let outstanding = outstanding
  let set_idle = set_idle
  let default_idle = default_idle
  let new_pending = new_pending
  let fill = fill
  let set_on_cancel = set_on_cancel

  (* Resolvers with a caller-supplied function name in the double-resolve error
     (for the core-swap candidate's "Lwt.wakeup" etc.). *)
  let wakeup_named = wakeup_named
  let wakeup_later_named = wakeup_later_named

  (* Bail out of the resolution loop (Lwt's [abandon_wakeups], issue #48). *)
  let abandon_resolution_loop = abandon_resolution_loop

  (* Fiber-local storage internals, in the exact shape of
     [Lwt.Private.Sequence_associated_storage] (needed by the core-swap
     candidate, and by [Lwt_direct]-style integrations). *)
  type nonrec storage = storage

  let get_from_storage = get_from_storage
  let modify_storage = modify_storage
  let empty_storage = empty_storage
  let current_storage = current_storage
end
