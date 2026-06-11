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

(* The concrete promise is a mutable cell, so its type parameter is necessarily
   {e invariant}. But the public type [+'a t] (below) must be {e covariant} to be
   a drop-in for [Lwt.t] (e.g. so that [int t :> [> ] t] and so that cohttp's
   [Cohttp.S.IO] functor, which requires [type +'a t], can be instantiated). *)
type 'a promise = { mutable st : 'a promise_state }

and 'a promise_state =
  | Fulfilled of 'a
  | Rejected of exn
  | Pending of 'a pending

and 'a pending = {
  mutable waiters : (('a, exn) result -> unit) list;
    (* Most-recently-added first; each waiter runs once and only enqueues. *)
  mutable cancel_waiters : (unit -> unit) list;
    (* [on_cancel] callbacks; run BEFORE [waiters] when rejected with
       [Canceled] (Lwt's ordering guarantee). *)
  mutable cancel : cancel_mode;
    (* How this promise reacts to [cancel] while pending (Lwt's model). *)
}

(* Lwt's cancellation model:
   - [Cancel_self hook]: directly cancelable ([task], timers, I/O) — [cancel]
     runs the hook (e.g. stopping an engine event) then rejects with [Canceled];
   - [Cancel_forward fwd]: a derived promise (e.g. a [bind] result) — [cancel]
     forwards to its current source; the rejection then flows back through the
     ordinary waiter chain (the promise is not rejected directly);
   - [Not_cancelable]: [wait]-created (and [no_cancel]) promises ignore [cancel]. *)
and cancel_mode =
  | Cancel_self of (unit -> unit)
  | Cancel_forward of (unit -> unit)
  | Not_cancelable

exception Canceled

(* Covariant public handle over the invariant concrete [promise].

   OCaml cannot express a covariant type with a mutable field, so — exactly like
   Lwt's [Public_types] ([to_public_promise]/[to_internal_promise]) — we declare
   [+'a t] as an abstract type and bridge it to [promise] with identity
   coercions. This is SOUND:
   - [t] and [promise] have the {e same runtime representation} ([Obj.magic] is a
     no-op cast here, not a reinterpretation);
   - covariance is safe because the public API never writes through a coerced
     promise in a way that would let a [<:b] value be observed at a wrong type:
     a resolved promise is only ever {e read}, and resolvers ([wakeup]) take the
     value at its own type. This mirrors Lwt's long-standing design. *)
(* No signature ascription here (exactly like Lwt's [Public_types]): [+'a t] is
   abstract because it is declared without a definition, yet [inj]/[prj] keep
   visible bodies so flambda can inline these identity coercions away. *)
module Public_handle = struct
  type +'a t
  type -'a u

  let inj : 'a promise -> 'a t = Obj.magic
  let prj : 'a t -> 'a promise = Obj.magic

  (* The resolver handle [-'a u] uses the same identity-coercion scheme, in the
     other direction: a resolver only ever {e consumes} values of type ['a]
     ([wakeup] writes a value into the cell), so contravariance is safe — using
     an ['a u] at a subtype ['b u] only ever feeds it ['b] values, which are
     also ['a]s. This mirrors Lwt's [type -'a u] in [Public_types]. *)
  let inj_u : 'a promise -> 'a u = Obj.magic
  let prj_u : 'a u -> 'a promise = Obj.magic
end

type +'a t = 'a Public_handle.t

let inj = Public_handle.inj
let prj = Public_handle.prj

(* ------------------------------------------------------------------ *)
(* Fiber-local storage (Lwt.key)                                      *)
(* ------------------------------------------------------------------ *)

(* Same trick as Lwt (no Obj.magic): each key owns a typed scratch cell, and the
   storage maps a key id to a "refresh" closure that writes the stored value
   into that cell. The current storage is restored on every fiber resume/start
   (see [run_scheduler] and the [Await]/[Yield] handler), so a value set with
   [with_value] survives suspensions and is inherited by spawned fibers. *)
module Storage_map = Map.Make (Int)

type storage = (unit -> unit) Storage_map.t
type 'a key = { id : int; mutable value : 'a option }

let next_key_id = ref 0

let new_key () =
  let id = !next_key_id in
  incr next_key_id;
  { id; value = None }

let empty_storage : storage = Storage_map.empty
let current_storage = ref empty_storage

let get_from_storage key storage =
  match Storage_map.find_opt key.id storage with
  | Some refresh ->
    refresh ();
    let value = key.value in
    key.value <- None;
    value
  | None -> None

let modify_storage key value storage =
  match value with
  | Some _ -> Storage_map.add key.id (fun () -> key.value <- value) storage
  | None -> Storage_map.remove key.id storage

let get key = get_from_storage key !current_storage

let with_value key value f =
  let saved = !current_storage in
  current_storage := modify_storage key value saved;
  match f () with
  | r ->
    current_storage := saved;
    r
  | exception e ->
    current_storage := saved;
    raise e

(* ------------------------------------------------------------------ *)
(* Resolution loop (Lwt's callback-deferral semantics)                *)
(* ------------------------------------------------------------------ *)

(* Lwt runs resolution callbacks inside a "resolution loop". The promise STATE
   is always set immediately; what may be deferred is running the callbacks:
   - [wakeup] runs them immediately, whatever the current nesting;
   - internal resolutions run them immediately up to a nesting depth of
     [default_maximum_callback_nesting_depth], beyond which they are deferred
     (Lwt's tail-call/stack protection);
   - [wakeup_later] defers them whenever some resolution is already in
     progress (nesting depth >= 1).
   Deferred callbacks run when the outermost loop exits. The fiber-local
   storage is snapshotted on entry and restored on exit, exactly as Lwt does
   (callbacks run under their registration-time storage and must not leak it
   to the resolver's caller). Mirroring Lwt, a callback that raises escapes
   the loop without unwinding it (Lwt's own [run_in_resolution_loop] does not
   catch). *)

let default_maximum_callback_nesting_depth = 42
let current_callback_nesting_depth = ref 0
let deferred_callbacks : (unit -> unit) Queue.t = Queue.create ()

(* Runs the deferred callbacks; called at depth 1, so a [wakeup_later]
   performed by a deferred callback is itself deferred and picked up by the
   same drain. *)
let drain_deferred () =
  while not (Queue.is_empty deferred_callbacks) do
    (Queue.pop deferred_callbacks) ()
  done

let leave_resolution_loop (storage_snapshot : storage) : unit =
  if !current_callback_nesting_depth = 1 then drain_deferred ();
  decr current_callback_nesting_depth;
  current_storage := storage_snapshot

let run_in_resolution_loop (f : unit -> unit) : unit =
  incr current_callback_nesting_depth;
  let storage_snapshot = !current_storage in
  f ();
  leave_resolution_loop storage_snapshot

(* Lwt.Private/abandon_wakeups: bail out of a resolution loop after an
   exception escaped it (https://github.com/ocsigen/lwt/issues/48). *)
let abandon_resolution_loop () =
  if !current_callback_nesting_depth <> 0 then begin
    current_callback_nesting_depth := 1;
    leave_resolution_loop empty_storage
  end

(* ------------------------------------------------------------------ *)
(* Promise primitives                                                 *)
(* ------------------------------------------------------------------ *)

let new_pending () : 'a t =
  inj
    { st = Pending { waiters = []; cancel_waiters = []; cancel = Cancel_self ignore } }

let run_resolution_callbacks (type a) (pe : a pending) (r : (a, exn) result) :
    unit =
  (match r with
  | Error Canceled -> List.iter (fun f -> f ()) (List.rev pe.cancel_waiters)
  | Ok _ | Error _ -> ());
  List.iter (fun w -> w r) (List.rev pe.waiters)

let fill_general (type a) ~allow_deferring ~maximum_callback_nesting_depth
    (p : a t) (r : (a, exn) result) : unit =
  let p = prj p in
  match p.st with
  | Pending pe ->
    p.st <- (match r with Ok v -> Fulfilled v | Error e -> Rejected e);
    if
      allow_deferring
      && !current_callback_nesting_depth >= maximum_callback_nesting_depth
    then Queue.push (fun () -> run_resolution_callbacks pe r) deferred_callbacks
    else run_in_resolution_loop (fun () -> run_resolution_callbacks pe r)
  | Fulfilled _ | Rejected _ -> ()

(* Internal resolution: immediate up to the default nesting depth. *)
let fill (type a) (p : a t) (r : (a, exn) result) : unit =
  fill_general ~allow_deferring:true
    ~maximum_callback_nesting_depth:default_maximum_callback_nesting_depth p r

let add_waiter (type a) (p : a t) (w : (a, exn) result -> unit) : unit =
  match (prj p).st with
  | Pending pe -> pe.waiters <- w :: pe.waiters
  | Fulfilled v -> w (Ok v)
  | Rejected e -> w (Error e)

let set_on_cancel (type a) (p : a t) (f : unit -> unit) : unit =
  match (prj p).st with
  | Pending pe -> pe.cancel <- Cancel_self f
  | Fulfilled _ | Rejected _ -> ()

let cancel (type a) (p : a t) : unit =
  match (prj p).st with
  | Pending pe -> (
    match pe.cancel with
    | Not_cancelable -> ()
    | Cancel_self hook ->
      hook ();
      fill p (Error Canceled)
    | Cancel_forward fwd -> fwd ())
  | Fulfilled _ | Rejected _ -> ()

(* Mark [result] as forwarding cancellation to its current source [src] (no-op
   if [result] is no longer pending). Used by the derived combinators. *)
let set_cancel_forward (type a b) (result : a t) (src : b t) : unit =
  match (prj result).st with
  | Pending pe -> pe.cancel <- Cancel_forward (fun () -> cancel src)
  | Fulfilled _ | Rejected _ -> ()

(* Forward cancellation to a whole list of sources (Lwt's
   [propagate_cancel_to_several], used by choose/pick/join/all/both/nchoose). *)
let set_cancel_forward_list (type a b) (result : a t) (ps : b t list) : unit =
  match (prj result).st with
  | Pending pe -> pe.cancel <- Cancel_forward (fun () -> List.iter cancel ps)
  | Fulfilled _ | Rejected _ -> ()

(* ------------------------------------------------------------------ *)
(* Scheduler state                                                    *)
(* ------------------------------------------------------------------ *)

(* A ready unit of work in the run queue. [Resume] avoids allocating a
   [fun () -> continue k r] closure for every suspension: the waiter pushes the
   continuation and its result directly. Each task carries the fiber-local
   storage to restore before it runs. *)
type task =
  | Thunk of storage * (unit -> unit)
  | Resume : storage * 'b * ('b, unit) Effect.Deep.continuation -> task

(* Growable ring buffer used as the run queue. Stdlib.Queue allocates a list
   cell on every push; this allocates only when it has to grow. Capacity is kept
   a power of two so indexing uses [land] instead of [mod]. A sentinel fills
   freed slots so consumed continuations are not retained. *)
module Run_queue = struct
  let sentinel : task = Thunk (Storage_map.empty, ignore)

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
let enqueue (f : unit -> unit) : unit =
  Run_queue.push run_queue (Thunk (!current_storage, f))

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
  match (prj p).st with
  | Fulfilled v -> Ok v
  | Rejected e -> Error e
  | Pending _ -> Effect.perform (Await p)

let await (type a) (p : a t) : a =
  match await_result p with Ok v -> v | Error e -> raise e

(* ------------------------------------------------------------------ *)
(* Constructors and combinators                                       *)
(* ------------------------------------------------------------------ *)

let return v = inj { st = Fulfilled v }
let fail e = inj { st = Rejected e }
let return_unit = return ()

(* Which exceptions Lwt machinery may catch (and turn into rejections), vs let
   bubble out of the scheduler. Same definition and default as Lwt's. *)
module Exception_filter = struct
  type t = exn -> bool

  let handle_all = fun _ -> true

  let handle_all_except_runtime = function
    | Out_of_memory | Stack_overflow -> false
    | _ -> true

  let v = ref handle_all_except_runtime
  let set f = v := f
  let run e = !v e
end

(* Apply the continuation of a bind, turning a synchronous exception into a
   rejected promise (when the exception filter allows catching it). Used on the
   deferred (pending) paths: fast paths apply [f] plainly, as Lwt does. *)
let apply (f : 'a -> 'b t) (v : 'a) : 'b t =
  try f v with e when Exception_filter.run e -> inj { st = Rejected e }

(* Forward the eventual result of [p'] into the pending promise [result], and
   make [result]'s cancellation follow [p'] (Lwt: cancelling a derived promise
   cancels its current source; the rejection then flows back through waiters). *)
let forward (type a) (result : a t) (p' : a t) : unit =
  match (prj p').st with
  | Fulfilled v' -> fill result (Ok v')
  | Rejected e -> fill result (Error e)
  | Pending _ ->
    set_cancel_forward result p';
    add_waiter p' (fun r' -> fill result r')

(* [bind] is non-blocking (like Lwt's): it does not suspend the caller, so a
   pending bind preserves Lwt's implicit concurrency — e.g.
   [both (a >>= f) (b >>= g)] starts both branches. It allocates one promise and
   one callback per pending bind (Lwt's trade-off, without the proxy machinery),
   and restores the storage in effect at the bind around the callback. The
   cheaper, suspending effect bind that gives up implicit concurrency lives only
   on the experimental branch. *)
let bind (type a b) (p : a t) (f : a -> b t) : b t =
  match (prj p).st with
  (* Fast path: a plain application, as in Lwt — a synchronous exception raised
     by [f] escapes to the caller here (only the deferred, pending-path callback
     turns it into a rejection). This matches Lwt's documented behaviour and
     keeps the fast path free of any try/with. *)
  | Fulfilled v -> f v
  | Rejected e -> inj { st = Rejected e }
  | Pending _ ->
    let result = new_pending () in
    set_cancel_forward result p;
    let saved = !current_storage in
    add_waiter p (fun r ->
      let outer = !current_storage in
      current_storage := saved;
      (match r with Ok v -> forward result (apply f v) | Error e -> fill result (Error e));
      current_storage := outer);
    result

(* Unlike {!bind}, [map] captures a synchronous exception of [f] into a rejected
   promise even on the fulfilled fast path (Lwt's deliberate asymmetry: map's [f]
   is a plain value function). Defined directly — no intermediate promise. *)
let map (type a b) (f : a -> b) (p : a t) : b t =
  match (prj p).st with
  | Fulfilled v -> (
    try return (f v) with e when Exception_filter.run e -> inj { st = Rejected e })
  | Rejected e -> inj { st = Rejected e }
  | Pending _ ->
    let result = new_pending () in
    set_cancel_forward result p;
    let saved = !current_storage in
    add_waiter p (fun r ->
      let outer = !current_storage in
      current_storage := saved;
      (match r with
      | Ok v -> (
        try fill result (Ok (f v))
        with e when Exception_filter.run e -> fill result (Error e))
      | Error e -> fill result (Error e));
      current_storage := outer);
    result

let ( >>= ) = bind
let ( >|= ) p f = map f p

(* [catch]/[try_bind] are non-blocking (they do not suspend the caller): a
   synchronous exception raised by [f] itself is routed to the handler (when the
   exception filter allows), as is a rejection of [f ()]'s promise, mirroring
   Lwt. On the already-resolved fast paths [g]/[h] are applied plainly, so their
   own synchronous exceptions escape to the caller — as in Lwt. *)
let try_bind (f : unit -> 'a t) (g : 'a -> 'b t) (h : exn -> 'b t) : 'b t =
  let p = try f () with e when Exception_filter.run e -> inj { st = Rejected e } in
  match (prj p).st with
  | Fulfilled v -> g v
  | Rejected e -> h e
  | Pending _ ->
    let result = new_pending () in
    set_cancel_forward result p;
    let saved = !current_storage in
    add_waiter p (fun r ->
      let outer = !current_storage in
      current_storage := saved;
      forward result (match r with Ok v -> apply g v | Error e -> apply h e);
      current_storage := outer);
    result

let catch (f : unit -> 'a t) (h : exn -> 'a t) : 'a t = try_bind f return h

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
          let s = !current_storage in
          add_waiter p (fun r -> Run_queue.push run_queue (Resume (s, r, k))))
    | Yield ->
      Some (fun k -> Run_queue.push run_queue (Resume (!current_storage, (), k)))
    | _ -> None
  in
  { retc; exnc; effc }

(* Start [body] as a fresh fiber under the shared handler. *)
let spawn (body : unit -> unit) : unit =
  enqueue (fun () -> Effect.Deep.match_with body () handler)

let async (f : unit -> 'a t) : 'a t =
  let p = new_pending () in
  spawn (fun () ->
    let r = try await_result (f ()) with e when Exception_filter.run e -> Error e in
    fill p r);
  p

(* Lwt's [both] waits for {e both} promises even when one is already rejected
   (the result stays pending until the other resolves), then rejects with the
   first rejection encountered. Callback-counting, like [join] below. *)
let both (a : 'a t) (b : 'b t) : ('a * 'b) t =
  let result = new_pending () in
  (match (prj result).st with
  | Pending pe ->
    pe.cancel <-
      Cancel_forward
        (fun () ->
          cancel a;
          cancel b)
  | Fulfilled _ | Rejected _ -> ());
  let va = ref None
  and vb = ref None
  and failure = ref None
  and remaining = ref 2 in
  let settle () =
    decr remaining;
    if !remaining = 0 then
      match (!failure, !va, !vb) with
      | Some e, _, _ -> fill result (Error e)
      | None, Some x, Some y -> fill result (Ok (x, y))
      | None, _, _ -> ()
  in
  add_waiter a (fun r ->
    (match r with
    | Ok x -> va := Some x
    | Error e -> if !failure = None then failure := Some e);
    settle ());
  add_waiter b (fun r ->
    (match r with
    | Ok y -> vb := Some y
    | Error e -> if !failure = None then failure := Some e);
    settle ());
  result

(* Among already-resolved promises, Lwt's [choose]/[pick] prefer a {e rejection};
   with several fulfilled and none rejected, one is chosen at random (fairness,
   as Lwt). The [Invalid_argument] messages use Lwt's wording: this core is meant
   to BE the Lwt core (B2b), where these are the right names. *)
let select_resolved (ps : 'a t list) : 'a t option =
  match
    List.filter (fun p -> match (prj p).st with Rejected _ -> true | _ -> false) ps
  with
  | p :: _ -> Some p
  | [] -> (
    match
      List.filter
        (fun p -> match (prj p).st with Fulfilled _ -> true | _ -> false)
        ps
    with
    | [] -> None
    | [ p ] -> Some p
    | l -> Some (List.nth l (Random.int (List.length l))))

let choose (ps : 'a t list) : 'a t =
  if ps = [] then
    invalid_arg "Lwt.choose [] would return a promise that is pending forever";
  match select_resolved ps with
  | Some p -> p
  | None ->
    let result = new_pending () in
    set_cancel_forward_list result ps;
    (* [fill] is a no-op on an already-filled promise: first resolution wins. *)
    List.iter (fun p -> add_waiter p (fun r -> fill result r)) ps;
    result

let pick (ps : 'a t list) : 'a t =
  if ps = [] then
    invalid_arg "Lwt.pick [] would return a promise that is pending forever";
  match select_resolved ps with
  | Some p ->
    List.iter (fun q -> if q != p then cancel q) ps;
    p
  | None ->
    let result = new_pending () in
    set_cancel_forward_list result ps;
    (* [settling] keeps the losers' own waiters (fired by the cancellations
       below) from resolving [result] before the winner's value does. *)
    let settling = ref false in
    List.iter
      (fun p ->
        add_waiter p (fun r ->
          if not !settling then
            match (prj result).st with
            | Pending _ ->
              settling := true;
              (* Cancel the losers BEFORE resolving the result, so their
                 cancellation callbacks run first (Lwt's ordering). *)
              List.iter (fun q -> if q != p then cancel q) ps;
              fill result r
            | Fulfilled _ | Rejected _ -> ()))
      ps;
    result

(* ------------------------------------------------------------------ *)
(* Yielding and timers                                                *)
(* ------------------------------------------------------------------ *)

(* Lwt's pause protocol: paused promises gather in a queue served on the next
   scheduler tick (or by an explicit [wakeup_paused], as Lwt_main does), with a
   count and an optional notifier — conformant with Lwt.{pause,paused_count,
   wakeup_paused,register_pause_notifier,abandon_paused}. *)
let paused : unit t list ref = ref []
let paused_n = ref 0
let pause_notifier : (int -> unit) option ref = ref None

let pause () : unit t =
  let p = new_pending () in
  paused := p :: !paused;
  incr paused_n;
  (match !pause_notifier with Some f -> f !paused_n | None -> ());
  p

let paused_count () = !paused_n

let wakeup_paused () =
  (* Snapshot first: a [pause] performed while waking lands in the next batch. *)
  let ps = List.rev !paused in
  paused := [];
  paused_n := 0;
  List.iter (fun p -> fill p ok_unit) ps

let register_pause_notifier f = pause_notifier := Some f
let abandon_paused () =
  paused := [];
  paused_n := 0

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
    (* Serve the paused promises first: a [pause] resolves on the next tick,
       before the scheduler blocks in the engine (whatever the idle backend). *)
    if !paused_n > 0 then begin
      wakeup_paused ();
      run_scheduler ()
    end
    else if !idle_hook () then run_scheduler ()
  end
  else begin
    (match Run_queue.pop run_queue with
    | Thunk (s, f) ->
      current_storage := s;
      f ()
    | Resume (s, v, k) ->
      current_storage := s;
      Effect.Deep.continue k v);
    run_scheduler ()
  end

let run (type a) (main : unit -> a t) : a =
  !on_reset ();
  current_storage := empty_storage;
  let outcome = ref None in
  spawn (fun () ->
    let r = try await_result (main ()) with e when Exception_filter.run e -> Error e in
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
(* Lwt-compatibility layer                                            *)
(* ------------------------------------------------------------------ *)

(* Enough of Lwt's public API to compile code written against Lwt. [bind] being
   non-blocking, the implicit-concurrency semantics is Lwt's — see the .mli.
   Combinators that Lwt makes non-blocking are implemented with [async] so they
   also return immediately. *)

type 'a state = Return of 'a | Fail of exn | Sleep

(* Contravariant resolver handle (same identity coercions as [t]; see
   [Public_handle]). [wait]/[task] return the same cell under both handles. *)
type -'a u = 'a Public_handle.u

let t_of_u (u : 'a u) : 'a t = inj (Public_handle.prj_u u)
let u_of_t (p : 'a t) : 'a u = Public_handle.inj_u (prj p)

(* [wait] promises are not cancelable; [task] promises are (Lwt's model). *)
let wait () =
  let p = new_pending () in
  (match (prj p).st with
  | Pending pe -> pe.cancel <- Not_cancelable
  | Fulfilled _ | Rejected _ -> ());
  (p, u_of_t p)

let task () =
  let p = new_pending () in
  (p, u_of_t p)

(* Lwt's resolver semantics: resolving an already-resolved promise raises
   [Invalid_argument fname] — except when it was resolved by cancellation, in
   which case it is a no-op (so a resolver raced by [cancel] stays safe).
   [fname] parameterises the message so a core-swap candidate can report
   "Lwt.wakeup" etc. (Internal [fill] keeps its silent no-op: backend completion
   handlers may legitimately fire after a cancel.) *)
let wakeup_named (fname : string) (u : 'a u) (r : ('a, exn) result) : unit =
  let p = t_of_u u in
  match (prj p).st with
  | Pending _ ->
    (* [wakeup] never defers: callbacks run now whatever the nesting. *)
    fill_general ~allow_deferring:false
      ~maximum_callback_nesting_depth:default_maximum_callback_nesting_depth p
      r
  | Rejected Canceled -> ()
  | Fulfilled _ | Rejected _ -> invalid_arg fname

(* [wakeup_later]: callbacks are deferred whenever some resolution is already
   in progress (Lwt resolves with [~maximum_callback_nesting_depth:1]). *)
let wakeup_later_named (fname : string) (u : 'a u) (r : ('a, exn) result) :
    unit =
  let p = t_of_u u in
  match (prj p).st with
  | Pending _ ->
    fill_general ~allow_deferring:true ~maximum_callback_nesting_depth:1 p r
  | Rejected Canceled -> ()
  | Fulfilled _ | Rejected _ -> invalid_arg fname

let wakeup (u : 'a u) v = wakeup_named "Lwt_effects.wakeup" u (Ok v)
let wakeup_exn (u : 'a u) e = wakeup_named "Lwt_effects.wakeup_exn" u (Error e)

let wakeup_later (u : 'a u) v =
  wakeup_later_named "Lwt_effects.wakeup_later" u (Ok v)

let wakeup_later_exn (u : 'a u) e =
  wakeup_later_named "Lwt_effects.wakeup_later_exn" u (Error e)

let state (type a) (p : a t) : a state =
  match (prj p).st with
  | Fulfilled v -> Return v
  | Rejected e -> Fail e
  | Pending _ -> Sleep

let is_sleeping p =
  match (prj p).st with Pending _ -> true | Fulfilled _ | Rejected _ -> false
let poll p =
  match (prj p).st with
  | Fulfilled v -> Some v
  | Rejected e -> raise e
  | Pending _ -> None
let of_result = function Ok v -> return v | Error e -> fail e

let fail_with msg = fail (Failure msg)
let fail_invalid_arg msg = fail (Invalid_argument msg)
(* Now generalisable thanks to [t] being covariant (relaxed value restriction). *)
let return_none = return None
let return_nil = return []
let return_some x = return (Some x)
let return_ok x = return (Ok x)
let return_error e = return (Error e)
let return_true = return true
let return_false = return false

let wrap f = try return (f ()) with e when Exception_filter.run e -> fail e

let finalize f g =
  try_bind f
    (fun x -> bind (g ()) (fun () -> return x))
    (fun e -> bind (g ()) (fun () -> fail e))

(* Non-blocking like Lwt's: the awaiting happens in a spawned fiber. *)
(* [join]/[all] are callback-counting (no fiber): they resolve {e immediately}
   when every promise is already resolved — Lwt code observes the state right
   after the call — and otherwise settle when the last pending one does. On
   rejection they still wait for all, then reject with the {e first} rejection
   encountered (already-rejected ones in list order first), as Lwt. *)
let join (ps : unit t list) : unit t =
  match ps with
  | [] -> return_unit
  | _ ->
    let result = new_pending () in
    set_cancel_forward_list result ps;
    let remaining = ref (List.length ps) in
    let failure = ref None in
    List.iter
      (fun p ->
        add_waiter p (fun r ->
          (match r with
          | Error e -> if !failure = None then failure := Some e
          | Ok () -> ());
          decr remaining;
          if !remaining = 0 then
            match !failure with
            | None -> fill result (Ok ())
            | Some e -> fill result (Error e)))
      ps;
    result

let all (ps : 'a t list) : 'a list t =
  match ps with
  | [] -> return []
  | _ ->
    let result = new_pending () in
    set_cancel_forward_list result ps;
    let n = List.length ps in
    let values = Array.make n None in
    let remaining = ref n in
    let failure = ref None in
    List.iteri
      (fun i p ->
        add_waiter p (fun r ->
          (match r with
          | Ok v -> values.(i) <- Some v
          | Error e -> if !failure = None then failure := Some e);
          decr remaining;
          if !remaining = 0 then
            match !failure with
            | None ->
              (* Every slot is [Some] once [remaining] is 0 with no failure. *)
              fill result (Ok (Array.to_list values |> List.filter_map Fun.id))
            | Some e -> fill result (Error e)))
      ps;
    result

(* The result of an [nchoose]-family snapshot: values of the currently-fulfilled
   promises in list order, or the first rejection in list order. *)
let nchoose_result (ps : 'a t list) : ('a list, exn) result =
  let rec collect acc = function
    | [] -> Ok (List.rev acc)
    | p :: rest -> (
      match (prj p).st with
      | Fulfilled v -> collect (v :: acc) rest
      | Rejected e -> Error e
      | Pending _ -> collect acc rest)
  in
  collect [] ps

let any_resolved ps =
  List.exists
    (fun p -> match (prj p).st with Pending _ -> false | _ -> true)
    ps

let nchoose (ps : 'a t list) : 'a list t =
  if ps = [] then
    invalid_arg "Lwt.nchoose [] would return a promise that is pending forever";
  if any_resolved ps then (
    match nchoose_result ps with Ok vs -> return vs | Error e -> fail e)
  else begin
    let result = new_pending () in
    set_cancel_forward_list result ps;
    List.iter
      (fun p ->
        add_waiter p (fun _ ->
          match (prj result).st with
          | Pending _ -> fill result (nchoose_result ps)
          | Fulfilled _ | Rejected _ -> ()))
      ps;
    result
  end

(* Like [nchoose], but also returns the promises still pending at that point. *)
let nchoose_split (type a) (ps : a t list) : (a list * a t list) t =
  if ps = [] then
    invalid_arg
      "Lwt.nchoose_split [] would return a promise that is pending forever";
  let snapshot () =
    let rec collect vs pend = function
      | [] -> Ok (List.rev vs, List.rev pend)
      | p :: rest -> (
        match (prj p).st with
        | Fulfilled v -> collect (v :: vs) pend rest
        | Rejected e -> Error e
        | Pending _ -> collect vs (p :: pend) rest)
    in
    collect [] [] ps
  in
  if any_resolved ps then (
    match snapshot () with Ok x -> return x | Error e -> fail e)
  else begin
    let result = new_pending () in
    set_cancel_forward_list result ps;
    List.iter
      (fun p ->
        add_waiter p (fun _ ->
          match (prj result).st with
          | Pending _ -> fill result (snapshot ())
          | Fulfilled _ | Rejected _ -> ()))
      ps;
    result
  end

let npick (ps : 'a t list) : 'a list t =
  if ps = [] then
    invalid_arg "Lwt.npick [] would return a promise that is pending forever";
  let cancel_pending () =
    List.iter (fun p -> if is_sleeping p then cancel p) ps
  in
  if any_resolved ps then begin
    (* Snapshot before cancelling: the cancellations must not become the
       result. *)
    let r = nchoose_result ps in
    cancel_pending ();
    match r with Ok vs -> return vs | Error e -> fail e
  end
  else begin
    let result = new_pending () in
    set_cancel_forward_list result ps;
    let settling = ref false in
    List.iter
      (fun p ->
        add_waiter p (fun _ ->
          if not !settling then
            match (prj result).st with
            | Pending _ ->
              settling := true;
              let r = nchoose_result ps in
              cancel_pending ();
              fill result r
            | Fulfilled _ | Rejected _ -> ()))
      ps;
    result
  end

let async_exception_hook =
  ref (fun exn ->
    prerr_string "Fatal error: exception ";
    prerr_string (Printexc.to_string exn);
    prerr_newline ();
    exit 2)

(* Lwt's [on_*] semantics: an exception raised by the callback goes to
   [async_exception_hook] (when the exception filter allows catching it), and
   the callback runs with the fiber-local storage that was current at
   registration time (as Lwt does for all its callbacks). *)
let hooked f x =
  try f x with e when Exception_filter.run e -> !async_exception_hook e

(* Wrap a one-argument callback so it restores the registration-time storage. *)
let with_registration_storage f =
  let saved = !current_storage in
  fun x ->
    let outer = !current_storage in
    current_storage := saved;
    hooked f x;
    current_storage := outer

let on_any p f g =
  let f = with_registration_storage f and g = with_registration_storage g in
  add_waiter p (function Ok v -> f v | Error e -> g e)

let on_success p f =
  let f = with_registration_storage f in
  add_waiter p (function Ok v -> f v | Error _ -> ())

let on_failure p f =
  let f = with_registration_storage f in
  add_waiter p (function Ok _ -> () | Error e -> f e)

let on_termination p f =
  let f = with_registration_storage f in
  add_waiter p (fun _ -> f ())

(* Runs [f] when the promise is rejected with [Canceled] — whether cancelled
   directly or through propagation. Cancel callbacks run {e before} ordinary
   waiters (Lwt's ordering guarantee); see [fill]. *)
let on_cancel (type a) (p : a t) (f : unit -> unit) : unit =
  let f = with_registration_storage f in
  match (prj p).st with
  | Pending pe -> pe.cancel_waiters <- f :: pe.cancel_waiters
  | Rejected Canceled -> f ()
  | Fulfilled _ | Rejected _ -> ()

let dont_wait f handler =
  ignore
    (async (fun () ->
       try_bind f (fun () -> return_unit) (fun e -> handler e; return_unit)))

(* Lwt's [ignore_result]: an already-rejected promise raises synchronously; a
   later rejection goes to [async_exception_hook]. *)
let ignore_result p =
  match (prj p).st with
  | Fulfilled _ -> ()
  | Rejected e -> raise e
  | Pending _ -> on_failure p (fun e -> !async_exception_hook e)

(* [no_cancel p] mirrors [p] but ignores [cancel]; [protected p] mirrors [p]
   and is cancelable without affecting [p] (cancelling rejects the mirror only). *)
let no_cancel (type a) (p : a t) : a t =
  match (prj p).st with
  | Fulfilled _ | Rejected _ -> p
  | Pending _ ->
    let result = new_pending () in
    (match (prj result).st with
    | Pending pe -> pe.cancel <- Not_cancelable
    | Fulfilled _ | Rejected _ -> ());
    add_waiter p (fun r -> fill result r);
    result
let protected (type a) (p : a t) : a t =
  match (prj p).st with
  | Fulfilled _ | Rejected _ -> p
  | Pending _ ->
    (* Default [Cancel_self ignore]: cancelable, leaves [p] untouched; a later
       resolution of [p] is absorbed by [fill]'s no-op on resolved. *)
    let result = new_pending () in
    add_waiter p (fun r -> fill result r);
    result

(* [wrap_in_cancelable p] mirrors [p] and is cancelable even if [p] is not:
   cancelling first forwards to [p] (no-op if [p] is not cancelable — its
   rejection, if any, flows into the mirror), then rejects the mirror. *)
let wrap_in_cancelable (type a) (p : a t) : a t =
  match (prj p).st with
  | Fulfilled _ | Rejected _ -> p
  | Pending _ ->
    let result = new_pending () in
    set_on_cancel result (fun () -> cancel p);
    add_waiter p (fun r -> fill result r);
    result

module Infix = struct
  let ( >>= ) = bind
  let ( =<< ) f p = bind p f
  let ( >|= ) p f = map f p
  let ( =|< ) f p = map f p
end


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
