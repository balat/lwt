(* Engine-free core of the effect-based scheduler for Lwt-style promises.

   This module is everything that does NOT touch an event source: the promise
   machinery, the resolution loop, the run queue and effect handler, the pause
   protocol and the Lwt-compatible combinator layer. It depends on nothing but
   the standard library — by construction, since the in-place Lwt core swap
   requires the core to sit below lwt.unix (which provides the event engine).
   The event-engine layer (timers, fd readiness, blocking idle wait, interop
   with the real Lwt) is [Lwt_effects], which [include]s this module and
   installs its idle hook. See lwt_effects.mli for the rationale.

   Key points of the implementation:

   - A promise ['a t] is a mutable cell, either resolved or pending. A pending
     promise carries its waiters and an optional cancel action.
   - [bind] is direct-style: on a resolved promise it is a plain application; on
     a pending one it performs an [Await] effect, so the fiber's continuation is
     captured by the scheduler instead of being allocated as a callback closure.
   - The scheduler is a single run queue of ready thunks. When the queue is
     empty, the pluggable idle hook blocks until external work arrives (the
     Lwt_engine layer or io_uring installs it; the bare core has none). A waiter
     never resumes a continuation directly: it only enqueues it, keeping
     resolution flat (no deep recursion). *)

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

(* ------------------------------------------------------------------ *)
(* The scheduler loop                                                 *)
(* ------------------------------------------------------------------ *)

(* Idle-wait hook: called when the run queue is empty, to block until external
   work arrives. Returns [true] if it may have produced new work (the loop
   continues), [false] if there is nothing left to wait for (scheduler is done).
   A backend (the Lwt_engine layer in [Lwt_effects], or io_uring) installs its
   own with [set_idle]. The bare core has no event source: with nothing to
   block on, an empty run queue means the scheduler is done. *)
let core_idle () : bool = false

(* Run at the start of [run] to reset back-end state (e.g. the I/O readiness
   table) that must not leak across independent scheduler runs. *)
let on_reset : (unit -> unit) ref = ref ignore

let idle_hook : (unit -> bool) ref = ref core_idle
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

