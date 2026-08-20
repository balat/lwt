(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Lean Lwt core (the in-place core swap, WITHOUT effects — the control
   experiment for the effect-based core: same promise machinery, same run
   queue and pause protocol, with the fiber/effect-handler layer removed).

   This implementation satisfies the historical lwt.mli unchanged (the only
   addition is a handful of scheduler hooks under [Private], used by
   [Lwt_main]). It is engine-free: the promise machinery, the resolution loop,
   the run queue and the pause protocol live here; blocking on an event source
   is delegated to the idle hook that lwt.unix's [Lwt_main] installs through
   [Private.scheduler_set_idle] (mirroring Lwt's historical layering, where
   the core knows nothing about engines).

   Key points of the implementation:

   - A promise ['a t] is a mutable cell, either resolved or pending — no proxy
     promises: [bind] never allocates an intermediate proxy, which is where
     most of the speedup over the historical core comes from.
   - The whole public (monadic) API is callback-based and non-blocking, like
     the historical Lwt: implicit concurrency is preserved. The run queue is
     drained by [Private.scheduler_run] (i.e. by [Lwt_main.run]); direct-style
     layers ([Lwt_direct]) push their resumptions onto it through
     [Private.scheduler_enqueue].
   - Attached callbacks run in Lwt's order (most recent first), under Lwt's
     resolution loop (deferral semantics of [wakeup_later], nesting cap). *)

(* [Lwt_sequence] is deprecated – we don't want users outside Lwt using it.
   However, it is still used internally by Lwt (here, [add_task_l]/
   [add_task_r]). So, briefly disable warning 3 ("deprecated") and create a
   local, non-deprecated alias. *)
[@@@ocaml.warning "-3"]
module Lwt_sequence = Lwt_sequence
[@@@ocaml.warning "+3"]

(* The per-domain slot is internal to the Lwt packages; this module is one of
   the two that may use it. *)
[@@@alert "-lwt_internal"]

(* The concrete promise is a mutable cell, so its type parameter is necessarily
   {e invariant}. But the public type [+'a t] (below) must be {e covariant} to be
   a drop-in for [Lwt.t] (e.g. so that [int t :> [> ] t] and so that cohttp's
   [Cohttp.S.IO] functor, which requires [type +'a t], can be instantiated). *)
(* ------------------------------------------------------------------ *)
(* Scheduler state, declared first                                     *)
(* ------------------------------------------------------------------ *)

(* [storage] and the run queue are declared here, ahead of the promise types,
   because the scheduler record below is mutually recursive with them: a promise's
   waiters receive the scheduler, and the scheduler holds the queue of paused
   promises. Neither of these two mentions promises, so lifting them costs
   nothing. *)
module Storage_map = Map.Make (Int)

type storage = exn Storage_map.t

(* A ready unit of work in the run queue. Each task carries the storage to
   restore before it runs. *)
type task = Thunk of storage * (unit -> unit)

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

type 'a promise = { mutable st : 'a promise_state }

and 'a promise_state =
  | Fulfilled of 'a
  | Rejected of exn
  | Pending of 'a pending

and 'a pending = {
  owner : sched;
    (* The scheduler that owns this promise, i.e. the domain that created it.

       IMMUTABLE, so reading it is safe from anywhere and needs no
       synchronisation; and it lives in the [pending] record, so it costs one
       word only during the window where mutation is possible, and a RESOLVED
       promise has no owner at all. That is what makes shared constants such as
       [Lwt.return_unit] safe by construction rather than by convention: they
       are born resolved, so there is nothing to compare.

       Stamped at creation rather than claimed on first mutation. Both were
       measured (study, 13.2ter): claiming spares a per-domain lookup at the
       creating entry points but makes the field mutable, and the write barrier
       it then pays on every promise costs slightly more than the lookup saved.
       Stamping also attributes a violation to the true creator, and it is what
       the level-2 sanitizer of annexe B.8 needs. *)
  mutable waiters : (('a, exn) result -> unit) list;
    (* Most-recently-added first; each waiter runs once and only enqueues.

       A waiter RECEIVES the scheduler rather than capturing it. It could capture
       it, since a promise's callbacks always run on the domain that owns it,
       which is the domain that installed the waiter; but capturing costs one
       word per waiter, measured at +5 words per suspended bind across the
       combinators (study, section 13.4), and receiving costs nothing. It also
       states the invariant in the type. *)
  mutable cancel_waiters : (unit -> unit) list;
    (* [on_cancel] callbacks; run BEFORE [waiters] when rejected with
       [Canceled] (Lwt's ordering guarantee). *)
  mutable cancel : cancel_mode;
    (* How this promise reacts to [cancel] while pending (Lwt's model). *)
  mutable link : 'a promise option;
    (* [Some root]: this cell is an alias of [root] — Lwt's proxy, with the
       merge direction REVERSED so that tail-recursive bind loops are O(1) in
       live memory. When a pending [bind] continuation returns a fresh pending
       promise, [forward] moves the fresh promise's waiters onto the (older,
       anchored) result and links the fresh cell to it: the fresh cell becomes
       garbage as soon as its creator drops it, while the anchored result —
       the one the outside world holds — never grows a chain. [prj] follows
       links with path compression, so all reads/writes act on the root. *)
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
  | Cancel_forward of (sched -> unit)
  | Not_cancelable

(* The scheduler: all of the core's per-scheduler state in ONE record, so that a
   hot path can obtain it once and then work on fields. Mutually recursive with
   the promise types because [paused] holds promises and a waiter receives a
   scheduler.

   Why one record and not one slot per variable: section 13 of the study measured
   a per-domain slot access at about 85 instructions, i.e. 5.5 % of a suspended
   bind, so what matters is the NUMBER of accesses. One record reached once per
   public entry point and then threaded is the shape that keeps that number at
   one. Grouping itself is free, and in fact measured marginally faster than the
   globals it replaces, because a threaded record spares the dereferences of
   several distinct globals.

   In this step the record is still a single global: making it per-domain is the
   next commit, and it is a two-line change precisely because everything below is
   already threaded. *)
and sched = {
  dom : Lwt_dls.token;
    (* The domain this scheduler belongs to, so that code holding a PROMISE can
       ask "is this scheduler mine?" without reading the per-domain slot: the
       promise already carries its scheduler, and a domain-identity comparison
       costs 9 instructions against the slot's 57. Immutable, set when the
       scheduler is created, which happens on its own domain. *)
  mutable storage : storage;
    (* Fiber-local storage in effect, restored around every waiter and before
       every task the run queue executes. *)
  mutable nesting : int;
    (* Depth of the resolution loop: Lwt's tail-call protection, and what
       [wakeup_later] tests to decide whether to defer. *)
  deferred : (unit -> unit) Queue.t;
    (* Callbacks deferred past the nesting cap; drained when the outermost loop
       exits. *)
  queue : Run_queue.t;
    (* Ready work: pauses, and the resumptions of direct-style layers. *)
  mutable paused : unit promise list;
  mutable paused_n : int;
  mutable pause_notifier : (int -> unit) option;
  on_reset : unit -> unit;
    (* Back-end state to clear at the start of [run], e.g. an I/O readiness
       table, which must not leak across independent scheduler runs.

       NOT mutable, and always [ignore], which is faithful: on the lean core this
       was a [ref] that nothing ever assigned and that the .mli did not expose,
       so [run] has always called [ignore]. The hook is kept rather than deleted
       because S2, which converts module initialisers into a per-loop setup and
       teardown, is exactly what needs it; making it settable belongs there and
       not here. *)
  mutable idle : sched -> bool;
    (* Blocks until external work may have arrived; [false] when there is
       nothing left to wait for. [Lwt_main] installs its own, driving the
       engine. It takes the scheduler so that serving pauses costs one lookup
       per [run] rather than one per lap. *)
}

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

(* Follow alias links to the canonical cell ([forward]'s reverse-merged proxy),
   with path compression. The overwhelmingly common case — a promise that was
   never forwarded — pays one [None] check. *)
let rec underlying (p : 'a promise) : 'a promise =
  match p.st with
  | Pending ({ link = Some p'; _ } as pe) ->
    let root = underlying p' in
    if root != p' then pe.link <- Some root;
    root
  | Pending { link = None; _ } | Fulfilled _ | Rejected _ -> p

let prj (p : 'a t) : 'a promise = underlying (Public_handle.prj p)

(* ------------------------------------------------------------------ *)
(* Fiber-local storage (Lwt.key)                                      *)
(* ------------------------------------------------------------------ *)

(* The storage is heterogeneous: it maps keys of unrelated types to their own
   values. Lwt obtains that without [Obj.magic] by giving each key a typed
   scratch CELL and storing, in the map, a closure that writes the value into
   that cell; [get] then calls the closure and reads the cell back.

   That trick is not thread-safe, and not for a subtle reason: the cell lives in
   the key, and a key is a single process-wide value (typically created once, at
   module initialisation). Two systhreads, and later two domains holding
   perfectly isolated storages, read and write the SAME cell, so one can observe
   the other's value, or [None].

   We use a sound universal type instead: each key owns a fresh exception
   CONSTRUCTOR, created with it, and the storage maps ids to [exn]. Injecting is
   applying the constructor, projecting is matching on it, and what one key
   injected can never be projected by another. No shared mutable state is left,
   this stays free of [Obj.magic], and it works on the OCaml 4.14 floor (unlike
   a [Type.Id]-based heterogeneous map, which needs 5.1).

   The constructor carries the ['a option], not the ['a]: a lookup then returns
   the option that was stored instead of building a fresh one, so it allocates
   nothing beyond what [Storage_map.find_opt] does, as before. Measured, per
   operation: this scheme allocates 9 words where the closure allocated 11 to
   store a binding, and 2 words in both schemes to read one back.

   The current storage is restored before every task the run queue executes (see
   [run_scheduler]) and around every waiter callback, so a value set with
   [with_value] survives suspensions. *)
type 'a key = {
  id : int;
  inject : 'a option -> exn;
  project : exn -> 'a option;
    (* [project (inject v) = v], and [project] applied to any other key's
       injection is [None], the constructor being fresh per key. *)
}

(* Atomic, not [incr]: two domains creating a key at the same time must not be
   handed the same id, since two keys sharing an id share a slot in the storage
   and would shadow each other. *)
let next_key_id = Atomic.make 0

let new_key (type a) () : a key =
  let module M = struct
    exception E of a option
  end in
  let id = Atomic.fetch_and_add next_key_id 1 in
  { id; inject = (fun v -> M.E v); project = (function M.E v -> v | _ -> None) }

let empty_storage : storage = Storage_map.empty

(* [idle]'s real default, [core_idle], needs the whole resolution machinery and
   is therefore defined far below; a scheduler record has to exist before that,
   since [bind] and friends reach for one. Hence one forward cell, set once, and
   read only on an idle lap. *)
let default_idle : (sched -> bool) ref = ref (fun _ -> false)

let new_sched () : sched =
  {
    dom = Lwt_dls.self_token ();
    storage = empty_storage;
    nesting = 0;
    deferred = Queue.create ();
    queue = Run_queue.create ();
    paused = [];
    paused_n = 0;
    pause_notifier = None;
    on_reset = ignore;
    idle = (fun s -> !default_idle s);
  }

(* S1 step 3: the record moves into a per-domain slot. This is the whole of the
   de-globalisation, and it is three lines, because step 2 already threaded the
   record through everything below: a hot path obtains it once at a public entry
   point and then works on fields.

   [Lwt_dls] is [Domain.DLS] on OCaml 5 and a plain cell on 4.14, so this
   degrades to exactly the previous global there, and under js_of_ocaml. *)
let sched_slot : sched Lwt_dls.t = Lwt_dls.new_key new_sched
let[@inline] self_sched () = Lwt_dls.get sched_slot

let get_from_storage key storage =
  match Storage_map.find_opt key.id storage with
  | Some e -> key.project e
  | None -> None

let modify_storage key value storage =
  match value with
  | Some _ -> Storage_map.add key.id (key.inject value) storage
  | None -> Storage_map.remove key.id storage

let get key =
  let sched = self_sched () in
  get_from_storage key sched.storage

let with_value key value f =
  let sched = self_sched () in
  let saved = sched.storage in
  sched.storage <- modify_storage key value saved;
  match f () with
  | r ->
    sched.storage <- saved;
    r
  | exception e ->
    sched.storage <- saved;
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

(* Runs the deferred callbacks; called at depth 1, so a [wakeup_later]
   performed by a deferred callback is itself deferred and picked up by the
   same drain. *)
let drain_deferred (sched : sched) =
  while not (Queue.is_empty sched.deferred) do
    (Queue.pop sched.deferred) ()
  done

let leave_resolution_loop (sched : sched) (storage_snapshot : storage) : unit =
  if sched.nesting = 1 then drain_deferred sched;
  sched.nesting <- sched.nesting - 1;
  sched.storage <- storage_snapshot

let run_in_resolution_loop (sched : sched) (f : unit -> unit) : unit =
  sched.nesting <- sched.nesting + 1;
  let storage_snapshot = sched.storage in
  f ();
  leave_resolution_loop sched storage_snapshot

(* Lwt.Private/abandon_wakeups: bail out of a resolution loop after an
   exception escaped it (https://github.com/ocsigen/lwt/issues/48). *)
let abandon_resolution_loop () =
  let sched = self_sched () in
  if sched.nesting <> 0 then begin
    sched.nesting <- 1;
    leave_resolution_loop sched empty_storage
  end

(* ------------------------------------------------------------------ *)
(* Promise primitives                                                 *)
(* ------------------------------------------------------------------ *)

exception Foreign_promise

(* The ownership check: a load and a pointer comparison against the scheduler the
   caller already holds. No per-domain lookup of its own, which is why it is
   placed at the MUTATION SITES rather than inside [underlying]: [underlying]
   receives no scheduler and would have to find one on every call, and section 13
   of the study measured that at about 85 instructions.

   The sites, enumerated rather than assumed, because an earlier version of this
   comment claimed four and the ownership test immediately found a fifth:

   - CHECKED, one per mutating operation: [fill_general] (writes [p.st]),
     [add_waiter], [set_on_cancel], [set_cancel_forward],
     [set_cancel_forward_list], [forward] (which moves waiters between TWO
     pending records, so both are checked), [both] (sets the cancel mode
     directly), the remover of a removable waiter (filters waiter lists), and
     [on_cancel] (writes [cancel_waiters]).
   - NOT checked because the promise is provably ours: [wait] and [no_cancel]
     set [Not_cancelable] on a promise [new_pending] has just created for us,
     so the owner is this scheduler by construction.
   - NOT checked, DELIBERATELY, and this is the one place where a foreign write
     can happen: the path compression in [underlying], [pe.link <- Some root].
     It is a mutation on a READ path, and [underlying] receives no scheduler, so
     checking it would cost a per-domain lookup on every projection, which is
     what this whole design exists to avoid. It is reachable in exactly the case
     the interface says is allowed: sharing a RESOLVED promise, whose cell may be
     a forwarded one whose root is resolved.

     It is benign, and by construction rather than by luck. The write happens
     only when the chain is longer than one hop, and [root] is the chain's unique
     terminal cell, so two domains compressing the same chain write [Some root]
     for the SAME root. A reader therefore sees either the old link or a new one,
     and both traversals reach the same cell: no interleaving can produce a wrong
     answer, and there is no update whose loss would matter. It is the classic
     racy-memoisation pattern, and OCaml 5 guarantees a non-atomic pointer read
     yields a value that was actually written, never a torn one.

     What it does cost is one entry in the TSan suppression file, since TSan will
     report it and will be right to: it is a data race, just a harmless one.
     Recorded for S6 rather than papered over.

   Reads are deliberately not checked: a resolved promise is immutable, so a
   foreign read cannot corrupt anything, and whether a foreign read of a pending
   promise's state is guaranteed to observe a fully initialised value is the open
   memory-model question of annexe C.3. *)
let[@inline] check_owner (sched : sched) (pe : 'a pending) : unit =
  if pe.owner != sched then raise Foreign_promise

(* The scheduler to work with, for the many operations that are handed a PENDING
   promise: it is the promise's own, and all that has to be established is that
   it belongs to this domain. That is one field load and a domain-identity
   comparison, 9 instructions, where reading the per-domain slot costs 57;
   measured, and it is why the field exists. Sound because a domain has exactly
   one scheduler, created by the slot's initialiser and never replaced, so
   [sched.dom] being ours means [sched] IS ours. *)
let[@inline] owner_sched (pe : 'a pending) : sched =
  let sched = pe.owner in
  if sched.dom <> Lwt_dls.self_token () then raise Foreign_promise;
  sched

let new_pending (sched : sched) : 'a t =
  inj
    { st =
        Pending
          {
            owner = sched;
            waiters = [];
            cancel_waiters = [];
            cancel = Cancel_self ignore;
            link = None;
          };
    }

(* Both lists are most-recently-added first and are run in THAT order: Lwt
   runs attached callbacks in reverse registration order (its callback trees
   prepend new nodes and are traversed front-first — LIFO). Observable, e.g.,
   through [Lwt_react.E.limit]'s flush racing a user [on_success]. *)
let run_resolution_callbacks (type a) (pe : a pending) (r : (a, exn) result) :
    unit =
  (match r with
  | Error Canceled -> List.iter (fun f -> f ()) pe.cancel_waiters
  | Ok _ | Error _ -> ());
  List.iter (fun w -> w r) pe.waiters

let fill_general (type a) (sched : sched) ~allow_deferring
    ~maximum_callback_nesting_depth (p : a t) (r : (a, exn) result) : unit =
  let p = prj p in
  match p.st with
  | Pending pe ->
    check_owner sched pe;
    p.st <- (match r with Ok v -> Fulfilled v | Error e -> Rejected e);
    if allow_deferring && sched.nesting >= maximum_callback_nesting_depth then
      Queue.push (fun () -> run_resolution_callbacks pe r) sched.deferred
    else run_in_resolution_loop sched (fun () -> run_resolution_callbacks pe r)
  | Fulfilled _ | Rejected _ -> ()

(* Internal resolution: immediate up to the default nesting depth. *)
let fill (type a) (sched : sched) (p : a t) (r : (a, exn) result) : unit =
  fill_general sched ~allow_deferring:true
    ~maximum_callback_nesting_depth:default_maximum_callback_nesting_depth p r

let add_waiter (type a) (sched : sched) (p : a t)
    (w : (a, exn) result -> unit) : unit =
  match (prj p).st with
  | Pending pe ->
    check_owner sched pe;
    pe.waiters <- w :: pe.waiters
  | Fulfilled v -> w (Ok v)
  | Rejected e -> w (Error e)

let set_on_cancel (type a) (sched : sched) (p : a t) (f : unit -> unit) : unit =
  match (prj p).st with
  | Pending pe ->
    check_owner sched pe;
    pe.cancel <- Cancel_self f
  | Fulfilled _ | Rejected _ -> ()

let cancel_gen (type a) (sched : sched) (p : a t) : unit =
  match (prj p).st with
  | Pending pe -> (
    match pe.cancel with
    | Not_cancelable -> ()
    | Cancel_self hook ->
      hook ();
      fill sched p (Error Canceled)
    | Cancel_forward fwd -> fwd sched)
  | Fulfilled _ | Rejected _ -> ()

(* [Lwt.cancel] is public and takes only the promise. *)
let cancel (type a) (p : a t) : unit = cancel_gen (self_sched ()) p

(* Mark [result] as forwarding cancellation to its current source [src] (no-op
   if [result] is no longer pending). Used by the derived combinators. *)
let set_cancel_forward (type a b) (sched : sched) (result : a t) (src : b t) :
    unit =
  match (prj result).st with
  | Pending pe ->
    check_owner sched pe;
    pe.cancel <- Cancel_forward (fun sched -> cancel_gen sched src)
  | Fulfilled _ | Rejected _ -> ()

(* Forward cancellation to a whole list of sources (Lwt's
   [propagate_cancel_to_several], used by choose/pick/join/all/both/nchoose). *)
let set_cancel_forward_list (type a b) (sched : sched) (result : a t)
    (ps : b t list) : unit =
  match (prj result).st with
  | Pending pe ->
    check_owner sched pe;
    pe.cancel <- Cancel_forward (fun sched -> List.iter (cancel_gen sched) ps)
  | Fulfilled _ | Rejected _ -> ()

(* ------------------------------------------------------------------ *)
(* Scheduler state                                                    *)
(* ------------------------------------------------------------------ *)


let enqueue (f : unit -> unit) : unit =
  let sched = self_sched () in
  Run_queue.push sched.queue (Thunk (sched.storage, f))

(* Shared [Ok ()] outcome: events and [pause] all resolve unit promises, so
   there is no need to allocate a fresh [Ok ()] each time. *)
let ok_unit : (unit, exn) result = Ok ()

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

  (* Process-wide by design (it decides what the whole program's Lwt machinery
     may catch), so it stays ONE cell rather than becoming per-domain. Atomic
     rather than a ref: with several domains, [set] from one and [run] from
     another is otherwise an unsynchronised access. The type is abstract and the
     module exposes only [set], so this is invisible from outside.

     [async_exception_hook] is the other process-wide setting and it CANNOT get
     the same treatment: [lwt.mli] declares it [(exn -> unit) ref], the ref value
     itself, and code across the ecosystem writes [Lwt.async_exception_hook := f].
     It therefore stays a ref, and the documented advice is to install it at
     start-up, before spawning domains, which is where [Domain.spawn] provides
     the publication. *)
  let v = Atomic.make handle_all_except_runtime
  let set f = Atomic.set v f
  let run e = (Atomic.get v) e
end

(* Apply the continuation of a bind, turning a synchronous exception into a
   rejected promise (when the exception filter allows catching it). Used on the
   deferred (pending) paths: fast paths apply [f] plainly, as Lwt does. *)
let apply (f : 'a -> 'b t) (v : 'a) : 'b t =
  try f v with e when Exception_filter.run e -> inj { st = Rejected e }

(* Forward the eventual result of [p'] into the pending promise [result], and
   make [result]'s cancellation follow [p'] (Lwt: cancelling a derived promise
   cancels its current source; the rejection then flows back through waiters). *)
(* [forward result p']: [result] resolves as [p'] does. When both are pending,
   this is Lwt's [make_into_proxy] with the merge direction REVERSED: [p']
   (the fresh promise a bind continuation just returned) is absorbed into
   [result] (the older, anchored one): its waiters are moved over (newest
   first, before [result]'s own — Lwt's merge/LIFO order), its cancel mode is
   taken (cancelling [result] must reach the {e current} source), and its cell
   becomes an alias of [result]. A tail-recursive [p >>= loop] therefore keeps
   a single live promise: each lap's fresh cell is dropped by its creator and
   collected, instead of chaining (which both a waiter-chain and Lwt's own
   merge direction would do, the latter saved upstream only by Lwt_main's
   per-lap [poll] compressing the proxy chain — our scheduler serves pauses
   without polling the main promise). *)
let forward (type a) (sched : sched) (result : a t) (p' : a t) : unit =
  let rp' = prj p' in
  match rp'.st with
  | Fulfilled v' -> fill sched result (Ok v')
  | Rejected e -> fill sched result (Error e)
  | Pending pe' -> (
    let r = prj result in
    if r == rp' then ()
    else
      match r.st with
      | Pending pe ->
        check_owner sched pe;
        check_owner sched pe';
        pe.waiters <- pe'.waiters @ pe.waiters;
        pe.cancel_waiters <- pe'.cancel_waiters @ pe.cancel_waiters;
        pe.cancel <- pe'.cancel;
        pe'.waiters <- [];
        pe'.cancel_waiters <- [];
        pe'.link <- Some r
      | Fulfilled _ | Rejected _ ->
        (* [result] already resolved (it was cancelled): drop the link; a
           resolution of [p'] is then a no-op, as Lwt's. *)
        add_waiter sched p' (fun r' -> fill sched result r'))

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
  | Pending pe ->
    let sched = owner_sched pe in
    let result = new_pending sched in
    set_cancel_forward sched result p;
    let saved = sched.storage in
    add_waiter sched p (fun r ->
      let outer = sched.storage in
      sched.storage <- saved;
      (match r with
      | Ok v -> forward sched result (apply f v)
      | Error e -> fill sched result (Error e));
      sched.storage <- outer);
    result

(* Unlike {!bind}, [map] captures a synchronous exception of [f] into a rejected
   promise even on the fulfilled fast path (Lwt's deliberate asymmetry: map's [f]
   is a plain value function). Defined directly — no intermediate promise. *)
let map (type a b) (f : a -> b) (p : a t) : b t =
  match (prj p).st with
  | Fulfilled v -> (
    try return (f v) with e when Exception_filter.run e -> inj { st = Rejected e })
  | Rejected e -> inj { st = Rejected e }
  | Pending pe ->
    let sched = owner_sched pe in
    let result = new_pending sched in
    set_cancel_forward sched result p;
    let saved = sched.storage in
    add_waiter sched p (fun r ->
      let outer = sched.storage in
      sched.storage <- saved;
      (match r with
      | Ok v -> (
        try fill sched result (Ok (f v))
        with e when Exception_filter.run e -> fill sched result (Error e))
      | Error e -> fill sched result (Error e));
      sched.storage <- outer);
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
  | Pending pe ->
    let sched = owner_sched pe in
    let result = new_pending sched in
    set_cancel_forward sched result p;
    let saved = sched.storage in
    add_waiter sched p (fun r ->
      let outer = sched.storage in
      sched.storage <- saved;
      forward sched result (match r with Ok v -> apply g v | Error e -> apply h e);
      sched.storage <- outer);
    result

let catch (f : unit -> 'a t) (h : exn -> 'a t) : 'a t = try_bind f return h

(* Lwt's [both] waits for {e both} promises even when one is already rejected
   (the result stays pending until the other resolves), then rejects with the
   first rejection encountered. Callback-counting, like [join] below. *)
let both (a : 'a t) (b : 'b t) : ('a * 'b) t =
  let sched = self_sched () in
  let result = new_pending sched in
  (match (prj result).st with
  | Pending pe ->
    check_owner sched pe;
    pe.cancel <-
      Cancel_forward
        (fun sched ->
          cancel_gen sched a;
          cancel_gen sched b)
  | Fulfilled _ | Rejected _ -> ());
  let va = ref None
  and vb = ref None
  and failure = ref None
  and remaining = ref 2 in
  let settle sched =
    decr remaining;
    if !remaining = 0 then
      match (!failure, !va, !vb) with
      | Some e, _, _ -> fill sched result (Error e)
      | None, Some x, Some y -> fill sched result (Ok (x, y))
      | None, _, _ -> ()
  in
  add_waiter sched a (fun r ->
    (match r with
    | Ok x -> va := Some x
    | Error e -> if !failure = None then failure := Some e);
    settle sched);
  add_waiter sched b (fun r ->
    (match r with
    | Ok y -> vb := Some y
    | Error e -> if !failure = None then failure := Some e);
    settle sched);
  result

(* Removable waiters (Lwt's "explicitly removable callbacks"). One shared cell
   holds [f]; a small wrapper is added to every promise in [ps]. The first
   resolution runs [f] once and REMOVES the wrapper from the still-pending
   promises, dropping [f] and its captures. Without this, a long-lived promise
   repeatedly passed to [choose]/[pick] (e.g. a server's shutdown promise, one
   pick per request/connection) accumulates dead waiters without bound — the
   leak classic Lwt prevents with [clear_explicitly_removable_callback_cell].
   Returns the remover, for mirrors ([protected]) that must detach on
   cancellation; calling it after the waiter fired is a no-op. *)
let add_removable_waiter_to_each_of (sched : sched) (ps : 'a t list)
    (f : ('a, exn) result -> unit) : unit -> unit =
  let cell = ref (Some f) in
  let rec wrapper r =
    match !cell with
    | None -> ()
    | Some f ->
      remove ();
      f r
  and remove () =
    match !cell with
    | None -> ()
    | Some _ ->
      cell := None;
      (* The promise being resolved is no longer [Pending], so this never
         mutates a waiter list while [run_resolution_callbacks] iterates it. *)
      List.iter
        (fun p ->
          match (prj p).st with
          | Pending pe ->
            check_owner sched pe;
            pe.waiters <- List.filter (fun w -> w != wrapper) pe.waiters
          | Fulfilled _ | Rejected _ -> ())
        ps
  in
  List.iter (fun p -> add_waiter sched p wrapper) ps;
  remove

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
  let sched = self_sched () in
  if ps = [] then
    invalid_arg "Lwt.choose [] would return a promise that is pending forever";
  match select_resolved ps with
  | Some p -> p
  | None ->
    let result = new_pending sched in
    set_cancel_forward_list sched result ps;
    (* The removable waiter fires once (first resolution wins) and detaches
       from the losers, so they don't retain a dead waiter. *)
    let (_ : unit -> unit) =
      add_removable_waiter_to_each_of sched ps (fun r ->
        fill sched result r)
    in
    result

let pick (ps : 'a t list) : 'a t =
  let sched = self_sched () in
  if ps = [] then
    invalid_arg "Lwt.pick [] would return a promise that is pending forever";
  match select_resolved ps with
  | Some p ->
    List.iter (fun q -> if q != p then cancel q) ps;
    p
  | None ->
    let result = new_pending sched in
    set_cancel_forward_list sched result ps;
    (* By the time the waiter runs it has detached from the losers, so the
       cancellations below cannot re-enter it; the winner is already resolved,
       so cancelling the whole list only reaches the losers. Cancel BEFORE
       resolving the result, so the losers' cancellation callbacks run first
       (Lwt's ordering). *)
    let (_ : unit -> unit) =
      add_removable_waiter_to_each_of sched ps (fun r ->
        List.iter (cancel_gen sched) ps;
        fill sched result r)
    in
    result

(* ------------------------------------------------------------------ *)
(* Yielding and timers                                                *)
(* ------------------------------------------------------------------ *)

(* Lwt's pause protocol: paused promises gather in a queue served on the next
   scheduler tick (or by an explicit [wakeup_paused], as Lwt_main does), with a
   count and an optional notifier — conformant with Lwt.{pause,paused_count,
   wakeup_paused,register_pause_notifier,abandon_paused}. *)
let pause () : unit t =
  let sched = self_sched () in
  let p = new_pending sched in
  sched.paused <- Public_handle.prj p :: sched.paused;
  sched.paused_n <- sched.paused_n + 1;
  (match sched.pause_notifier with Some f -> f sched.paused_n | None -> ());
  p

let paused_count () = (self_sched ()).paused_n

(* The scheduler is passed in: this is on the pause-serving path, reached once
   per lap from the idle hook, which already holds the record. *)
let wakeup_paused_sched (sched : sched) =
  (* Snapshot first: a [pause] performed while waking lands in the next batch. *)
  let ps = List.rev sched.paused in
  sched.paused <- [];
  sched.paused_n <- 0;
  List.iter (fun p -> fill sched (inj p) ok_unit) ps

(* [Lwt.wakeup_paused] is public and takes no argument. *)
let wakeup_paused () = wakeup_paused_sched (self_sched ())

let register_pause_notifier f = (self_sched ()).pause_notifier <- Some f

let abandon_paused () =
  let sched = self_sched () in
  sched.paused <- [];
  sched.paused_n <- 0

(* ------------------------------------------------------------------ *)
(* The scheduler loop                                                 *)
(* ------------------------------------------------------------------ *)

(* Idle-wait hook: called when the run queue is empty, to advance the world by
   exactly one lap. Returns [true] if it may have produced new work (the loop
   continues), [false] if there is nothing left to wait for (scheduler is done).
   A backend ([Lwt_main], driving [Lwt_engine]) installs its own with
   [set_idle]; it serves one paused batch per lap, interleaved with one engine
   iteration, mirroring classic Lwt's loop (see [Lwt_main.run]).

   The bare core has no event source, so a lap is just one paused batch: serve
   it and report progress, otherwise — empty run queue, no pauses — the
   scheduler is done. Serving pauses one batch per lap (rather than draining
   every pause generation between laps) is what keeps a backend's engine
   iterations from starving under a sustained stream of pauses. *)
let core_idle (sched : sched) : bool =
  if sched.paused_n > 0 then begin
    wakeup_paused_sched sched;
    true
  end
  else false

(* Close the forward reference opened next to [new_sched]: from here on a fresh
   scheduler serves its own pauses. *)
let () = default_idle := core_idle

(* [Lwt.Private.scheduler_set_idle] keeps its historical [unit -> bool] shape,
   so back ends are unaffected; the record is threaded on our side only. *)
let set_idle (f : unit -> bool) : unit = (self_sched ()).idle <- fun _ -> f ()

let rec run_scheduler (sched : sched) : unit =
  if Run_queue.is_empty sched.queue then begin
    (* Run queue drained: advance the world by one lap through the idle hook.
       The hook serves at most one paused batch (and, for a backend, one engine
       iteration) before returning here — so under a sustained stream of pauses
       the engine still runs once per batch instead of after the whole pause
       cascade settles. Returns [false] only when there is nothing left to do. *)
    if sched.idle sched then run_scheduler sched
  end
  else begin
    (match Run_queue.pop sched.queue with
    | Thunk (s, f) ->
      sched.storage <- s;
      f ());
    run_scheduler sched
  end

let run (type a) (main : unit -> a t) : a =
  (* ONE lookup for the whole run: everything below is threaded. *)
  let sched = self_sched () in
  sched.on_reset ();
  sched.storage <- empty_storage;
  let outcome = ref None in
  (* When [main ()] itself raises synchronously, capture its raw backtrace at
     the boundary so we can re-raise it faithfully below with
     [Printexc.raise_with_backtrace] instead of resetting the trace with a bare
     [raise]. The monadic path keeps no stack to preserve (each [bind] reboxes
     the exception into a rejected promise), so it leaves [raw_bt] at [None].
     This is off the hot path: [run] is entered once per event-loop run, and
     the capture only happens on the exception path. *)
  let raw_bt = ref None in
  enqueue (fun () ->
    match main () with
    | p -> (
      match (prj p).st with
      | Fulfilled v -> outcome := Some (Ok v)
      | Rejected e -> outcome := Some (Error e)
      | Pending _ -> add_waiter sched p (fun r -> outcome := Some r))
    | exception e when Exception_filter.run e ->
      raw_bt := Some (Printexc.get_raw_backtrace ());
      outcome := Some (Error e));
  run_scheduler sched;
  match !outcome with
  | Some (Ok v) -> v
  | Some (Error e) ->
    (* Prefer the backtrace captured synchronously at the boundary ([main ()]
       raised). Otherwise (the main promise was rejected asynchronously — e.g.
       a [Lwt_direct] task reboxed an exception into a rejected promise) fall
       back to the runtime's current backtrace buffer, which may still hold the
       relevant trace. Either way re-raise *with* a backtrace rather than a
       bare [raise], which would reset it to this point. *)
    let bt =
      match !raw_bt with Some bt -> bt | None -> Printexc.get_raw_backtrace ()
    in
    Printexc.raise_with_backtrace e bt
  | None ->
    failwith
      "Lwt.Private.scheduler_run: scheduler stalled before the main promise \
       resolved"

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
  let sched = self_sched () in
  let p = new_pending sched in
  (match (prj p).st with
  | Pending pe -> pe.cancel <- Not_cancelable
  | Fulfilled _ | Rejected _ -> ());
  (p, u_of_t p)

let task () =
  let sched = self_sched () in
  let p = new_pending sched in
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
  | Pending pe ->
    (* [wakeup] never defers: callbacks run now whatever the nesting. *)
    fill_general (owner_sched pe) ~allow_deferring:false
      ~maximum_callback_nesting_depth:default_maximum_callback_nesting_depth p r
  | Rejected Canceled -> ()
  | Fulfilled _ | Rejected _ -> invalid_arg fname

(* [wakeup_later]: callbacks are deferred whenever some resolution is already
   in progress (Lwt resolves with [~maximum_callback_nesting_depth:1]). *)
let wakeup_later_named (fname : string) (u : 'a u) (r : ('a, exn) result) :
    unit =
  let p = t_of_u u in
  match (prj p).st with
  | Pending pe ->
    fill_general (owner_sched pe) ~allow_deferring:true
      ~maximum_callback_nesting_depth:1 p r
  | Rejected Canceled -> ()
  | Fulfilled _ | Rejected _ -> invalid_arg fname

let wakeup (u : 'a u) v = wakeup_named "Lwt.wakeup" u (Ok v)
let wakeup_exn (u : 'a u) e = wakeup_named "Lwt.wakeup_exn" u (Error e)
let wakeup_result u r = wakeup_named "Lwt.wakeup_result" u r
let wakeup_later (u : 'a u) v = wakeup_later_named "Lwt.wakeup_later" u (Ok v)

let wakeup_later_exn (u : 'a u) e =
  wakeup_later_named "Lwt.wakeup_later_exn" u (Error e)

let wakeup_later_result u r = wakeup_later_named "Lwt.wakeup_later_result" u r

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

(* [join]/[all] are callback-counting: they resolve {e immediately}
   when every promise is already resolved — Lwt code observes the state right
   after the call — and otherwise settle when the last pending one does. On
   rejection they still wait for all, then reject with the {e first} rejection
   encountered (already-rejected ones in list order first), as Lwt. *)
let join (ps : unit t list) : unit t =
  let sched = self_sched () in
  match ps with
  | [] -> return_unit
  | _ ->
    let result = new_pending sched in
    set_cancel_forward_list sched result ps;
    let remaining = ref (List.length ps) in
    let failure = ref None in
    List.iter
      (fun p ->
        add_waiter sched p (fun r ->
          (match r with
          | Error e -> if !failure = None then failure := Some e
          | Ok () -> ());
          decr remaining;
          if !remaining = 0 then
            match !failure with
            | None -> fill sched result (Ok ())
            | Some e -> fill sched result (Error e)))
      ps;
    result

let all (ps : 'a t list) : 'a list t =
  let sched = self_sched () in
  match ps with
  | [] -> return []
  | _ ->
    let result = new_pending sched in
    set_cancel_forward_list sched result ps;
    let n = List.length ps in
    let values = Array.make n None in
    let remaining = ref n in
    let failure = ref None in
    List.iteri
      (fun i p ->
        add_waiter sched p (fun r ->
          (match r with
          | Ok v -> values.(i) <- Some v
          | Error e -> if !failure = None then failure := Some e);
          decr remaining;
          if !remaining = 0 then
            match !failure with
            | None ->
              (* Every slot is [Some] once [remaining] is 0 with no failure. *)
              fill sched result
                (Ok (Array.to_list values |> List.filter_map Fun.id))
            | Some e -> fill sched result (Error e)))
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
  let sched = self_sched () in
  if ps = [] then
    invalid_arg "Lwt.nchoose [] would return a promise that is pending forever";
  if any_resolved ps then (
    match nchoose_result ps with Ok vs -> return vs | Error e -> fail e)
  else begin
    let result = new_pending sched in
    set_cancel_forward_list sched result ps;
    (* Fires once on the first resolution, snapshotting the fulfilled ones,
       and detaches from the still-pending promises. *)
    let (_ : unit -> unit) =
      add_removable_waiter_to_each_of sched ps (fun _ ->
        fill sched result (nchoose_result ps))
    in
    result
  end

(* Like [nchoose], but also returns the promises still pending at that point. *)
let nchoose_split (type a) (ps : a t list) : (a list * a t list) t =
  let sched = self_sched () in
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
    let result = new_pending sched in
    set_cancel_forward_list sched result ps;
    let (_ : unit -> unit) =
      add_removable_waiter_to_each_of sched ps (fun _ ->
        fill sched result (snapshot ()))
    in
    result
  end

let npick (ps : 'a t list) : 'a list t =
  let sched = self_sched () in
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
    let result = new_pending sched in
    set_cancel_forward_list sched result ps;
    (* Detached from the losers before it runs, so [cancel_pending] cannot
       re-enter it. Snapshot before cancelling: the cancellations must not
       become the result. *)
    let (_ : unit -> unit) =
      add_removable_waiter_to_each_of sched ps (fun _ ->
        let r = nchoose_result ps in
        cancel_pending ();
        fill sched result r)
    in
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
  let sched = self_sched () in
  let saved = sched.storage in
  fun x ->
    let outer = sched.storage in
    sched.storage <- saved;
    hooked f x;
    sched.storage <- outer

let on_any p f g =
  let sched = self_sched () in
  let f = with_registration_storage f and g = with_registration_storage g in
  add_waiter sched p (function Ok v -> f v | Error e -> g e)

let on_success p f =
  let sched = self_sched () in
  let f = with_registration_storage f in
  add_waiter sched p (function Ok v -> f v | Error _ -> ())

let on_failure p f =
  let sched = self_sched () in
  let f = with_registration_storage f in
  add_waiter sched p (function Ok _ -> () | Error e -> f e)

let on_termination p f =
  let sched = self_sched () in
  let f = with_registration_storage f in
  add_waiter sched p (fun _ -> f ())

(* Runs [f] when the promise is rejected with [Canceled] — whether cancelled
   directly or through propagation. Cancel callbacks run {e before} ordinary
   waiters (Lwt's ordering guarantee); see [fill]. *)
let on_cancel (type a) (p : a t) (f : unit -> unit) : unit =
  let sched = self_sched () in
  let f = with_registration_storage f in
  match (prj p).st with
  | Pending pe ->
    check_owner sched pe;
    pe.cancel_waiters <- f :: pe.cancel_waiters
  | Rejected Canceled -> f ()
  | Fulfilled _ | Rejected _ -> ()

(* Lwt's [async] is fire-and-forget and runs [f ()] {e immediately} on the
   caller's stack (its callbacks register before the caller's next action —
   tests rely on this); a synchronous raise or a rejection goes to
   [async_exception_hook]. *)
let async (f : unit -> unit t) : unit =
  let p = try f () with e when Exception_filter.run e -> fail e in
  on_failure p (fun e -> !async_exception_hook e)

(* Same immediate-run semantics for [dont_wait], with a user handler. *)
let dont_wait (f : unit -> unit t) (handler : exn -> unit) : unit =
  let p = try f () with e when Exception_filter.run e -> fail e in
  on_failure p handler

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
  let sched = self_sched () in
  match (prj p).st with
  | Fulfilled _ | Rejected _ -> p
  | Pending _ ->
    let result = new_pending sched in
    (match (prj result).st with
    | Pending pe -> pe.cancel <- Not_cancelable
    | Fulfilled _ | Rejected _ -> ());
    add_waiter sched p (fun r -> fill sched result r);
    result
let protected (type a) (p : a t) : a t =
  let sched = self_sched () in
  match (prj p).st with
  | Fulfilled _ | Rejected _ -> p
  | Pending _ ->
    (* Cancelable, leaves [p] untouched. The mirror waiter is removable:
       cancelling the mirror detaches it from [p], so repeated
       [protected]+[cancel] against a long-lived [p] does not accumulate dead
       waiters. *)
    let result = new_pending sched in
    let remove =
      add_removable_waiter_to_each_of sched [ p ] (fun r ->
        fill sched result r)
    in
    set_on_cancel sched result remove;
    result

(* [wrap_in_cancelable p] mirrors [p] and is cancelable even if [p] is not:
   cancelling first forwards to [p] (no-op if [p] is not cancelable — its
   rejection, if any, flows into the mirror), then rejects the mirror. *)
let wrap_in_cancelable (type a) (p : a t) : a t =
  let sched = self_sched () in
  match (prj p).st with
  | Fulfilled _ | Rejected _ -> p
  | Pending _ ->
    let result = new_pending sched in
    let remove =
      add_removable_waiter_to_each_of sched [ p ] (fun r ->
        fill sched result r)
    in
    (* Cancel [p] first (if it is cancelable, its rejection flows into the
       mirror through the still-attached waiter, as before); then detach, so a
       non-cancelable long-lived [p] is not left holding a dead waiter. *)
    set_on_cancel sched result (fun () ->
      cancel p;
      remove ());
    result

(* ------------------------------------------------------------------ *)
(* Operators, ppx support, sequence tasks, tracing/debug, Private     *)
(* ------------------------------------------------------------------ *)

let ( <?> ) a b = choose [ a; b ]
let ( <&> ) a b = join [ a; b ]
let ( =<< ) f p = bind p f
let ( =|< ) f p = map f p

let wrap1 f = fun x -> (try return (f x) with e -> fail e)
let wrap2 f = fun x y -> (try return (f x y) with e -> fail e)
let wrap3 f = fun x y z -> (try return (f x y z) with e -> fail e)
let wrap4 f = fun a b c d -> (try return (f a b c d) with e -> fail e)
let wrap5 f = fun a b c d e -> (try return (f a b c d e) with ex -> fail ex)
let wrap6 f = fun a b c d e g -> (try return (f a b c d e g) with ex -> fail ex)

let wrap7 f =
 fun a b c d e g h -> (try return (f a b c d e g h) with ex -> fail ex)

external reraise : exn -> 'a = "%reraise"

(* The tracing/backtrace variants take location metadata (name, line, an
   exception-rewriting function) and otherwise delegate to the plain
   combinators: this core does not maintain Lwt's backtraces. *)
let backtrace_bind _name _line _add_loc p f = bind p f
let backtrace_catch _name _line _add_loc f h = catch f h
let backtrace_finalize _name _line _add_loc f g = finalize f g
let backtrace_try_bind _name _line _add_loc f g h = try_bind f g h

module Let_syntax = struct
  module Let_syntax = struct
    let return = return
    let map p ~f = map f p
    let bind p ~f = bind p f
    let both = both

    module Open_on_rhs = struct end
  end
end

module Infix = struct
  let ( >>= ) = bind
  let ( =<< ) f p = bind p f
  let ( >|= ) p f = map f p
  let ( =|< ) f p = map f p
  let ( <&> ) a b = join [ a; b ]
  let ( <?> ) a b = choose [ a; b ]

  module Let_syntax = Let_syntax.Let_syntax
end

(* [add_task_r]/[add_task_l]: a task whose resolver is added to an
   Lwt_sequence, removed on cancel (the documented behavior in lwt.mli). *)
let add_task_r seq =
  let p, r = task () in
  let node = Lwt_sequence.add_r r seq in
  on_cancel p (fun () -> Lwt_sequence.remove node);
  p

let add_task_l seq =
  let p, r = task () in
  let node = Lwt_sequence.add_l r seq in
  on_cancel p (fun () -> Lwt_sequence.remove node);
  p

(* Tracing is a no-op here (this core does not emit Lwt's span events). *)
let with_tracing_context _name f = f ()

(* Compares the {e constructor} of the expected state with the promise's. *)
let debug_state_is expected p =
  return
    (match (expected, state p) with
    | Return _, Return _ | Fail _, Fail _ | Sleep, Sleep -> true
    | _ -> false)

(* Bail out of the resolution loop after an exception escaped it (issue #48);
   the paused queue is dropped separately by [abandon_paused]. *)
let abandon_wakeups () = abandon_resolution_loop ()

module Private = struct
  type nonrec storage = storage

  module Sequence_associated_storage = struct
    let get_from_storage = get_from_storage
    let modify_storage = modify_storage
    let empty_storage = empty_storage

    (* Was [val current_storage : storage ref]. A [ref] VALUE cannot survive
       per-domain scheduler state: whoever read it would keep the cell of the
       domain that loaded the module, for good. See the audit, section 2.1. *)
    let get_current_storage () = (self_sched ()).storage
    let set_current_storage s = (self_sched ()).storage <- s
  end

  let tracing_context : string key = new_key ()

  (* Effect-scheduler hooks (this core is engine-free): [Lwt_main.run] drives
     the run queue and installs the engine-blocking idle hook through these. *)
  let scheduler_run = run
  let scheduler_set_idle = set_idle
  let scheduler_queue_is_empty () = Run_queue.is_empty (self_sched ()).queue
  let scheduler_enqueue = enqueue
end

