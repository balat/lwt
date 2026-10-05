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

(* Growable ring buffer used as the run queue. Stdlib.Queue allocates a list
   cell on every push; this allocates only when it has to grow. Capacity is kept
   a power of two so indexing uses [land] instead of [mod]. A sentinel fills
   freed slots so consumed continuations are not retained. Generic over its
   elements, because the tasks it holds mention promises, which are declared
   further down. *)
module Run_queue = struct
  type 'a t = {
    mutable a : 'a array;
    mutable head : int;
    mutable len : int;
    sentinel : 'a;
  }

  let create sentinel = { a = Array.make 16 sentinel; head = 0; len = 0; sentinel }
  let is_empty q = q.len = 0
  let length q = q.len

  let grow q =
    let cap = Array.length q.a in
    let a' = Array.make (2 * cap) q.sentinel in
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
    q.a.(q.head) <- q.sentinel;
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
  mutable newest : 'a waiter;
  mutable oldest : 'a waiter;
    (* The waiters attached to this promise, as an intrusive doubly-linked
       list between its two ends, [No_waiter] standing for "none" at an end as
       in a link. A fresh promise has [No_waiter] at both ends.

       Why not a plain list: a waiter list must support adding (every
       suspended bind), unlinking one node (the removable waiters of [choose],
       [pick], [protected] and friends, which detach from the losers when the
       winner resolves) and splicing one list into another ([forward], when a
       continuation returns a pending promise). On a plain list the last two
       are linear in the list and the first of them reallocates it. Measured:
       ten thousand concurrent picks against one long-lived promise cost 50
       microseconds per resolution against 1.4 on the historical core, and ten
       thousand binds returning one shared promise carrying ten thousand
       waiters cost 89 microseconds per merge against 0.26. The historical core
       buys its O(1) with callback trees, a removable cell shared across
       promises and a throttled cleanup; here every operation is O(1) by
       construction, for one more word per waiter (a node is four words where a
       list cell is three) and one more word per pending promise (the second
       end).

       Why a variant with an inline record, and two ends rather than a circular
       list: the node IS the variant block, so a link costs no box and
       [No_waiter] is an immediate; and a node is always built with both its
       links in hand, where the first node of a circular list would need a
       recursive value definition, which OCaml compiles through a dummy block
       and a copy, measured at 26 ns against 7 for a plain allocation.

       Waiters run most recently added first, Lwt's order; see
       [run_resolution_callbacks]. A waiter RECEIVES the scheduler rather than
       capturing it: capturing would cost one word per waiter, measured at +5
       words per suspended bind across the combinators (study, section 13.4),
       and a promise's callbacks always run on the domain that installed them,
       which the type then states. *)
  mutable cancel_waiters : (unit -> unit) list;
    (* [on_cancel] callbacks; run BEFORE [waiters] when rejected with
       [Canceled] (Lwt's ordering guarantee). Rarely more than one and never
       unlinked individually, so a plain list is right here. *)
  mutable cancel : cancel_mode;
    (* How this promise reacts to [cancel] while pending (Lwt's model). *)
  mutable link : 'a promise option;
    (* [Some root]: this cell is an alias of [root], Lwt's proxy. When a pending
       [bind] continuation returns a fresh pending promise, [forward] moves the
       fresh promise's waiters onto the (older, anchored) result and links the
       fresh cell to it, exactly as Lwt's [make_into_proxy] makes the promise
       the continuation returned a proxy of the outer one: the fresh cell
       becomes garbage as soon as its creator drops it, while the anchored
       result, the one the outside world holds, never grows a chain. [prj]
       follows links with path compression, so all reads and writes act on the
       root. *)
}

(* A node of a promise's waiter list, or the absence of one. [run] is the
   waiter proper. A linked node has [No_waiter] as [newer] only when it is the
   newest and as [older] only when it is the oldest; an unlinked node has
   [No_waiter] at both, which [unlink_waiter] tells apart from a node alone in
   its list by looking at the list's newest end. *)
and 'a waiter =
  | No_waiter
  | Waiter of {
      run : ('a, exn) result -> unit;
      mutable newer : 'a waiter;
      mutable older : 'a waiter;
    }

(* Lwt's cancellation model:
   - [Cancel_self hook]: directly cancelable ([task], and the mirrors of
     [protected] and [wrap_in_cancelable]): [cancel] rejects it with [Canceled]
     after running the hook;
   - [Cancel_forward collect]: a derived promise (e.g. a [bind] result):
     [cancel] goes on to its current sources, and the rejection then flows back
     through the ordinary waiter chain (the promise is not rejected directly).
     [collect] adds to the list it is given what its sources' cancellation
     finds, see [collect_cancelable];
   - [Not_cancelable]: [wait]-created (and [no_cancel]) promises ignore [cancel]. *)
and cancel_mode =
  | Cancel_self of (unit -> unit)
  | Cancel_forward of (sched -> cancel_found list -> cancel_found list)
  | Not_cancelable

(* A promise [cancel] found to cancel, already marked [Canceled], with the hook
   to run and the waiters to deliver to once the search is over. *)
and cancel_found = Found : 'a pending * (unit -> unit) -> cancel_found

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
  prng : Random.State.t Lazy.t;
    (* What [choose] and [pick] draw from, among promises already resolved. The
       scheduler's own, as in Lwt: drawing from the global [Random] consumed the
       program's numbers and made a program's seed change which promise was
       chosen. Per scheduler, hence per domain, since a [Random.State.t] is not
       safe to share. Seeded with a constant, as Lwt's was. *)
    (* Callbacks deferred past the nesting cap; drained when the outermost loop
       exits. *)
  queue : task Run_queue.t;
    (* Ready work: pauses, the resumptions of direct-style layers, and the
       resolutions performed during an engine iteration. *)
  mutable paused : unit promise list;
  mutable paused_n : int;
  mutable yielded : task list;
    (* Resumptions that asked for the next lap, [Lwt_direct.yield]'s: served
       with the paused batch, after one engine iteration, so that a task
       yielding in a loop starves nothing. Most recent first, like [paused]. *)
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
  mutable drainer_gen : int;
    (* Generation of the pass currently draining the run queue, i.e. of the
       fiber running [run_scheduler]. Bumped by [retire_drainer] when a
       direct-style layer suspends that fiber, so that once resumed it ends
       after its task instead of competing with the new drainer. *)
  mutable defer_fills : bool;
    (* While set, resolutions queue their callbacks as tasks instead of running
       them on the spot; see [defer_fills]. *)
  mutable cascades : cascade list;
    (* The cascades in progress on the current fiber's stack, innermost first,
       when they have more than one waiter. Part of the state a suspension
       takes with it; see [suspend]. *)
  mutable suspension_forbidden : int;
    (* Depth of the regions in which suspending the current task is an error;
       see [no_suspend]. *)
}

(* A resolution cascade in progress: the result being delivered and the
   waiters still to receive it. [run_resolution_callbacks] iterates through
   the cursor rather than through the list itself so that a direct-style layer
   suspending the fiber in the middle of it can hand the remaining waiters to
   the run queue ([suspend]), instead of freezing them in the continuation. *)
and cascade =
  | Cascade : ('a, exn) result * 'a waiter ref -> cascade
  | Cancels of (unit -> unit) list ref
    (* The [on_cancel] callbacks still to run, for a cancellation. *)

(* A ready unit of work in the run queue. A [Thunk] carries the storage to
   restore before it runs. [Paused] and [Fill] are the loop's own events served
   as tasks (see [serve_paused_sched] and [defer_fills]), and [Detached] one
   waiter detached from a suspended cascade (see [suspend]); as variants rather
   than thunks they cost one small block each and no closure. *)
and task =
  | Thunk of storage * (unit -> unit)
  | Paused of unit promise
  | Fill : 'a pending * ('a, exn) result -> task
  | Detached : ('a, exn) result * (('a, exn) result -> unit) -> task

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
(* In two passes, both tail-recursive: find the root, then point every link on
   the way at it. A recursion that compressed on the way back used the stack
   in proportion to the chain, and a chain of a million aliases (each
   continuation of a bind returning the previous accumulator) overflowed it on
   OCaml 4.14, as in the historical core. As before, a link is rewritten only
   when it does not already name the root, and only with [Some root]. *)
let rec root_of (p : 'a promise) : 'a promise =
  match p.st with
  | Pending { link = Some p'; _ } -> root_of p'
  | Pending { link = None; _ } | Fulfilled _ | Rejected _ -> p

let rec compress (p : 'a promise) (root : 'a promise) =
  match p.st with
  | Pending ({ link = Some p'; _ } as pe) when p' != root ->
    pe.link <- Some root;
    compress p' root
  | Pending _ | Fulfilled _ | Rejected _ -> ()

let underlying (p : 'a promise) : 'a promise =
  match p.st with
  | Pending { link = Some _; _ } ->
    let root = root_of p in
    compress p root;
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
    prng = lazy (Random.State.make [||]);
    queue = Run_queue.create (Thunk (empty_storage, ignore));
    paused = [];
    yielded = [];
    paused_n = 0;
    pause_notifier = None;
    on_reset = ignore;
    idle = (fun s -> !default_idle s);
    drainer_gen = 0;
    defer_fills = false;
    cascades = [];
    suspension_forbidden = 0;
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
    sched.cascades <- [];
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
     directly), the remover of a removable waiter (unlinks its node from each
     list), and [on_cancel] (writes [cancel_waiters]).
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

(* The waiter list primitives. Both ends are at hand in the pending record, so
   linking the newest, unlinking any node and splicing one list behind another
   are a few pointer writes each, and none allocates beyond the node itself. *)

let link_waiter (pe : 'a pending) (run : ('a, exn) result -> unit) : 'a waiter =
  let node = Waiter { run; newer = No_waiter; older = pe.newest } in
  (match pe.newest with
  | Waiter newest -> newest.newer <- node
  | No_waiter -> pe.oldest <- node);
  pe.newest <- node;
  node

(* [unlink_waiter pe node] takes [node] out of [pe]'s list. Harmless when there
   is no node, and when [node] was already unlinked: its links are then both
   [No_waiter] and it is not the list's newest, which tells it apart from a
   node alone in its list, whose links are also both [No_waiter]. *)
let unlink_waiter (pe : 'a pending) (node : 'a waiter) : unit =
  match node with
  | No_waiter -> ()
  | Waiter w ->
    let linked =
      match w.newer with Waiter _ -> true | No_waiter -> pe.newest == node
    in
    if linked then begin
      (match w.newer with
      | Waiter newer -> newer.older <- w.older
      | No_waiter -> pe.newest <- w.older);
      (match w.older with
      | Waiter older -> older.newer <- w.newer
      | No_waiter -> pe.oldest <- w.newer);
      w.newer <- No_waiter;
      w.older <- No_waiter
    end

(* [splice_waiters ~into ~from] moves every waiter of [from] behind those of
   [into] and empties [from]. Afterwards [into]'s own waiters run first, newest
   to oldest, then [from]'s: Lwt's order when a bind result absorbs the promise
   its continuation returned (the outer promise's callbacks before the returned
   promise's, see [Pending_callbacks.merge_callbacks] in the historical core). *)
let splice_waiters ~(into : 'a pending) ~(from : 'a pending) : unit =
  (match from.newest with
  | No_waiter -> ()
  | Waiter from_newest -> (
    match into.oldest with
    | No_waiter ->
      into.newest <- from.newest;
      into.oldest <- from.oldest
    | Waiter into_oldest ->
      into_oldest.older <- from.newest;
      from_newest.newer <- into.oldest;
      into.oldest <- from.oldest));
  from.newest <- No_waiter;
  from.oldest <- No_waiter

let new_pending (sched : sched) : 'a t =
  inj
    { st =
        Pending
          {
            owner = sched;
            newest = No_waiter;
            oldest = No_waiter;
            cancel_waiters = [];
            cancel = Cancel_self ignore;
            link = None;
          };
    }

(* Both lists run most-recently-added first: Lwt runs attached callbacks in
   reverse registration order (its callback trees prepend new nodes and are
   traversed front-first). Observable, e.g., through [Lwt_react.E.limit]'s flush
   racing a user [on_success].

   The waiter list is not mutated while this runs: the promise is no longer
   [Pending], so a waiter added to it from a callback runs at once instead of
   being linked, and a removable waiter firing from here finds the promise
   resolved and leaves its node alone (see [add_removable_waiter_to_each_of]).
   That is also what lets a cursor into it be followed later. *)
(* [with_cascades sched outer f]: run [f ()] with the cascade entries it
   registered on top of [outer], and put [outer] back whether [f] returned or
   raised: a waiter that raises escapes the loop, as in Lwt, and must not
   leave a stale entry that a later suspension would detach, resurrecting
   callbacks the exception had dropped. *)
let with_cascades (sched : sched) (outer : cascade list) (f : unit -> unit) :
    unit =
  match f () with
  | () -> sched.cascades <- outer
  | exception e ->
    let bt = Printexc.get_raw_backtrace () in
    sched.cascades <- outer;
    Printexc.raise_with_backtrace e bt

let rec run_waiters cursor r =
  match !cursor with
  | No_waiter -> ()
  | Waiter w ->
    cursor := w.older;
    w.run r;
    run_waiters cursor r

let rec run_cancels (cursor : (unit -> unit) list ref) : unit =
  match !cursor with
  | [] -> ()
  | f :: rest ->
    cursor := rest;
    f ();
    run_cancels cursor

let run_resolution_callbacks (type a) (sched : sched) (pe : a pending)
    (r : (a, exn) result) : unit =
  match (r, pe.cancel_waiters) with
  | Error Canceled, (_ :: _ as cancels) ->
    (* A cancellation with [on_cancel] callbacks: they run first, then the
       waiters, and both go through cursors the scheduler knows about, the
       waiters' registered first, so that a suspension inside an [on_cancel]
       callback detaches the cancel callbacks still to run and then every
       waiter, in that order ([suspend]). *)
    let outer = sched.cascades in
    let waiters = ref pe.newest in
    let cancels = ref cancels in
    sched.cascades <- Cancels cancels :: Cascade (r, waiters) :: outer;
    with_cascades sched outer (fun () ->
      run_cancels cancels;
      sched.cascades <- Cascade (r, waiters) :: outer;
      run_waiters waiters r)
  | _ -> (
    match pe.newest with
    | No_waiter -> ()
    | Waiter { older = No_waiter; run; _ } -> run r
    | Waiter _ as first ->
      (* Several waiters: go through a cursor the scheduler knows about, so
         that a suspension inside one of them can detach the others
         ([suspend]). The single-waiter case above, which is every pending
         [bind], pays nothing. *)
      let cursor = ref first in
      let outer = sched.cascades in
      sched.cascades <- Cascade (r, cursor) :: outer;
      with_cascades sched outer (fun () -> run_waiters cursor r))

(* Runs, defers or queues the callbacks of a promise whose state has just been
   set to [r]: the second half of a resolution, shared by [fill_general] and by
   [cancel_gen], which sets the states of all it cancels before delivering. *)
let deliver (type a) (sched : sched) ~allow_deferring
    ~maximum_callback_nesting_depth (pe : a pending) (r : (a, exn) result) :
    unit =
  if sched.defer_fills then
    (* Inside the engine iteration: the callbacks become a task, see
       [defer_fills]. *)
    Run_queue.push sched.queue (Fill (pe, r))
  else if allow_deferring && sched.nesting >= maximum_callback_nesting_depth then
    Queue.push (fun () -> run_resolution_callbacks sched pe r) sched.deferred
  else
    run_in_resolution_loop sched (fun () -> run_resolution_callbacks sched pe r)

let fill_general (type a) (sched : sched) ~allow_deferring
    ~maximum_callback_nesting_depth (p : a t) (r : (a, exn) result) : unit =
  let p = prj p in
  match p.st with
  | Pending pe ->
    check_owner sched pe;
    p.st <- (match r with Ok v -> Fulfilled v | Error e -> Rejected e);
    deliver sched ~allow_deferring ~maximum_callback_nesting_depth pe r
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
    let (_ : a waiter) = link_waiter pe w in
    ()
  | Fulfilled v -> w (Ok v)
  | Rejected e -> w (Error e)

let set_on_cancel (type a) (sched : sched) (p : a t) (f : unit -> unit) : unit =
  match (prj p).st with
  | Pending pe ->
    check_owner sched pe;
    pe.cancel <- Cancel_self f
  | Fulfilled _ | Rejected _ -> ()

(* Cancellation runs in two phases, as in Lwt. The first, this function, walks
   back from the promise being cancelled through the sources of derived
   promises, and every directly cancelable promise it reaches is marked
   [Canceled] on the spot, so that one reached twice (a diamond) is cancelled
   once, but nothing runs yet. The second, in [cancel_gen], runs what the first
   found, last found first. So when [cancel (join [a; b])] cancels both [a] and
   [b], a callback of either already sees the other [Canceled], as in Lwt, rather
   than the state of a cancellation still under way.

   The owner is checked at every hop, before any hook runs: the hook of
   [protected] (or of [wrap_in_cancelable]) unlinks a waiter from ANOTHER
   pending promise, with the scheduler that promise captured, so without this
   check it would pass its own and mutate the owner's list from a foreign
   domain, and the mirror would never resolve. *)
let collect_cancelable (type a) (sched : sched) (found : cancel_found list)
    (p : a t) : cancel_found list =
  let c = prj p in
  match c.st with
  | Pending pe -> (
    check_owner sched pe;
    match pe.cancel with
    | Not_cancelable -> found
    | Cancel_self hook ->
      c.st <- Rejected Canceled;
      Found (pe, hook) :: found
    | Cancel_forward collect -> collect sched found)
  | Fulfilled _ | Rejected _ -> found

(* The second phase delivers without deferring, as Lwt's [cancel] does: the
   cancellation of a promise is visible to the caller of [cancel] when it
   returns. *)
let cancel_gen (type a) (sched : sched) (p : a t) : unit =
  List.iter
    (fun (Found (pe, hook)) ->
      hook ();
      deliver sched ~allow_deferring:false
        ~maximum_callback_nesting_depth:default_maximum_callback_nesting_depth
        pe (Error Canceled))
    (collect_cancelable sched [] p)

(* [Lwt.cancel] is public and takes only the promise. *)
let cancel (type a) (p : a t) : unit = cancel_gen (self_sched ()) p

(* Mark [result] as forwarding cancellation to its current source [src] (no-op
   if [result] is no longer pending). Used by the derived combinators. *)
let set_cancel_forward (type a b) (sched : sched) (result : a t) (src : b t) :
    unit =
  match (prj result).st with
  | Pending pe ->
    check_owner sched pe;
    pe.cancel <-
      Cancel_forward (fun sched found -> collect_cancelable sched found src)
  | Fulfilled _ | Rejected _ -> ()

(* Forward cancellation to a whole list of sources (Lwt's
   [propagate_cancel_to_several], used by choose/pick/join/all/both/nchoose),
   searched in list order. *)
let set_cancel_forward_list (type a b) (sched : sched) (result : a t)
    (ps : b t list) : unit =
  match (prj result).st with
  | Pending pe ->
    check_owner sched pe;
    pe.cancel <-
      Cancel_forward
        (fun sched found -> List.fold_left (collect_cancelable sched) found ps)
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
   this is Lwt's [make_into_proxy]: [p'], the promise a bind continuation just
   returned, is absorbed into [result], the older, anchored one. Its waiters are
   spliced behind [result]'s own (Lwt runs the outer promise's callbacks before
   the returned promise's), its cancel mode is taken (cancelling [result] must
   reach the {e current} source), and its cell becomes an alias of [result]. As
   in Lwt, the fresh cell is the one that becomes the proxy, so a tail-recursive
   [p >>= loop] keeps a single live promise: each lap's fresh cell is dropped by
   its creator and collected, and the anchored result never grows a chain. *)
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
        splice_waiters ~into:pe ~from:pe';
        pe.cancel_waiters <- pe.cancel_waiters @ pe'.cancel_waiters;
        pe.cancel <- pe'.cancel;
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
        (* Only [f] inside the handler, not the fill: the fill runs [result]'s
           callbacks, and an exception escaping one of them was taken for an
           exception of [f], then lost on a promise already resolved. *)
        match f v with
        | v' -> fill sched result (Ok v')
        | exception e when Exception_filter.run e -> fill sched result (Error e))
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
let call_guarded (f : unit -> 'a t) : 'a t =
  try f () with e when Exception_filter.run e -> inj { st = Rejected e }

(* [try_bind] once [f ()] has given [p]. *)
let try_bind_promise (p : 'a t) (g : 'a -> 'b t) (h : exn -> 'b t) : 'b t =
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

let try_bind (f : unit -> 'a t) (g : 'a -> 'b t) (h : exn -> 'b t) : 'b t =
  try_bind_promise (call_guarded f) g h

(* A fulfilled [f ()] is returned as it is, as the historical core did, rather
   than as a copy made by [return]: [catch (fun () -> p) h == p], and nothing
   is allocated on that path. *)
let catch (f : unit -> 'a t) (h : exn -> 'a t) : 'a t =
  let p = call_guarded f in
  match (prj p).st with
  | Fulfilled _ -> p
  | Rejected _ | Pending _ -> try_bind_promise p return h

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
        (fun sched found ->
          collect_cancelable sched (collect_cancelable sched found a) b)
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
   holds [f]; a small wrapper is linked into every promise in [ps]. The first
   resolution runs [f] once and UNLINKS the wrapper from the still-pending
   promises, dropping [f] and its captures. Without this, a long-lived promise
   repeatedly passed to [choose]/[pick] (e.g. a server's shutdown promise, one
   pick per request/connection) accumulates dead waiters without bound, the
   leak classic Lwt prevents with [clear_explicitly_removable_callback_cell].
   Each unlink is O(1), where classic Lwt clears a shared cell and rebuilds the
   list every forty-second time, so ten thousand concurrent picks against one
   promise cost the same per pick as one.
   Returns the remover, for mirrors ([protected]) that must detach on
   cancellation; calling it after the waiter fired is a no-op. *)
let add_removable_waiter_to_each_of (sched : sched) (ps : 'a t list)
    (f : ('a, exn) result -> unit) : unit -> unit =
  let cell = ref (Some f) in
  (* Each node with its promise: unlinking needs the record the node lives in
     NOW, which [forward] may have moved it to, and [prj] finds that record. *)
  let nodes = ref [] in
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
        (fun (p, node) ->
          match (prj p).st with
          | Pending pe ->
            check_owner sched pe;
            unlink_waiter pe node
          | Fulfilled _ | Rejected _ -> ())
        !nodes
  in
  List.iter
    (fun p ->
      match (prj p).st with
      | Pending pe ->
        check_owner sched pe;
        nodes := (p, link_waiter pe wrapper) :: !nodes
      | Fulfilled v -> wrapper (Ok v)
      | Rejected e -> wrapper (Error e))
    ps;
  remove

(* Among already-resolved promises, Lwt's [choose]/[pick] prefer a {e rejection};
   with several fulfilled and none rejected, one is chosen at random (fairness,
   as Lwt). The [Invalid_argument] messages use Lwt's wording: this core is meant
   to BE the Lwt core (B2b), where these are the right names. *)
(* A rejected promise wins over a fulfilled one; among several of the winning
   kind, one at random, from the scheduler's generator, as in Lwt. *)
let select_resolved (sched : sched) (ps : 'a t list) : 'a t option =
  let pick_one = function
    | [] -> None
    | [ p ] -> Some p
    | l ->
      Some (List.nth l (Random.State.int (Lazy.force sched.prng) (List.length l)))
  in
  match
    List.filter (fun p -> match (prj p).st with Rejected _ -> true | _ -> false) ps
  with
  | _ :: _ as rejected -> pick_one rejected
  | [] ->
    pick_one
      (List.filter
         (fun p -> match (prj p).st with Fulfilled _ -> true | _ -> false)
         ps)

let choose (ps : 'a t list) : 'a t =
  let sched = self_sched () in
  if ps = [] then
    invalid_arg "Lwt.choose [] would return a promise that is pending forever";
  match select_resolved sched ps with
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
  match select_resolved sched ps with
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

(* Serve the paused batch as one task per promise, and the yielded tasks after
   them, instead of resolving the whole batch on the idle hook's stack. Each pause's callbacks then run from
   the loop as their own task, so an [await] in one of them (see [runner])
   suspends that task alone; resolving them in one [List.iter] would freeze the
   rest of the batch in the continuation. Same order as [wakeup_paused_sched]
   (FIFO), and the batch still runs before the next idle lap. [Lwt.wakeup_paused],
   the public entry point, keeps its synchronous semantics. *)
let serve_paused_sched (sched : sched) =
  let ps = List.rev sched.paused in
  sched.paused <- [];
  sched.paused_n <- 0;
  List.iter (fun p -> Run_queue.push sched.queue (Paused p)) ps;
  let ys = List.rev sched.yielded in
  sched.yielded <- [];
  List.iter (Run_queue.push sched.queue) ys

(* Run [f] with every resolution it triggers turned into a task of the run
   queue (see [fill_general]) rather than run on the spot. [Lwt_main] wraps the
   engine iteration in it: an engine callback may run on a C frame (libev
   invokes its watchers through [caml_callback], and an effect cannot be
   performed across a C call), and the callbacks of one event must not freeze
   the dispatch of the others when one of them awaits. The state of a promise is
   still set at once; only its callbacks move to the queue, in FIFO order, which
   is within what [wakeup_later] promises. For [wakeup] it is the one documented
   exception to its immediacy: inside the engine iteration its callbacks are
   queued as well, since they must not run on the engine's stack. *)
let defer_fills (sched : sched) (f : 'a -> unit) (x : 'a) : unit =
  let saved = sched.defer_fills in
  sched.defer_fills <- true;
  match f x with
  | () -> sched.defer_fills <- saved
  | exception e ->
    sched.defer_fills <- saved;
    raise e

(* The resolution loop keeps stack-shaped state in the scheduler: [nesting]
   and [storage] are restored by the frames that set them, and [cascades] by
   [run_resolution_callbacks]. A direct-style layer suspends a fiber in the
   middle of such frames, so that state must travel with the fiber, or the
   pass that goes on without it runs at the wrong depth: a [wakeup_later]
   there is deferred, and never drained until the fiber resumes. [suspend]
   captures it for the fiber about to be parked and leaves the scheduler as
   at the start of a pass; [resume] installs it around the resumption and
   puts the resumer's own back afterwards.

   What the parked frames were still to run must not wait for them either:
   the remaining waiters of each cascade in progress, innermost first, and the
   callbacks deferred to the end of the outermost one. They become tasks, in
   their order, each run at depth 1 as it would have been, so [wakeup_later]
   keeps deferring inside them. *)
type resolution_state = {
  r_nesting : int;
  r_storage : storage;
  r_cascades : cascade list;
  r_defer_fills : bool;
    (* A fiber suspended inside the engine iteration (which [Lwt_main] runs
       under [defer_fills]) must not leave [defer_fills] set for the pass that
       goes on without it: [Lwt.wakeup] would stop running its callbacks for
       everyone. Kept here so that it is restored with the fiber. *)
}

let capture (sched : sched) : resolution_state =
  {
    r_nesting = sched.nesting;
    r_storage = sched.storage;
    r_cascades = sched.cascades;
    r_defer_fills = sched.defer_fills;
  }

let install (sched : sched) (st : resolution_state) : unit =
  sched.nesting <- st.r_nesting;
  sched.storage <- st.r_storage;
  sched.cascades <- st.r_cascades;
  sched.defer_fills <- st.r_defer_fills

(* The state at the start of a pass; one value, not one allocation per
   suspension. *)
let pass_start : resolution_state =
  { r_nesting = 0; r_storage = empty_storage; r_cascades = []; r_defer_fills = false }

let suspend (sched : sched) : resolution_state =
  let st = capture sched in
  List.iter
    (function
      | Cascade (r, cursor) ->
        let rec detach = function
          | No_waiter -> ()
          | Waiter w ->
            Run_queue.push sched.queue (Detached (r, w.run));
            detach w.older
        in
        detach !cursor;
        cursor := No_waiter
      | Cancels cursor ->
        List.iter
          (fun f ->
            Run_queue.push sched.queue
              (Thunk (st.r_storage, fun () -> run_in_resolution_loop sched f)))
          !cursor;
        cursor := [])
    st.r_cascades;
  while not (Queue.is_empty sched.deferred) do
    let f = Queue.pop sched.deferred in
    Run_queue.push sched.queue
      (Thunk (st.r_storage, fun () -> run_in_resolution_loop sched f))
  done;
  install sched pass_start;
  st

let resume (sched : sched) (st : resolution_state) (f : unit -> unit) : unit =
  (* The resumer's state in locals rather than a record: nothing allocated
     per resumption. *)
  let nesting = sched.nesting
  and storage = sched.storage
  and cascades = sched.cascades
  and defer_fills = sched.defer_fills in
  let restore () =
    sched.nesting <- nesting;
    sched.storage <- storage;
    sched.cascades <- cascades;
    sched.defer_fills <- defer_fills
  in
  install sched st;
  match f () with
  | () -> restore ()
  | exception e ->
    let bt = Printexc.get_raw_backtrace () in
    restore ();
    Printexc.raise_with_backtrace e bt

(* A region in which suspending the current task is an error. The core only
   keeps the count: a direct-style layer consults it before suspending and
   raises ([Lwt_direct.no_await], [Lwt_direct.await]), and [Lwt_react] opens
   one around every propagation its setters start, without depending on that
   layer or on effects. A counter so that regions nest; stack-shaped, which
   holds because nothing can suspend while it is set. *)
let no_suspend (sched : sched) (f : unit -> 'a) : 'a =
  sched.suspension_forbidden <- sched.suspension_forbidden + 1;
  match f () with
  | v ->
    sched.suspension_forbidden <- sched.suspension_forbidden - 1;
    v
  | exception e ->
    let bt = Printexc.get_raw_backtrace () in
    sched.suspension_forbidden <- sched.suspension_forbidden - 1;
    Printexc.raise_with_backtrace e bt

let register_pause_notifier f = (self_sched ()).pause_notifier <- Some f

let abandon_paused () =
  let sched = self_sched () in
  sched.paused <- [];
  sched.paused_n <- 0;
  sched.yielded <- [];
  (* The pauses the idle lap already turned into tasks are abandoned too (the
     child of a fork must not run the parent's); one pass over the queue,
     which happens once per fork. *)
  let n = Run_queue.length sched.queue in
  for _ = 1 to n do
    match Run_queue.pop sched.queue with
    | Paused _ -> ()
    | task -> Run_queue.push sched.queue task
  done

(* How [run] executes the scheduler loop. The default runs it plainly. A
   direct-style layer ([Lwt_direct]) installs a runner that runs the loop under
   its effect handler and starts a new pass whenever the fiber draining the
   queue is suspended by an [await]: that is what lets [await] work in any
   callback, not only inside [Lwt_direct.spawn]. Process-wide and set at module
   initialisation, like [async_exception_hook]: it is a property of the program,
   not of a scheduler. The core itself performs no effect, so it still builds
   and runs on OCaml 4.14, where no layer ever installs a runner. *)
let runner : ((unit -> unit) -> unit) ref = ref (fun loop -> loop ())

(* Tell the pass currently draining the run queue to stop after its task. The
   direct-style layer calls this when it suspends the drainer's fiber, right
   before starting a new pass through [runner]; see [run_scheduler]. *)
let retire_drainer (sched : sched) : unit =
  sched.drainer_gen <- sched.drainer_gen + 1

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
  if sched.paused_n > 0 || sched.yielded <> [] then begin
    serve_paused_sched sched;
    true
  end
  else false

(* Close the forward reference opened next to [new_sched]: from here on a fresh
   scheduler serves its own pauses. *)
let () = default_idle := core_idle

(* [Lwt.Private.scheduler_set_idle] keeps its historical [unit -> bool] shape,
   so back ends are unaffected; the record is threaded on our side only. *)
let set_idle (f : unit -> bool) : unit = (self_sched ()).idle <- fun _ -> f ()

let run_scheduler (sched : sched) : unit =
  (* One pass of the loop is bound to the fiber that runs it: [gen] identifies
     that fiber as the current drainer. When a direct-style layer suspends the
     drainer (an [await] performed by a callback the loop was running), it
     retires it ([retire_drainer]) and starts a new pass through [runner]; the
     suspended pass must then end as soon as its fiber is resumed and its task
     has finished, otherwise two passes would drain the same queue, the resumed
     one sitting on top of the resumer's stack. The check is one load and one
     compare per task; without a direct-style layer [gen] never changes. *)
  let gen = sched.drainer_gen in
  (* The idle lap (iteration hooks, engine iteration, pause service) runs on
     this pass's stack but belongs to no task: a suspension there would park
     the engine's own state and the hooks' in the continuation, with the next
     pass restarting them over it (a select engine re-arming the same timers
     for ever). It is a region where suspension is an error, on every engine,
     which a direct-style layer turns into its exception at the call site. The
     callbacks of the promises the lap resolves are not concerned: they run
     later, as tasks. *)
  let idle_lap () =
    sched.suspension_forbidden <- sched.suspension_forbidden + 1;
    match sched.idle sched with
    | more ->
      sched.suspension_forbidden <- sched.suspension_forbidden - 1;
      more
    | exception e ->
      let bt = Printexc.get_raw_backtrace () in
      sched.suspension_forbidden <- sched.suspension_forbidden - 1;
      Printexc.raise_with_backtrace e bt
  in
  let rec loop () =
    if Run_queue.is_empty sched.queue then begin
      (* Run queue drained: advance the world by one lap through the idle hook.
         The hook serves at most one paused batch (and, for a backend, one
         engine iteration) before returning here, so under a sustained stream
         of pauses the engine still runs once per batch instead of after the
         whole pause cascade settles. Returns [false] only when there is
         nothing left to do. *)
      if idle_lap () && sched.drainer_gen = gen then loop ()
    end
    else begin
      (match Run_queue.pop sched.queue with
      | Thunk (s, f) ->
        sched.storage <- s;
        f ()
      | Paused p -> fill sched (inj p) ok_unit
      | Fill (pe, r) ->
        run_in_resolution_loop sched (fun () ->
          run_resolution_callbacks sched pe r)
      | Detached (r, w) -> run_in_resolution_loop sched (fun () -> w r));
      if sched.drainer_gen = gen then loop ()
    end
  in
  loop ()

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
  !runner (fun () -> run_scheduler sched);
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

(* The handler of [async], [dont_wait] and [ignore_result] is called DIRECTLY,
   as Lwt always has: no registration storage, and no [hooked] around it, so an
   exception it raises propagates, to the caller when the promise is already
   rejected, to the resolver otherwise. Through [on_failure], the hook that
   [async] calls was itself wrapped in a call to the hook, and a hook that
   raised was called a second time with its own exception; and the handler of
   [dont_wait], which is the caller's, sent its exception to the hook. *)
let on_failure_direct (type a) (p : a t) (handler : exn -> unit) : unit =
  add_waiter (self_sched ()) p (function Ok _ -> () | Error e -> handler e)

(* Lwt's [async] is fire-and-forget and runs [f ()] {e immediately} on the
   caller's stack (its callbacks register before the caller's next action —
   tests rely on this); a synchronous raise or a rejection goes to
   [async_exception_hook]. *)
let async (f : unit -> unit t) : unit =
  let p = try f () with e when Exception_filter.run e -> fail e in
  on_failure_direct p (fun e -> !async_exception_hook e)

(* Same immediate-run semantics for [dont_wait], with a user handler. *)
let dont_wait (f : unit -> unit t) (handler : exn -> unit) : unit =
  let p = try f () with e when Exception_filter.run e -> fail e in
  on_failure_direct p handler

(* Lwt's [ignore_result]: an already-rejected promise raises synchronously; a
   later rejection goes to [async_exception_hook]. *)
let ignore_result p =
  match (prj p).st with
  | Fulfilled _ -> ()
  | Rejected e -> raise e
  | Pending _ -> on_failure_direct p (fun e -> !async_exception_hook e)

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

(* Through the exception filter, like [wrap]: a runtime exception (a stack
   overflow, out of memory) is not turned into a rejection unless the program
   asked for it. They caught everything. *)
let wrap1 f = fun x ->
  (try return (f x) with e when Exception_filter.run e -> fail e)
let wrap2 f = fun x y ->
  (try return (f x y) with e when Exception_filter.run e -> fail e)
let wrap3 f = fun x y z ->
  (try return (f x y z) with e when Exception_filter.run e -> fail e)
let wrap4 f = fun a b c d ->
  (try return (f a b c d) with e when Exception_filter.run e -> fail e)
let wrap5 f = fun a b c d e ->
  (try return (f a b c d e) with ex when Exception_filter.run ex -> fail ex)
let wrap6 f = fun a b c d e g ->
  (try return (f a b c d e g) with ex when Exception_filter.run ex -> fail ex)

let wrap7 f = fun a b c d e g h ->
  (try return (f a b c d e g h) with ex when Exception_filter.run ex -> fail ex)

external reraise : exn -> 'a = "%reraise"

(* The tracing/backtrace variants, called only by the ppx.

   [add_loc] is the ppx's [fun exn -> try Lwt.reraise exn with exn -> exn], built
   at the source location of the [let%lwt], so applying it appends a "Re-raised at"
   frame naming that line. Applied at every rejection that crosses a ppx bind, it
   RECONSTRUCTS the chain of source locations an exception travelled through, which
   is the whole reason these four functions exist. Lwt has done this since the
   backtrace workaround was introduced, and a ppx user who loses it drops to the
   backtrace of a bare [>>=], which shows nothing of his own code.

   Where [add_loc] goes is Lwt's choice, mirrored here rather than tidied: on a
   rejection produced or propagated by these combinators, and on a synchronous
   exception raised by a continuation. NOT on the fulfilled paths, which stay the
   plain fast path of [bind], so the cost of this on a program that is not raising
   is nil.

   [name] and [line] locate the [let%lwt] for runtime events, as in Lwt: a ppx
   bind that has to wait, because its promise is pending, emits a [Begin] span
   when it is set up and the matching [End] when its callback starts, both
   carrying the tracing context of [with_tracing_context] in effect at the
   bind. A bind on a resolved promise emits nothing. Without the
   [lwt_runtime_events] library, [Lwt_rte] makes both no-ops. *)

(* The context the spans carry. ONE key, also exported as
   [Private.tracing_context]: the ppx reads it for the spans of the loops it
   expands, so a second key would leave those spans without their context. *)
let tracing_context : string key = new_key ()

let with_tracing_context name f = with_value tracing_context (Some name) f

(* [apply], with the location function applied to a synchronous exception. *)
let apply_loc add_loc (f : 'a -> 'b t) (v : 'a) : 'b t =
  try f v with e when Exception_filter.run e -> inj { st = Rejected (add_loc e) }

let backtrace_bind (type a b) name line add_loc (p : a t) (f : a -> b t) : b t =
  match (prj p).st with
  | Fulfilled v -> f v
  | Rejected e -> inj { st = Rejected (add_loc e) }
  | Pending pe ->
    let sched = owner_sched pe in
    let result = new_pending sched in
    set_cancel_forward sched result p;
    let saved = sched.storage in
    let context = get_from_storage tracing_context saved in
    Lwt_rte.emit_trace Begin context name line;
    add_waiter sched p (fun r ->
      Lwt_rte.emit_trace End context name line;
      let outer = sched.storage in
      sched.storage <- saved;
      (match r with
      | Ok v -> forward sched result (apply_loc add_loc f v)
      | Error e -> fill sched result (Error (add_loc e)));
      sched.storage <- outer);
    result

let backtrace_try_bind name line add_loc (f : unit -> 'a t) (g : 'a -> 'b t)
    (h : exn -> 'b t) : 'b t =
  let p = try f () with e when Exception_filter.run e -> inj { st = Rejected e } in
  match (prj p).st with
  | Fulfilled v -> g v
  | Rejected e -> h (add_loc e)
  | Pending pe ->
    let sched = owner_sched pe in
    let result = new_pending sched in
    set_cancel_forward sched result p;
    let saved = sched.storage in
    let context = get_from_storage tracing_context saved in
    Lwt_rte.emit_trace Begin context name line;
    add_waiter sched p (fun r ->
      Lwt_rte.emit_trace End context name line;
      let outer = sched.storage in
      sched.storage <- saved;
      forward sched result
        (match r with
        | Ok v -> apply_loc add_loc g v
        | Error e -> apply_loc add_loc h e);
      sched.storage <- outer);
    result

let backtrace_catch name line add_loc f h =
  backtrace_try_bind name line add_loc f return h

let backtrace_finalize name line add_loc f g =
  backtrace_try_bind name line add_loc f
    (fun x -> bind (g ()) (fun () -> return x))
    (fun e -> bind (g ()) (fun () -> fail (add_loc e)))

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

  let tracing_context = tracing_context

  (* Effect-scheduler hooks (this core is engine-free): [Lwt_main.run] drives
     the run queue and installs the engine-blocking idle hook through these. *)
  let scheduler_run = run
  let scheduler_set_idle = set_idle
  let scheduler_queue_is_empty () = Run_queue.is_empty (self_sched ()).queue
  let scheduler_enqueue = enqueue

  (* The loop runner and the drainer generation, for a direct-style layer that
     wants [await] to work in any callback: see [run_scheduler] and [runner]. *)
  let scheduler_set_runner f = runner := f
  let scheduler_drainer_gen () = (self_sched ()).drainer_gen
  let scheduler_retire_drainer () = retire_drainer (self_sched ())

  (* Serving the loop's own events as tasks, so that a callback they run may
     await without freezing its neighbours or sitting on a C frame; see
     [serve_paused_sched] and [defer_fills]. *)
  let scheduler_serve_paused () = serve_paused_sched (self_sched ())

  (* The next-lap list, for [Lwt_direct.yield]: a resumption parked here runs
     after the next idle lap, with the paused batch. *)
  let scheduler_enqueue_next_lap (f : unit -> unit) : unit =
    let sched = self_sched () in
    sched.yielded <- Thunk (sched.storage, f) :: sched.yielded

  let scheduler_next_lap_pending () =
    let sched = self_sched () in
    sched.paused_n > 0 || sched.yielded <> []

  (* Run [f] as a callback runs, at depth 1 of the resolution loop: a
     [wakeup_later] inside defers its callbacks to the end of [f], or to its
     next suspension, which hands them to the run queue; they never run on
     [f]'s stack. For the bodies of direct-style tasks. *)
  let in_resolution_loop f = run_in_resolution_loop (self_sched ()) f
  let scheduler_defer_fills f x = defer_fills (self_sched ()) f x

  (* The resolution state a suspended fiber takes with it; see [suspend]. *)
  type nonrec resolution_state = resolution_state

  let scheduler_suspend () = suspend (self_sched ())
  let scheduler_resume st f = resume (self_sched ()) st f

  (* For a layer about to suspend on [p]: the check [add_waiter] would make,
     made before the continuation is captured, so that [Foreign_promise] is
     raised at the call site and not out of the handler. *)
  let check_owner (type a) (p : a t) : unit =
    match (prj p).st with
    | Pending pe -> check_owner (self_sched ()) pe
    | Fulfilled _ | Rejected _ -> ()

  (* Regions where suspension is an error; see [no_suspend]. *)
  let no_suspend f = no_suspend (self_sched ()) f
  let suspension_forbidden () = (self_sched ()).suspension_forbidden > 0

  (* Which domain owns a PENDING promise, for the one layer that needs to ask:
     adopting a foreign promise means getting its owner to attach the callback,
     since attaching it ourselves is exactly what the ownership check forbids. A
     READ, so unchecked, like [state]; [dom] is immutable and the promise reached
     us by being passed, which is what publishes it. *)
  let promise_owner_domain (type a) (p : a t) : int option =
    match (prj p).st with
    | Pending pe -> Some (pe.owner.dom :> int)
    | Fulfilled _ | Rejected _ -> None
end

