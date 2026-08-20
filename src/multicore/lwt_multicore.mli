(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(** Communication between Lwt loops running on different domains.

    Lwt promises are domain-local: a promise belongs to the loop that created it,
    and touching one from another domain raises {!Lwt.Foreign_promise} rather than
    corrupting it quietly. That is what makes N independent loops safe, and it is
    also why they need something to talk through. This module is that something.

    {2 The shape of this API}

    Every operation here returns an ORDINARY LOCAL PROMISE, resolved by the
    calling loop. Nothing crosses a domain except plain data and the wake-up that
    carries it. What tells you a value is shared is its TYPE, not the operations
    on it: a {!Mutex.t} at module level is visible on re-reading, while the code
    that uses it looks like ordinary Lwt.

    {2 What this module is not}

    It is not a domain-safe mirror of Lwt's containers. {!Lwt_stream},
    {!Lwt_pool}, {!Lwt_mvar} and the rest stay domain-local, deliberately: a
    lazy clonable stream is not what one wants across a domain boundary, and a
    shared pool of Lwt resources is not merely hard but wrong, since a connection
    belongs to the loop that opened it. Give each loop its own pool and bound the
    total with a {!Semaphore}. *)

(** {2 Loops} *)

type loop
(** A handle on a running Lwt loop, which other domains can post work to.

    Obtained with {!self} on the loop itself and then shared: it is plain data,
    safe to pass to another domain. *)

val self : unit -> loop
(** [self ()] is a handle on the calling domain's loop. Creating it registers the
    inbox and the notification that drains it, so a domain that never calls this
    pays nothing. *)

exception Loop_terminated
(** Raised by {!run_on} when the target loop's domain has terminated. *)

val run_on : loop -> (unit -> unit) -> unit
(** [run_on loop f] posts [f] to [loop] and wakes it. [f] runs ON THAT LOOP's
    domain, on its next lap, with nothing held: it may do anything an ordinary
    Lwt callback may do, including resolving that loop's promises.

    Callable from any domain and from any thread, including a thread that is not
    running Lwt at all.

    This is the one unsafe primitive of this module, in the sense that it is the
    only one that hands code to another domain; everything else here is built on
    it. What it does not do is carry a result back, which is what
    {!Lwt_multicore.t} is for.

    @raise Loop_terminated if [loop]'s domain has already terminated. A domain
    that terminates BETWEEN the post and the execution is a race nothing can
    close from here: the work is then simply not done, and a caller who needs to
    know that should use the higher-level primitives, which reject their promise
    in that case. *)

(** {2 A value several loops can wait for} *)

type 'a t
(** A one-shot value that any domain may resolve and any number of loops may wait
    for. This is the answer to "get a result from another domain".

    It is shared data, so what travels through it must be safe to share: plain
    values, immutable structures, or ownership handed over for good. Do not send a
    mutable structure you keep using, and do not send an Lwt promise or any of the
    domain-affine containers ({!Lwt_io.channel}, {!Lwt_unix.file_descr},
    {!Lwt_mutex.t} and the rest); those belong to the loop that made them, and the
    ownership check will say so. Nothing in the type prevents it; this is the one
    place where a reader has to be told rather than checked. *)

val create : unit -> 'a t
(** A value nobody has resolved yet. *)

val await : 'a t -> 'a Lwt.t
(** [await t] is an ORDINARY LOCAL PROMISE of the calling loop, fulfilled when
    [t] is resolved, rejected when it is rejected. Several loops may await the
    same [t], and each gets its own promise, resolved on its own domain.

    The promise is cancellable: cancelling it withdraws this loop's interest and
    rejects the promise with {!Lwt.Canceled}, leaving [t] and the other waiters
    alone. *)

val resolve : 'a t -> 'a -> unit
(** [resolve t v] fulfils [t] and wakes every loop waiting for it, each on its own
    domain. Callable from any domain and any thread.

    If the calling loop is itself waiting, its own promise is resolved
    SYNCHRONOUSLY, as {!Lwt.wakeup} does, rather than being posted back to
    itself. Other loops are woken through their inbox and see it on their next
    lap.

    A waiter whose domain has terminated is skipped, rather than being an error
    for the resolver: it is not the resolver's business that someone has gone.

    @raise Invalid_argument if [t] is already resolved, as {!Lwt.wakeup} does. *)

val reject : 'a t -> exn -> unit
(** Like {!resolve}, with a rejection.

    @raise Invalid_argument if [t] is already resolved. *)

val cancel : 'a t -> unit
(** [cancel t] rejects [t] with {!Lwt.Canceled} if it is still pending, and does
    nothing otherwise. Unlike {!reject} it never raises, which is what makes it
    usable to broadcast a shutdown from anywhere. *)

val is_pending : 'a t -> bool
(** Whether [t] has yet to be resolved. A snapshot, and the answer may already be
    stale when it reaches you: useful for reporting, never for deciding. *)

(** {2 Adopting a foreign promise} *)

exception Cannot_adopt
(** Raised by {!adopt} when the promise's owning loop cannot be reached: either
    its domain has terminated, or it never took a handle with {!self}. A loop that
    may be adopted from must have called {!self} once; any loop that takes part in
    this module's traffic has. *)

val adopt : 'a Lwt.t -> 'a Lwt.t
(** [adopt p] is a LOCAL promise that follows [p], which may belong to another
    domain. After it, plain {!Lwt.bind} works as usual.

    This is the explicit escape hatch for a promise you cannot change the making
    of: one created by a library on the domain that initialised it, typically.
    Waiting on it directly would raise {!Lwt.Foreign_promise}, and rightly so,
    since attaching a callback to a foreign pending promise is what corrupts its
    owner's waiter list.

    What it does instead is ask the OWNER to attach the callback, on its own
    domain, and hand the outcome over as data. So it costs one round trip, and it
    requires the owner's loop to be running: a promise whose owner has stopped
    running a loop will never resolve here either.

    Returns [p] itself when it is already resolved, or already ours: both are
    correct and free.

    @raise Cannot_adopt if the owning loop cannot be reached. *)

(** {2 Mutual exclusion, conditions, counting}

    These are the shared counterparts of {!Lwt_mutex}, {!Lwt_condition} and a
    counting semaphore. Their operations return ordinary local promises, so code
    using them reads like ordinary Lwt; what tells you the value is shared is its
    type.

    Use them for what crosses a domain boundary, and keep {!Lwt_mutex} and
    friends for what does not: they are cheaper, and being domain-local is checked
    for you. *)

module Mutex : sig
  type t
  (** A mutex several loops can contend for. Unlike {!Lwt_mutex.t}, which belongs
      to one loop and refuses any other, this one is meant to be shared. *)

  val create : unit -> t

  val lock : t -> unit Lwt.t
  (** Waits until the mutex is free and takes it. The promise is local and
      cancellable: cancelling it withdraws the request, and if the mutex had
      already been handed over in the meantime, it is passed on to the next
      waiter rather than lost. *)

  val unlock : t -> unit
  (** Releases the mutex, handing it directly to the first waiting loop if there
      is one, so that no third party can jump the queue. Does nothing if the mutex
      is not held. Callable from any domain, and not only from the one that
      locked: that is a discipline for the caller, as in {!Lwt_mutex}. *)

  val with_lock : t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
  (** [with_lock t f] locks, runs [f], and unlocks whatever [f] does, including
      raising or being cancelled. *)

  val is_locked : t -> bool
  (** A snapshot, useful for reporting and never for deciding. *)
end

module Semaphore : sig
  type t
  (** A counting semaphore, which is the right way to BOUND A GLOBAL RESOURCE
      across loops: give each loop its own pool of connections and let a shared
      semaphore cap the total. A shared pool of Lwt resources would not be merely
      hard, it would be wrong, a connection belonging to the loop that opened
      it. *)

  val create : int -> t
  (** [create n] starts with [n] units available. *)

  val available : t -> int
  (** A snapshot. *)

  val acquire : t -> unit Lwt.t
  (** Takes one unit, waiting if none is free. Cancellable, with the same
      hand-on-if-already-served guarantee as {!Mutex.lock}. *)

  val release : t -> unit
  (** Returns one unit, handing it to the first waiting loop if there is one. *)

  val with_resource : t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
  (** Acquires, runs, releases whatever happens. *)
end

module Condition : sig
  type 'a t
  (** A condition variable carrying a value, as {!Lwt_condition} does. *)

  val create : unit -> 'a t

  val wait : ?mutex:Mutex.t -> 'a t -> 'a Lwt.t
  (** Waits for a signal. If [mutex] is given it is unlocked while waiting and
      locked again afterwards, the discipline {!Lwt_condition.wait} follows. *)

  val signal : 'a t -> 'a -> unit
  (** Wakes ONE waiting loop, if any. A value nobody is waiting for is dropped,
      as in {!Lwt_condition}. *)

  val broadcast : 'a t -> 'a -> unit
  (** Wakes every waiting loop, each on its own domain. *)
end

module Stream : sig
  (** A bounded channel between loops: many producers, many consumers, with
      back-pressure. This is what to reach for to send WORK and DATA to another
      domain, and it is deliberately not a domain-safe {!Lwt_stream}: a lazy,
      clonable stream is not the right thing across a boundary, a channel is. *)

  exception Closed
  (** Raised by {!push} on a closed stream, and used to reject producers that
      were waiting for room when the stream was closed. *)

  type 'a t

  val create : capacity:int -> 'a t
  (** [create ~capacity] is an empty stream holding at most [capacity] items
      before producers have to wait. [capacity] must be at least 1: an unbounded
      channel is a memory leak waiting to happen, so this module does not offer
      one. *)

  val push : 'a t -> 'a -> unit Lwt.t
  (** Adds an item, waiting while the stream is full. That wait is the
      back-pressure, and it is the point of the bound.

      Cancelling the wait is harmless: a producer that is waiting has handed over
      nothing, so nothing can be lost by giving up.

      @raise Closed if the stream is closed, as a rejected promise. *)

  val take : 'a t -> 'a option Lwt.t
  (** Takes the next item, waiting if there is none. [None] means the stream is
      closed AND drained, so a consumer learns the end rather than waiting for
      it.

      Cancelling is safe: if an item had already been handed to this consumer, it
      goes back to the front of the stream rather than being lost. *)

  val close : 'a t -> unit
  (** Closes the stream. Waiting consumers get [None], waiting producers are
      rejected with {!Closed}, and items already in the stream are still there to
      be taken. Idempotent. *)

  val length : 'a t -> int
  (** How many items are waiting. A snapshot. *)

  val is_closed : 'a t -> bool
  (** A snapshot. *)
end
