(** An effect-based scheduler for Lwt-style promises (POC).

    This is a proof-of-concept reimplementation of Lwt's monadic core on top of
    OCaml 5 {{:https://ocaml.org/manual/5.3/effects.html} effects}. It keeps the
    familiar monadic interface ([return], [bind], [>>=], ...) so that the
    asynchronicity of a function stays visible in its type. On an already
    resolved promise {!bind} is a plain application ([bind] itself allocates
    nothing — no callback, no proxy), and a lean array-based scheduler replaces
    Lwt's machinery, so scheduling- and bind-heavy code runs markedly faster than
    Lwt. (It is not allocation-free overall: [return] and the user's continuation
    closure still allocate, and flambda does not remove them — see the
    benchmarks.)

    {b Benchmarks & write-up.} A comparative study (this scheduler vs classic
    Lwt, Eio and Miou — scheduling, bind, ping-pong, echo TCP, and cohttp, with
    charts and how to interpret them) lives in the companion benchmarks
    repository: {{:https://github.com/ocsigen/lwt-effects-bench}
    ocsigen/lwt-effects-bench}.

    {1 Representation}

    A value of type ['a t] is a {e promise cell}: it is either already resolved
    (with a value or an exception) or pending (with a list of waiters). This is
    the "design A" of the POC: promises are first-class, shareable and can be
    awaited several times — unlike a suspended-computation encoding.

    {1 The performance idea}

    With this representation:
    - [bind p f] when [p] is already resolved is just [f v]: no promise, no
      callback, no proxy is allocated. The compiler can inline it.
    - [bind p f] when [p] is pending allocates one result promise and registers
      one callback (running [f] when [p] resolves and forwarding [f]'s promise to
      the result) — exactly Lwt's trade-off, but {e without} Lwt's proxy
      machinery and on a leaner, array-based scheduler.

    {1 Semantics (Lwt-faithful)}

    {!bind} is {b non-blocking}, like Lwt's: it does not suspend the caller, so
    Lwt's {e implicit concurrency} is preserved — [both (a >>= f) (b >>= g)] runs
    both branches. Code written against [Lwt] keeps its meaning; this is the
    semantics-preserving, mergeable form of the scheduler.

    Direct-style escape hatches (suspend the current fiber, give up the [_ t]
    typing) live under {!Direct} — use them as you would {!Lwt_direct}, inside
    {!run}/{!async} and never in a {!bind} continuation. *)

type +'a t
(** A promise for a value of type ['a]. Covariant, like {!Lwt.t} — so e.g.
    [[ `A ] t] can be used where [[> `A ] t] is expected, and cohttp's
    [Cohttp.S.IO] functor (which requires [type +'a t]) can be instantiated. *)

exception Canceled
(** Raised in a fiber whose awaited promise is {!cancel}led, and used to reject
    a cancelled promise. *)

(** {1 Constructors} *)

val return : 'a -> 'a t
(** [return v] is an already-fulfilled promise. *)

val fail : exn -> 'a t
(** [fail e] is an already-rejected promise. *)

val return_unit : unit t

(** {1 Monadic combinators} *)

val bind : 'a t -> ('a -> 'b t) -> 'b t
(** [bind p f] is [f v] once [p] is fulfilled with [v]. Non-blocking, like Lwt's:
    it does not suspend the caller, so implicit concurrency is preserved. On an
    already-fulfilled [p] it is a direct application ([bind] itself allocates
    nothing on this path; [f] may). If [f] raises or [p] is rejected, the result
    is a rejected promise. *)

val map : ('a -> 'b) -> 'a t -> 'b t
(** [map f p] applies [f] to the value of [p]. *)

val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t
(** Infix {!bind}. *)

val ( >|= ) : 'a t -> ('a -> 'b) -> 'b t
(** Infix {!map} (arguments flipped to match {!Lwt}). *)

(** Binding operators, as in Lwt and OCaml's [let*] syntax. *)
module Syntax : sig
  val ( let* ) : 'a t -> ('a -> 'b t) -> 'b t
  val ( let+ ) : 'a t -> ('a -> 'b) -> 'b t
  val ( and* ) : 'a t -> 'b t -> ('a * 'b) t
  val ( and+ ) : 'a t -> 'b t -> ('a * 'b) t
end

(** {1 Error handling} *)

val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t
(** [catch f h] runs [f ()] and, if its promise is rejected with [e], runs
    [h e]. Non-blocking (composes inside a {!bind} chain). *)

val try_bind : (unit -> 'a t) -> ('a -> 'b t) -> (exn -> 'b t) -> 'b t
(** [try_bind f g h] runs [f ()]; on success its value is passed to [g], on
    rejection the exception is passed to [h]. Non-blocking. *)

(** {1 Concurrency} *)

val async : (unit -> 'a t) -> 'a t
(** [async f] starts [f ()] in a new fiber and returns a promise for its result.
    The caller continues immediately; the two run concurrently. *)

val both : 'a t -> 'b t -> ('a * 'b) t
(** [both a b] waits for both [a] and [b] (which already run concurrently). *)

val choose : 'a t list -> 'a t
(** [choose ps] resolves as soon as one of [ps] resolves (with its value or
    exception). The other promises are left running. *)

val pick : 'a t list -> 'a t
(** [pick ps] is like {!choose}, but {!cancel}s the other promises once one of
    them resolves. *)

(** {1 Cancellation} *)

val cancel : 'a t -> unit
(** [cancel p] rejects the pending promise [p] with {!Canceled} and runs its
    cancel action (e.g. stopping a pending I/O or timer event). Resolved
    promises are unaffected.

    Note: cancelling the promise returned by {!async} marks that promise as
    cancelled but does not stop the already-running fiber (as in
    {!Lwt_direct.spawn}). *)

(** {1 Yielding and timers} *)

val pause : unit -> unit t
(** [pause ()] resolves on the next scheduler tick, giving other fibers a chance
    to run. *)

val yield : unit -> unit
(** [yield ()] reschedules the current fiber behind the others, like
    [await (pause ())] but without allocating a promise. Must be called inside a
    fiber ({!run}/{!async}). *)

val sleep : float -> unit t
(** [sleep d] resolves after [d] seconds. *)

(** {1 Direct style (escape hatch)}

    Like {!Lwt_direct}: turn a promise into a plain value, yield, and do
    plain-value I/O. These suspend the current fiber and give up the [_ t] async
    typing, so they must run {e inside} a fiber ({!run}/{!async}) and {e not} in a
    {!bind} continuation. The default monadic API above is the recommended one.

    Possible improvement: these primitives do not break implicit concurrency (they
    are explicit, like {!Lwt_direct}), but if a {e strictly} monadic package is
    wanted, {!Direct} could be dropped entirely and direct style left to
    {!Lwt_direct} over the real Lwt core. *)
module Direct : sig
  val await : 'a t -> 'a
  (** [await p] returns the value of [p], suspending the current fiber until [p]
      resolves, or raising the exception with which [p] was rejected. *)

  val yield : unit -> unit
  (** [yield ()] reschedules the current fiber behind the others, like
      [await (pause ())] but without allocating a promise. *)

  (** Direct-style I/O on raw non-blocking {!Unix.file_descr}s: each call
      suspends the fiber until the descriptor is ready, then performs the
      syscall. *)
  module Io : sig
    val wait_readable : Unix.file_descr -> unit
    val wait_writable : Unix.file_descr -> unit
    val read : Unix.file_descr -> bytes -> int -> int -> int
    val write : Unix.file_descr -> bytes -> int -> int -> int
    val accept : Unix.file_descr -> Unix.file_descr * Unix.sockaddr
    val connect : Unix.file_descr -> Unix.sockaddr -> unit
  end
end

(** {1 Interoperability with Lwt}

    These bridges let effect fibers cooperate with ordinary [Lwt] code (and
    libraries such as [Lwt_unix]). Under {!run}, Lwt's paused queue and event
    loop are driven automatically, so a real [Lwt.t] resolves while a fiber
    waits on it. *)

val of_lwt : 'a Lwt.t -> 'a t
(** [of_lwt p] is an effect promise that resolves when the Lwt promise [p]
    does. Cancelling it cancels [p]. *)

val await_lwt : 'a Lwt.t -> 'a
(** [await_lwt p] is [await (of_lwt p)]: block the current fiber on a real Lwt
    promise. *)

val to_lwt : 'a t -> 'a Lwt.t
(** [to_lwt p] exposes the effect promise [p] as an ordinary [Lwt.t]. *)

(** {1:compat Lwt-compatibility layer}

    Enough of {!Lwt}'s public API to compile code written against Lwt by aliasing
    [module Lwt = Lwt_effects]. Since {!bind} is non-blocking, Lwt's implicit
    concurrency is preserved, so this is a faithful drop-in for the covered API.
    Notable shape differences that remain: {!async} returns a promise (Lwt's
    returns [unit]); infix operators live in {!Infix}. *)

(** State of a promise, as in {!Lwt.state}. *)
type 'a state = Return of 'a | Fail of exn | Sleep

type 'a u
(** A resolver for a pending promise (as {!Lwt.u}). *)

val wait : unit -> 'a t * 'a u
val task : unit -> 'a t * 'a u
val wakeup : 'a u -> 'a -> unit
val wakeup_exn : 'a u -> exn -> unit
val wakeup_later : 'a u -> 'a -> unit
val wakeup_later_exn : 'a u -> exn -> unit
val state : 'a t -> 'a state
val is_sleeping : 'a t -> bool
val poll : 'a t -> 'a option
val of_result : ('a, exn) result -> 'a t

val fail_with : string -> 'a t
val fail_invalid_arg : string -> 'a t
val return_none : 'a option t
val return_nil : 'a list t
val return_some : 'a -> 'a option t
val return_ok : 'a -> ('a, 'b) result t
val return_error : 'b -> ('a, 'b) result t
val return_true : bool t
val return_false : bool t

val wrap : (unit -> 'a) -> 'a t
val finalize : (unit -> 'a t) -> (unit -> unit t) -> 'a t

val join : unit t list -> unit t
val all : 'a t list -> 'a list t
val nchoose : 'a t list -> 'a list t
val npick : 'a t list -> 'a list t

val on_any : 'a t -> ('a -> unit) -> (exn -> unit) -> unit
val on_success : 'a t -> ('a -> unit) -> unit
val on_failure : 'a t -> (exn -> unit) -> unit
val on_termination : 'a t -> (unit -> unit) -> unit
val on_cancel : 'a t -> (unit -> unit) -> unit

val async_exception_hook : (exn -> unit) ref
val dont_wait : (unit -> unit t) -> (exn -> unit) -> unit
val ignore_result : 'a t -> unit

(** Fiber-local storage, as {!Lwt.key}. A value set with {!with_value} is visible
    to {!get} for the dynamic extent of the callback, survives suspensions, and
    is inherited by fibers spawned (via {!async}) during that extent. *)
type 'a key

val new_key : unit -> 'a key
val get : 'a key -> 'a option
val with_value : 'a key -> 'a option -> (unit -> 'b) -> 'b

val no_cancel : 'a t -> 'a t
(** Approximation (cancellation isolation is not modelled). *)

val protected : 'a t -> 'a t
(** Approximation (cancellation isolation is not modelled). *)

(** Infix operators, as in {!Lwt.Infix}. *)
module Infix : sig
  val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t
  val ( =<< ) : ('a -> 'b t) -> 'a t -> 'b t
  val ( >|= ) : 'a t -> ('a -> 'b) -> 'b t
  val ( =|< ) : ('a -> 'b) -> 'a t -> 'b t
end

(** {1 Running} *)

val run : (unit -> 'a t) -> 'a
(** [run main] runs the scheduler until the promise returned by [main ()]
    resolves, then returns its value (or raises its exception). While the run
    queue is empty and the underlying {!Lwt_engine} still has registered events,
    [run] blocks in the event loop. *)

(** {1 Monadic I/O}

    Non-blocking I/O on raw {!Unix.file_descr}s: every interruptible call returns
    a promise ([_ t]), as in [Lwt_unix], so the async type is preserved and the
    call composes with {!bind}. The descriptors must be in non-blocking mode
    ([Unix.set_nonblock]). (For direct-style plain-value I/O, see {!Direct.Io}.) *)
module Io : sig
  val read : Unix.file_descr -> bytes -> int -> int -> int t
  (** Like [Lwt_unix.read]: a promise resolved with the number of bytes read. *)

  val write : Unix.file_descr -> bytes -> int -> int -> int t
  (** Like [Lwt_unix.write] (see {!read}). *)

  val accept : Unix.file_descr -> (Unix.file_descr * Unix.sockaddr) t
  (** Like [Lwt_unix.accept] (see {!read}). *)

  val connect : Unix.file_descr -> Unix.sockaddr -> unit t
  (** Like [Lwt_unix.connect] (see {!read}). *)
end

(**/**)

(** Internal primitives, exposed so that alternative back ends (e.g. io_uring)
    can drive the scheduler and build their own I/O operations. Not part of the
    stable API. *)
module Private : sig
  val enqueue : (unit -> unit) -> unit
  val outstanding : int ref
  val set_idle : (unit -> bool) -> unit
  val default_idle : unit -> bool
  val new_pending : unit -> 'a t
  val fill : 'a t -> ('a, exn) result -> unit
  val set_on_cancel : 'a t -> (unit -> unit) -> unit
end
