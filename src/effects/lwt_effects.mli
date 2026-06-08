(** An effect-based scheduler for Lwt-style promises (POC).

    This is a proof-of-concept reimplementation of Lwt's monadic core on top of
    OCaml 5 {{:https://ocaml.org/manual/5.3/effects.html} effects}. It keeps the
    familiar monadic interface ([return], [bind], [>>=], ...) so that the
    asynchronicity of a function stays visible in its type, while making the
    common case of {!bind} a plain function application with no heap allocation.

    {1 Representation}

    A value of type ['a t] is a {e promise cell}: it is either already resolved
    (with a value or an exception) or pending (with a list of waiters). This is
    the "design A" of the POC: promises are first-class, shareable and can be
    awaited several times — unlike a suspended-computation encoding.

    {1 The performance idea}

    With this representation:
    - [bind p f] when [p] is already resolved is just [f v]: no promise, no
      callback, no proxy is allocated. The compiler can inline it.
    - [bind p f] when [p] is pending performs an [Await] effect: the current
      fiber is suspended and its continuation is attached as a waiter of [p],
      instead of building the callback + proxy chain that classic Lwt allocates.

    {1 Execution model (and its caveat)}

    Code runs inside a fiber installed by {!run} (or {!async}). Because a pending
    {!bind} suspends the {e whole} current fiber until the awaited promise
    resolves, a linear chain [p >>= f >>= g] behaves exactly as in Lwt, but code
    written {e after} a pending [bind] in the same fiber is sequenced after it,
    not run concurrently. Concurrency is then obtained explicitly through
    {!async}, {!both} and {!choose} — as in Eio.

    {b Two flavours of bind.} If you need Lwt's {e implicit} concurrency (where
    [both (a >>= f) (b >>= g)] runs both branches without an explicit [async]),
    use {!mbind} / the {!Compat} module instead: that bind does not suspend the
    caller. The top-level {!bind} trades implicit concurrency for a cheaper,
    suspension-based implementation; {!Compat} preserves Lwt semantics at Lwt's
    allocation cost (minus the proxy machinery). See {!section:compat}. *)

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
(** [bind p f] is [f v] once [p] is fulfilled with [v]. If [p] is already
    fulfilled, this is a direct application with no allocation. If [f] raises or
    [p] is rejected, the result is a rejected promise. *)

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

(** {1 Awaiting} *)

val await : 'a t -> 'a
(** [await p] returns the value of [p], blocking the current fiber until [p]
    resolves, or raising the exception with which [p] was rejected. Must be
    called inside {!run}/{!async}. *)

(** {1 Error handling} *)

val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t
(** [catch f h] runs [f ()] and, if its promise is rejected with [e], runs
    [h e]. Note that inside a fiber you can equivalently write a direct-style
    [try await p with ...], which is often clearer. *)

val try_bind : (unit -> 'a t) -> ('a -> 'b t) -> (exn -> 'b t) -> 'b t
(** [try_bind f g h] runs [f ()]; on success its value is passed to [g], on
    rejection the exception is passed to [h]. *)

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
    [module Lwt = Lwt_effects].

    {b Important semantic caveat}: {!bind} here suspends the current fiber, so
    Lwt's {e implicit concurrency} is not preserved — e.g.
    [both (a >>= f) (b >>= g)] runs sequentially, not concurrently. Use {!async}
    explicitly for concurrency. This layer maps the API {e shape}, not Lwt's
    bind semantics. Notable shape differences that remain: {!async} returns a
    promise (Lwt's returns [unit]); infix operators live in {!Infix}. *)

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

val mbind : 'a t -> ('a -> 'b t) -> 'b t
(** The {e semantics-preserving} bind: unlike {!bind} (which suspends the
    current fiber), [mbind] does not block the caller — it allocates a result
    promise and a callback — so Lwt's {b implicit concurrency} is preserved
    ([both (a >>= f) (b >>= g)] runs both branches). This is Lwt's trade-off:
    one promise + one callback per pending bind, but no proxy machinery. *)

(** A Lwt-semantics facade: same shape as the top-level API, but [bind]/[>>=]/
    [map]/[both]/[join] are the non-blocking {!mbind} (implicit concurrency
    preserved). Closer to a drop-in [Lwt]: [module Lwt = Lwt_effects.Compat]. *)
module Compat : sig
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t
  val map : ('a -> 'b) -> 'a t -> 'b t
  val ( >|= ) : 'a t -> ('a -> 'b) -> 'b t
  val both : 'a t -> 'b t -> ('a * 'b) t
  val join : unit t list -> unit t

  module Infix : sig
    val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t
    val ( =<< ) : ('a -> 'b t) -> 'a t -> 'b t
    val ( >|= ) : 'a t -> ('a -> 'b) -> 'b t
    val ( =|< ) : ('a -> 'b) -> 'a t -> 'b t
  end

  module Syntax : sig
    val ( let* ) : 'a t -> ('a -> 'b t) -> 'b t
    val ( let+ ) : 'a t -> ('a -> 'b) -> 'b t
    val ( and* ) : 'a t -> 'b t -> ('a * 'b) t
    val ( and+ ) : 'a t -> 'b t -> ('a * 'b) t
  end
end

(** {1 Running} *)

val run : (unit -> 'a t) -> 'a
(** [run main] runs the scheduler until the promise returned by [main ()]
    resolves, then returns its value (or raises its exception). While the run
    queue is empty and the underlying {!Lwt_engine} still has registered events,
    [run] blocks in the event loop. *)

(** {1 Non-blocking I/O}

    Direct-style I/O on raw {!Unix.file_descr}s. Each call suspends the current
    fiber on the {!Lwt_engine} until the descriptor is ready, then performs the
    syscall. The descriptors must be in non-blocking mode
    ([Unix.set_nonblock]). These functions must be called inside a fiber
    ({!run}/{!async}). *)
module Io : sig
  val wait_readable : Unix.file_descr -> unit
  (** Suspend until the descriptor is readable. *)

  val wait_writable : Unix.file_descr -> unit
  (** Suspend until the descriptor is writable. *)

  val read : Unix.file_descr -> bytes -> int -> int -> int
  (** Like [Unix.read], retrying on [EAGAIN]/[EWOULDBLOCK]/[EINTR]. *)

  val write : Unix.file_descr -> bytes -> int -> int -> int
  (** Like [Unix.write], retrying on [EAGAIN]/[EWOULDBLOCK]/[EINTR]. *)

  val accept : Unix.file_descr -> Unix.file_descr * Unix.sockaddr
  (** Like [Unix.accept], waiting for an incoming connection. *)

  val connect : Unix.file_descr -> Unix.sockaddr -> unit
  (** Like [Unix.connect], waiting for an in-progress connection to complete. *)

  val read_m : Unix.file_descr -> bytes -> int -> int -> int t
  (** Monadic, non-blocking read: returns a promise resolved by a callback when
      the descriptor is ready (like [Lwt_unix.read], no fiber). Composes with
      {!mbind} / {!Compat} to express Lwt-style monadic I/O. *)

  val write_m : Unix.file_descr -> bytes -> int -> int -> int t
  (** Monadic, non-blocking write (see {!read_m}). *)

  val accept_m : Unix.file_descr -> (Unix.file_descr * Unix.sockaddr) t
  (** Monadic, non-blocking accept (see {!read_m}). *)

  val connect_m : Unix.file_descr -> Unix.sockaddr -> unit t
  (** Monadic, non-blocking connect (see {!read_m}). *)
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
