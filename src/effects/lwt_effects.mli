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
    not run concurrently. Concurrency is obtained explicitly through {!async},
    {!both} and {!choose} — as in Eio. This is a deliberate POC simplification;
    see the session report for the drop-in story. *)

type 'a t
(** A promise for a value of type ['a]. *)

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
