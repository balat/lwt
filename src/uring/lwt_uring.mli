(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(** An {{:https://en.wikipedia.org/wiki/Io_uring} io_uring}-based Lwt engine
    (Linux only).

    This module provides an alternative {!Lwt_engine} implementation backed by
    Linux's io_uring interface, in place of the default libev/select engines. It
    is a drop-in replacement: installing it with {!set} (or
    [Lwt_engine.set (new Lwt_uring.uring ())]) makes the whole of [Lwt_unix]
    run on io_uring, without any change to application code.

    {b This is the readiness-based engine (stage 1).} File-descriptor waits are
    implemented with io_uring {e poll} submissions ([IORING_OP_POLL_ADD]) and
    timers with io_uring {e timeout} submissions; [Lwt_unix] still performs the
    actual [read]/[write]/… syscalls once a descriptor is reported ready, exactly
    as with libev or select. The benefit over libev/select is that readiness and
    timer registrations are {e batched} into a single [io_uring_enter] system
    call per loop iteration. A later, completion-based stage will additionally
    offload the [read]/[write]/[accept]/[connect] syscalls to the ring.

    {b Availability.} The engine requires a Linux kernel with io_uring support.
    Use {!available} to test at runtime before installing it. *)

(** {2 Installing the engine} *)

val available : unit -> bool
(** [available ()] is [true] if an io_uring ring can be created on this system
    (a recent enough Linux kernel). It never raises. *)

val set : ?queue_depth:int -> ?deferred:bool -> unit -> unit
(** [set ?queue_depth ()] installs a fresh io_uring engine as the current Lwt
    engine, transferring the events registered on the previous engine (see
    {!Lwt_engine.set}).

    @param queue_depth
      the io_uring submission-queue depth, rounded up to a power of two by the
      kernel. This is a batching/memory tuning knob, {e not} a hard limit on the
      number of monitored descriptors: when the submission queue is momentarily
      full it is flushed and the submission retried, and the kernel backlogs
      completions if the completion queue overflows. A larger value batches more
      registrations per system call at the cost of more locked memory (very
      large values can fail with [ENOMEM]). Defaults to [256].
    @param deferred
      whether to ask the kernel for [SINGLE_ISSUER] and [DEFER_TASKRUN]. A
      per-domain ring is what makes the first legitimate: one loop, on one system
      thread, owns the ring. Defaults to [false], and falls back silently to a
      plain ring on a kernel older than 6.0.

      Off by default on two grounds. Measured here, twice, with both
      configurations interleaved in one process, they bought nothing: -1.7% on a
      sequential ping-pong and +3.6% on a 50-connection keep-alive load, both
      inside the noise. And DEFER_TASKRUN has a condition attached: the kernel runs
      completion work when the ring is entered ASKING FOR EVENTS, which this engine
      only does when it blocks, so a loop that never goes idle could starve its own
      I/O. Turning them on for good means entering with GETEVENTS on every
      iteration, and measuring that somewhere quieter. *)

(** {2 The engine class} *)

type Lwt_engine.engine_id += Engine_id__uring

(** The io_uring engine. Creating an instance allocates a ring; it is released
    when the engine is {{!Lwt_engine.abstract.destroy} destroyed}. Raises
    {!Lwt_sys.Not_available} (wrapping the underlying error) if io_uring is not
    available on this system. *)
class uring : ?queue_depth:int -> ?deferred:bool -> unit -> object
  inherit Lwt_engine.t
end

(** {2 Completion-based I/O}

    {b This is stage 2.} Unlike {!Lwt_unix}'s default operations — which wait for
    a descriptor to become ready and then perform the syscall — these submit the
    actual [read]/[write] to io_uring and resolve their promise on completion.
    The kernel performs the transfer, so there is no separate readiness syscall,
    and this works on regular files too (where readiness polling does not).

    They require a {!uring} engine to be installed (via {!set}); otherwise they
    raise [Failure]. The descriptor is the raw {!Unix.file_descr} (use
    {!Lwt_unix.unix_file_descr} to obtain it). For now this is an explicit API;
    a later step will route {!Lwt_unix}'s own operations through it transparently
    when the io_uring engine is active. *)
module Io : sig
  type bigarray =
    (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

  val read : Unix.file_descr -> bytes -> int -> int -> int Lwt.t
  (** [read fd buf pos len] reads at most [len] bytes into [buf] at [pos]
      through io_uring, resolving with the number of bytes read (a [bytes]
      buffer requires one copy from the ring's [Cstruct]; see {!read_bigarray}
      for the copy-free variant). *)

  val write : Unix.file_descr -> bytes -> int -> int -> int Lwt.t
  (** [write fd buf pos len] writes up to [len] bytes of [buf] from [pos]
      through io_uring, resolving with the number of bytes written. *)

  val read_bigarray : Unix.file_descr -> bigarray -> int -> int -> int Lwt.t
  (** Like {!read}, into a bigarray with no intermediate copy. *)

  val write_bigarray : Unix.file_descr -> bigarray -> int -> int -> int Lwt.t
  (** Like {!write}, from a bigarray with no intermediate copy. *)
end
