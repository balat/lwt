(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(** An {{:https://en.wikipedia.org/wiki/Io_uring} io_uring}-based Lwt engine,
    where Linux provides it.

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

    {b Availability.} The package installs on every system, but io_uring
    exists only on Linux, in a kernel that provides it and does not forbid it.
    Elsewhere, and in an opam switch without the [uring] library, this module
    has the same interface, with {!available} returning [false] and {!set}
    raising [Lwt_sys.Not_available]: a program links the same way everywhere
    and decides at run time. *)

(** {2 Installing the engine} *)

val available : unit -> bool
(** [available ()] is [true] if an io_uring ring can be created on this system
    (a recent enough Linux kernel, which does not forbid io_uring). It never
    raises. *)

val set : ?queue_depth:int -> unit -> unit
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
      large values can fail with [ENOMEM]). Defaults to [256]. *)

(** {2 The engine class} *)

type Lwt_engine.engine_id += Engine_id__uring

(** The io_uring engine. Creating an instance allocates a ring; it is released
    when the engine is {{!Lwt_engine.abstract.destroy} destroyed}. Raises
    [Lwt_sys.Not_available "io_uring"] if the system has no io_uring
    ([ENOSYS]) or forbids it ([EPERM]); other failures, such as [ENOMEM] for a
    queue too deep for the locked-memory limit, raise {!Unix.Unix_error}. *)
class uring : ?queue_depth:int -> unit -> object
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
