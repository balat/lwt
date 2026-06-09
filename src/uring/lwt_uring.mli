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
    {!Lwt_sys.Not_available} (wrapping the underlying error) if io_uring is not
    available on this system. *)
class uring : ?queue_depth:int -> unit -> object
  inherit Lwt_engine.t
end
