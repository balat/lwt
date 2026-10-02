(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(** Lwt engine on {{:https://en.wikipedia.org/wiki/Io_uring} io_uring}, where
    Linux provides it.

    io_uring is the Linux interface for asynchronous system calls. This module
    provides an {!Lwt_engine} engine that runs on it, and makes [Lwt_unix]
    perform its common I/O through it, without any change to the code that
    uses [Lwt_unix], [Lwt_io] or the libraries built on them.

    {2 Use}

    Call {!set_if_available} once, before {!Lwt_main.run}:
    {[
      let () =
        ignore (Lwt_uring.set_if_available ());
        Lwt_main.run (main ())
    ]}

    The package installs on every system. Where io_uring is missing, that is
    on any system but Linux, in an opam switch without the [uring] library, or
    on a kernel that lacks or forbids io_uring, {!set_if_available} returns
    [false] and the default engine stays. A program therefore links the same
    way everywhere and decides at run time. Setting the environment variable
    [LWT_URING] to [0] turns io_uring off without recompiling.

    {2 What runs on io_uring}

    Once the engine is installed:
    - waits for a descriptor to become readable or writable, and timers, are
      io_uring submissions, batched into one system call per iteration of the
      loop;
    - [Lwt_unix.read], [write], [read_bigarray] and [write_bigarray], which is
      the path of [Lwt_io], and [connect] are completion-based: the kernel
      performs the operation, and the promise resolves when it completes.
      Sockets use [recv] and [send], regular files and block devices read and
      write at the current position, other descriptors without a position;
    - the other operations of [Lwt_unix], such as [accept], [recv], [send],
      [pread] or [writev], keep their default implementation, whose waits run on
      the ring.

    {2 Differences from the default engines}

    - A completion-based operation reaches the kernel when the loop next runs.
      Cancelled before that, with [Lwt.cancel], [Lwt.pick] or a timeout, it
      never happens, as with the default engines. Cancelled later, its promise
      is rejected with [Lwt.Canceled] at once, and the operation is cancelled
      in the kernel, which may have performed it already. The bytes of such a
      read are not lost: the next read of the descriptor gets them, waiting if
      need be for the cancelled read to complete. Such a write stays written.
    - Closing or aborting a descriptor fails the completion-based operations
      in flight on it, unless they completed first, with what the default path
      raises: [Unix.Unix_error (EBADF, _, _)] for {!Lwt_unix.close}, the
      exception for {!Lwt_unix.abort}.
    - Replacing the engine, for instance with {!Lwt_engine.set}, cancels the
      completion-based operations in flight: their promises are rejected with
      [Lwt.Canceled], unless they completed first.
    - Writing to a socket whose peer has closed fails with [EPIPE], and raises
      no [SIGPIPE].
    - Timers count the time the system spends suspended.
    - In the child of {!Lwt_unix.fork}, the engine continues on a ring of its
      own. The operations the parent had in flight are lost to the child, as
      its pending jobs are. *)

(** {2 Installing the engine} *)

val available : unit -> bool
(** [available ()] is [true] if an io_uring ring can be created on this system
    (a recent enough Linux kernel, which does not forbid io_uring). It never
    raises. *)

val set : ?queue_depth:int -> ?deferred:bool -> unit -> unit
(** [set ?queue_depth ()] installs a fresh io_uring engine as the current Lwt
    engine, transferring the events registered on the previous engine (see
    {!Lwt_engine.set}).

    Raises [Lwt_sys.Not_available "io_uring"] where io_uring is missing or
    forbidden; {!set_if_available} falls back instead.

    @param queue_depth
      the depth of the submission queue, rounded up to a power of two by the
      kernel. It bounds how many submissions are batched into one system call,
      not how many descriptors or operations can be in flight: a full queue is
      flushed and the submission retried, and an iteration of the loop handles
      at most that many completions, leaving the others to the next one. A
      larger value costs locked memory, and a very large one can fail with
      [ENOMEM]. Defaults to [256].
    @param deferred
      whether to ask the kernel for [SINGLE_ISSUER] and [DEFER_TASKRUN]. They
      assume that one loop, on one system thread, owns the ring, and make the
      engine enter the ring asking for events on every iteration of the loop.
      Defaults to [false]: measured, they brought no gain. On a kernel older
      than 6.0, which refuses them, the engine uses a plain ring. *)

val set_if_available : ?queue_depth:int -> ?deferred:bool -> unit -> bool
(** [set_if_available ?queue_depth ?deferred ()] installs the io_uring engine as {!set}
    does and returns [true], where io_uring is available. Otherwise it leaves
    the current engine in place and returns [false]: on a system other than
    Linux, where the kernel lacks or forbids io_uring, and when the environment
    variable [LWT_URING] is set to [0], which turns io_uring off without
    recompiling. Other failures to create the ring raise, as with {!set}.

    This is the simplest way to use io_uring where it exists and the default
    engine elsewhere: call it once, before {!Lwt_main.run}.
    {[
      let () =
        ignore (Lwt_uring.set_if_available ());
        Lwt_main.run (main ())
    ]} *)

(** {2 The engine class} *)

type Lwt_engine.engine_id += Engine_id__uring

(** The io_uring engine. Creating an instance allocates a ring; it is released
    when the engine is {{!Lwt_engine.abstract.destroy} destroyed}. Raises
    [Lwt_sys.Not_available "io_uring"] if the system has no io_uring
    ([ENOSYS]) or forbids it ([EPERM]); other failures, such as [ENOMEM] for a
    queue too deep for the locked-memory limit, raise [Unix.Unix_error]. *)
class uring : ?queue_depth:int -> ?deferred:bool -> unit -> object
  inherit Lwt_engine.t
end

(** {2 Completion-based I/O on raw descriptors}

    These operations submit a read or a write to the ring of the installed
    engine and resolve when it completes. They work on any descriptor,
    including regular files, which readiness cannot wait for. A [Unix.close]
    of the descriptor does not stop the operations in flight on it; closing it
    through [Lwt_unix] does.

    They raise [Failure] if no io_uring engine is installed, and
    [Lwt_sys.Not_available] where io_uring is missing. They raise
    [Invalid_argument] if [pos] and [len] do not designate a valid range of
    the buffer. *)
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
