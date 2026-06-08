(** io_uring back end for {!Lwt_effects} (POC, Linux only).

    A completion-based alternative to the default {!Lwt_engine} back end: I/O
    requests are submitted to an io_uring and resolved when the kernel posts
    their completion, allowing batched submission and removing the explicit
    readiness syscall. *)

val run : ?queue_depth:int -> (unit -> 'a Lwt_effects.t) -> 'a
(** [run main] is like {!Lwt_effects.run}, but drives the scheduler with an
    io_uring (created with the given [queue_depth], default 256) instead of
    {!Lwt_engine}. I/O inside [main] must use the {!Io} module below. *)

(** Direct-style I/O issued through the ring. Buffers are {!Cstruct.t} (the
    kernel reads from / writes to them directly). Must be called inside
    {!run}. *)
module Io : sig
  val read : Unix.file_descr -> Cstruct.t -> int
  (** Submit a [read] and return the number of bytes read. *)

  val write : Unix.file_descr -> Cstruct.t -> int
  (** Submit a [write] and return the number of bytes written. *)
end
