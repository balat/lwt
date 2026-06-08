(** io_uring back end for {!Lwt_effects} (POC, Linux only).

    A completion-based alternative to the default {!Lwt_engine} back end: I/O
    requests are submitted to an io_uring and resolved when the kernel posts
    their completion, allowing batched submission and removing the explicit
    readiness syscall. *)

val run :
  ?queue_depth:int ->
  ?buffer_blocks:int ->
  ?block_size:int ->
  (unit -> 'a Lwt_effects.t) ->
  'a
(** [run main] is like {!Lwt_effects.run}, but drives the scheduler with an
    io_uring (created with the given [queue_depth], default 256) instead of
    {!Lwt_engine}. I/O inside [main] uses {!Io} or {!Fixed}.

    A fixed buffer of [buffer_blocks] (default 256) chunks of [block_size]
    (default 4096) bytes is registered with the kernel for {!Fixed}. *)

(** Direct-style I/O issued through the ring. Buffers are {!Cstruct.t} (the
    kernel reads from / writes to them directly, with no userspace copy, but
    pins the pages on each call). Must be called inside {!run}. *)
module Io : sig
  val read : Unix.file_descr -> Cstruct.t -> int
  (** Submit a [read] and return the number of bytes read. *)

  val write : Unix.file_descr -> Cstruct.t -> int
  (** Submit a [write] and return the number of bytes written. *)

  val wait_readable : Unix.file_descr -> unit
  (** Wait (via io_uring poll) until [fd] is readable. *)

  val wait_writable : Unix.file_descr -> unit
  (** Wait (via io_uring poll) until [fd] is writable. *)

  val accept : Unix.file_descr -> Unix.file_descr * Unix.sockaddr
  (** Accept a connection on the (non-blocking) listening socket [fd]. *)

  val connect : Unix.file_descr -> Unix.sockaddr -> unit
  (** Connect the (non-blocking) socket [fd], waiting for completion. *)

  (** {2 Monadic, non-blocking I/O}

      Return a promise ([_ Lwt_effects.t]) resolved when the ring completes the
      request — so the async type is preserved. Compose with
      {!Lwt_effects.Compat} to write Lwt-style code running on io_uring. *)

  val read_m : Unix.file_descr -> Cstruct.t -> int Lwt_effects.t
  val write_m : Unix.file_descr -> Cstruct.t -> int Lwt_effects.t
  val accept_m : Unix.file_descr -> (Unix.file_descr * Unix.sockaddr) Lwt_effects.t
  val connect_m : Unix.file_descr -> Unix.sockaddr -> unit Lwt_effects.t
end

(** Zero-copy I/O through the ring's registered fixed buffer: the kernel keeps
    the buffer pinned, so [read_fixed]/[write_fixed] avoid mapping user pages on
    each call. Data lives in {!chunk}s allocated from the fixed buffer. Must be
    called inside {!run}. *)
module Fixed : sig
  type chunk
  (** A slice of the registered fixed buffer. *)

  val alloc : unit -> chunk
  (** Allocate a chunk from the fixed buffer (raises if exhausted). *)

  val free : chunk -> unit
  (** Return a chunk to the fixed buffer. *)

  val length : chunk -> int
  (** The chunk's block size in bytes. *)

  val to_cstruct : ?len:int -> chunk -> Cstruct.t
  (** Zero-copy view of the chunk. *)

  val to_string : ?len:int -> chunk -> string
  (** Copy the chunk's contents (first [len] bytes) to a string. *)

  val blit_string : string -> chunk -> unit
  (** Copy a string into the start of the chunk. *)

  val read : ?len:int -> Unix.file_descr -> chunk -> int
  (** Read into the chunk; returns the number of bytes read. *)

  val write : ?len:int -> Unix.file_descr -> chunk -> int
  (** Write the chunk (first [len] bytes); returns the number of bytes written. *)
end
