(* io_uring back end for the Lwt_effects scheduler (POC, Linux only).

   Unlike the default Lwt_engine back end, which is readiness-based (it waits for
   a descriptor to become ready, then performs the syscall), io_uring is
   completion-based: an I/O request is submitted to a ring and the kernel posts a
   completion with the syscall result. This lets many requests be submitted in a
   single system call (batching) and removes the explicit readiness syscall.

   The integration plugs into Lwt_effects via its [Private] interface: each I/O
   call queues a submission, suspends the fiber, and the scheduler's idle hook
   blocks on the ring and resumes fibers as completions arrive. *)

module U = Uring
module P = Lwt_effects.Private

(* The ring's user data is the completion handler [int -> unit] that resolves
   the suspended promise with the syscall result. *)
let ring : (int -> unit) U.t option ref = ref None

(* Region carved out of the ring's registered fixed buffer (for zero-copy
   [read_fixed]/[write_fixed], which avoid per-I/O page pinning). *)
let region : U.Region.t option ref = ref None

let current_ring () =
  match !ring with
  | Some r -> r
  | None -> failwith "Lwt_effects_uring: I/O attempted outside Lwt_effects_uring.run"

let current_region () =
  match !region with
  | Some r -> r
  | None -> failwith "Lwt_effects_uring: no fixed buffer (call inside run)"

(* Queue an I/O request, suspend the fiber, and return the syscall result (or
   raise the corresponding Unix error). [submit] performs the actual
   [Uring.<op>] call with the completion handler. *)
let perform submit =
  let r = current_ring () in
  let p = P.new_pending () in
  incr P.outstanding;
  let handler result =
    decr P.outstanding;
    if result < 0 then
      P.fill p
        (Error (Unix.Unix_error (Unix.EUNKNOWNERR (-result), "io_uring", "")))
    else P.fill p (Ok result)
  in
  (match submit r handler with
  | Some _job -> ()
  | None -> failwith "Lwt_effects_uring: submission queue full");
  Lwt_effects.await p

(* The scheduler's idle hook: block on the ring for one completion, then drain
   any further ready completions without blocking. [Uring.wait] submits queued
   requests automatically. *)
let idle r () =
  if !P.outstanding > 0 then begin
    (match U.wait r with U.Some { result; data } -> data result | U.None -> ());
    let rec drain () =
      match U.get_cqe_nonblocking r with
      | U.Some { result; data } ->
        data result;
        drain ()
      | U.None -> ()
    in
    drain ();
    true
  end
  else false

let run ?(queue_depth = 256) ?(buffer_blocks = 256) ?(block_size = 4096) main =
  let r = U.create ~queue_depth () in
  ring := Some r;
  (* Register a fixed buffer and carve it into a region of equal-sized chunks. *)
  let buf =
    Bigarray.Array1.create Bigarray.char Bigarray.c_layout
      (block_size * buffer_blocks)
  in
  (match U.set_fixed_buffer r buf with
  | Ok () -> region := Some (U.Region.init ~block_size buf buffer_blocks)
  | Error `ENOMEM -> failwith "Lwt_effects_uring: could not register fixed buffer");
  P.set_idle (idle r);
  Fun.protect
    ~finally:(fun () ->
      P.set_idle P.default_idle;
      ring := None;
      region := None;
      U.exit r)
    (fun () -> Lwt_effects.run main)

(* Sockets must use a file offset of zero (see {!Uring.read}). *)
let socket_offset = Optint.Int63.zero

module Io = struct
  let read fd (buf : Cstruct.t) =
    perform (fun r h -> U.read r ~file_offset:socket_offset fd buf h)

  let write fd (buf : Cstruct.t) =
    perform (fun r h -> U.write r ~file_offset:socket_offset fd buf h)

  (* Readiness via io_uring's poll, used to build accept/connect on top of the
     plain Unix syscalls (the descriptors must be non-blocking). *)
  let wait_readable fd =
    ignore (perform (fun r h -> U.poll_add r fd U.Poll_mask.pollin h))

  let wait_writable fd =
    ignore (perform (fun r h -> U.poll_add r fd U.Poll_mask.pollout h))

  let rec accept fd =
    match Unix.accept fd with
    | res -> res
    | exception
        Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) ->
      wait_readable fd;
      accept fd

  let connect fd addr =
    match Unix.connect fd addr with
    | () -> ()
    | exception
        Unix.Unix_error ((Unix.EINPROGRESS | Unix.EWOULDBLOCK), _, _) -> (
      wait_writable fd;
      match Unix.getsockopt_error fd with
      | None -> ()
      | Some err -> raise (Unix.Unix_error (err, "connect", "")))
end

(* Zero-copy I/O through the registered fixed buffer. Data lives in chunks
   allocated from the region; [read]/[write] avoid per-I/O page pinning. *)
module Fixed = struct
  type chunk = U.Region.chunk

  let alloc () = U.Region.alloc (current_region ())
  let free = U.Region.free
  let length = U.Region.length
  let to_cstruct = U.Region.to_cstruct
  let to_string = U.Region.to_string

  let blit_string src chunk =
    Cstruct.blit_from_string src 0 (U.Region.to_cstruct chunk) 0
      (String.length src)

  let read ?len fd chunk =
    perform (fun r h -> U.read_chunk ?len r ~file_offset:socket_offset fd chunk h)

  let write ?len fd chunk =
    perform (fun r h ->
      U.write_chunk ?len r ~file_offset:socket_offset fd chunk h)
end
