(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* A readiness-based Lwt engine on top of Linux io_uring.

   The default Lwt engines (libev, select) register file descriptors with the
   kernel and, on each loop iteration, ask "which are ready now?". This engine
   does the same but through io_uring: a descriptor wait becomes a one-shot
   [IORING_OP_POLL_ADD] submission, a timer becomes an [IORING_OP_TIMEOUT]
   submission, and one [io_uring_enter] (via {!Uring.wait}) both flushes all
   pending submissions and reaps the completions. Registrations therefore cost
   no immediate system call — they are batched until the next loop iteration.

   To match the level-triggered semantics that {!Lwt_engine} expects (a
   registered callback fires every time the descriptor is ready, until the event
   is stopped), each poll is automatically {e re-armed} after it fires, as long
   as the event has not been stopped. Stopping an event cancels its in-flight
   submission with [IORING_OP_ASYNC_CANCEL]. *)

module U = Uring

(* User data attached to every submission, used to dispatch the completion and,
   for polls and repeating timers, to re-arm the operation. *)
type req =
  | Poll of poll_req
  | Timer of timer_req
  | Io of (int -> unit)
      (* A completion-based I/O submission (read/write/...). The handler is
         called with the syscall result (negative for an errno). *)
  | Accept of accept_stream
      (* A multishot accept on a listening socket: ONE submission, one
         completion per accepted connection (no syscall per accept). *)
  | Cancel
      (* Completion of an [Uring.cancel] submission; nothing to do. *)

(* Multishot-accept state of one listening socket. Accepted descriptors are
   handed to waiting [Lwt_unix.accept] callers (FIFO), or queued until one
   arrives — they were going to be accepted anyway, the kernel merely got
   ahead. Cancelled waiters are skipped via [Lwt.is_sleeping] on their
   promise. *)
and accept_stream = {
  ls_fd : Unix.file_descr; (* the listening socket *)
  mutable armed : req U.job option; (* the in-flight multishot, if any *)
  accepted : Unix.file_descr Queue.t; (* accepted, not yet claimed *)
  acceptors :
    ((Unix.file_descr * Unix.sockaddr) Lwt.t
    * (Unix.file_descr * Unix.sockaddr) Lwt.u)
    Queue.t; (* waiting accept calls *)
}

and poll_req = {
  fd : Unix.file_descr;
  mask : U.Poll_mask.t;
  callback : unit -> unit;
  mutable active : bool;
      (* Set to [false] when the event is stopped; a completion for an inactive
         poll is dropped and not re-armed. *)
  mutable job : req U.job option;
      (* The in-flight submission, kept so the event can be cancelled. Set to
         [None] as soon as its completion is collected. *)
}

and timer_req = {
  ns : int64;
  repeat : bool;
  t_callback : unit -> unit;
  mutable t_active : bool;
  mutable t_job : req U.job option;
}

type Lwt_engine.engine_id += Engine_id__uring

(* Submit [data], retrying once after an explicit flush if the submission queue
   is momentarily full. Raises if it is still full afterwards (the configured
   [queue_depth] is too small for the number of concurrent registrations). *)
let submit ring data make =
  match make ring data with
  | Some job -> job
  | None ->
    ignore (U.submit ring);
    (match make ring data with
     | Some job -> job
     | None ->
       failwith "Lwt_uring: submission queue full (increase ~queue_depth)")

let submit_poll ring pr =
  pr.job <- Some (submit ring (Poll pr)
                    (fun ring data -> U.poll_add ring pr.fd pr.mask data))

let submit_timer ring tr =
  tr.t_job <- Some (submit ring (Timer tr)
                      (fun ring data -> U.timeout ring U.Boottime tr.ns data))

(* Cancel an in-flight submission. The original operation still produces a
   completion (with [ECANCELED]), which is dropped because its event is no
   longer active; the cancel submission itself produces a [Cancel] completion,
   which is ignored. *)
let cancel ring job =
  try ignore (U.cancel ring job Cancel)
  with Invalid_argument _ -> ()
  (* The job was already collected — nothing to cancel. *)

(* ---- multishot accept ---- *)

(* Set to [false] when the kernel rejects a multishot accept (EINVAL: Linux
   < 5.19): the back end then declines and Lwt_unix uses its readiness path. *)
let multishot_supported = ref true

(* One stream per listening descriptor, created at the first hooked accept and
   torn down when the engine is destroyed (or on accept error). *)
let accept_streams : (Unix.file_descr, accept_stream) Hashtbl.t =
  Hashtbl.create 8

let arm_accept ring str =
  str.armed <-
    Some
      (submit ring (Accept str) (fun ring data ->
           U.accept_multishot ~cloexec:false ~nonblock:true ring str.ls_fd data))

(* Hand [fd] to the first still-waiting acceptor, or queue it. The peer
   address comes from [getpeername] (a multishot accept collects none). If the
   peer already vanished (ENOTCONN race), drop the connection and keep the
   waiter for the next one — the readiness path would never have seen that
   connection either. *)
let deliver_accepted str fd =
  match Unix.getpeername fd with
  | exception Unix.Unix_error (_, _, _) -> (try Unix.close fd with _ -> ())
  | addr ->
    let rec wake () =
      match Queue.take_opt str.acceptors with
      | None -> Queue.push fd str.accepted
      | Some (p, r) ->
        if Lwt.is_sleeping p then Lwt.wakeup_later r (fd, addr) else wake ()
    in
    wake ()

(* A real accept error: fail the current waiters (the stream is disarmed by
   the caller and re-armed lazily by the next accept call). *)
let reject_acceptors str exn =
  Queue.iter
    (fun (p, r) -> if Lwt.is_sleeping p then Lwt.wakeup_later_exn r exn)
    str.acceptors;
  Queue.clear str.acceptors

let teardown_accept_streams () =
  Hashtbl.iter
    (fun _ str ->
      Queue.iter (fun fd -> try Unix.close fd with _ -> ()) str.accepted;
      Queue.clear str.accepted;
      reject_acceptors str Lwt.Canceled;
      str.armed <- None)
    accept_streams;
  Hashtbl.reset accept_streams

let dispatch ring result more data =
  match data with
  | Cancel -> ()
  | Io handler -> handler result
  | Poll pr ->
    pr.job <- None;
    if pr.active then begin
      pr.callback ();
      (* The callback may have stopped the event; only re-arm if still active. *)
      if pr.active then submit_poll ring pr
    end
  | Timer tr ->
    tr.t_job <- None;
    if tr.t_active then begin
      tr.t_callback ();
      if tr.repeat && tr.t_active then submit_timer ring tr
    end
  | Accept str ->
    if not more then str.armed <- None;
    (* The stream is live iff it is still the table entry for its descriptor
       (physical equality: after a close the number may already name a new
       listener with its own stream). Late completions for a dead stream must
       not deliver or re-arm. *)
    let live =
      match Hashtbl.find_opt accept_streams str.ls_fd with
      | Some s -> s == str
      | None -> false
    in
    if result >= 0 then begin
      let fd = U.file_descr_of_accept_result result in
      if live then begin
        deliver_accepted str fd;
        (* [more = false] on a success means the operation stopped (e.g. CQ
           pressure): re-arm so the stream keeps accepting. *)
        if not more then arm_accept ring str
      end
      else try Unix.close fd with _ -> ()
    end
    else if result <> -125 (* ECANCELED: stream torn down, nothing to do *)
            && live
    then begin
      let error = U.error_of_errno result in
      if error = Unix.EINVAL then multishot_supported := false;
      Hashtbl.remove accept_streams str.ls_fd;
      reject_acceptors str (Unix.Unix_error (error, "accept", ""))
    end

(* The ring of the currently-installed io_uring engine, if any. It is used by
   the completion-based I/O of {!Io}, which must submit to the same ring that the
   engine's [iter] reaps. Set when an engine is created, cleared when it is
   destroyed (using physical equality so that replacing one uring engine with
   another keeps the pointer on the live ring). *)
let the_ring : req U.t option ref = ref None

class uring ?(queue_depth = 256) () = object
  inherit Lwt_engine.abstract

  val ring : req U.t = U.create ~queue_depth ()

  initializer the_ring := Some ring

  method id = Engine_id__uring

  method private cleanup =
    (match !the_ring with Some r when r == ring -> the_ring := None | _ -> ());
    teardown_accept_streams ();
    U.exit ring

  method private register_readable fd f =
    let pr =
      { fd; mask = U.Poll_mask.pollin; callback = f; active = true; job = None }
    in
    submit_poll ring pr;
    lazy (
      pr.active <- false;
      match pr.job with Some job -> cancel ring job | None -> ())

  method private register_writable fd f =
    let pr =
      { fd; mask = U.Poll_mask.pollout; callback = f; active = true; job = None }
    in
    submit_poll ring pr;
    lazy (
      pr.active <- false;
      match pr.job with Some job -> cancel ring job | None -> ())

  method private register_timer delay repeat f =
    let ns = Int64.of_float (delay *. 1e9) in
    let tr = { ns; repeat; t_callback = f; t_active = true; t_job = None } in
    submit_timer ring tr;
    lazy (
      tr.t_active <- false;
      match tr.t_job with Some job -> cancel ring job | None -> ())

  method iter block =
    ignore (U.submit ring);
    (* When [block] is requested and the ring has outstanding operations, wait
       for at least one completion; otherwise just harvest what is ready. With
       nothing outstanding there is nothing to wait for, so we never block. *)
    if block && U.active_ops ring > 0 then begin
      match U.wait ring with
      | U.Some { result; data; more } -> dispatch ring result more data
      | U.None -> ()
    end;
    let rec drain () =
      match U.get_cqe_nonblocking ring with
      | U.Some { result; data; more } ->
        dispatch ring result more data;
        drain ()
      | U.None -> ()
    in
    drain ()
end

let get_ring () =
  match !the_ring with
  | Some r -> r
  | None ->
    failwith "Lwt_uring.Io: no io_uring engine installed (use Lwt_uring.set)"

(* Offset [-1] tells io_uring to use the descriptor's current position, like
   [read(2)]/[write(2)] — used for seekable files. *)
let current_offset = Optint.Int63.minus_one

(* Submit a completion-based operation built by [make] and return a promise
   resolved with [post result], or rejected with the corresponding
   [Unix.Unix_error]. [post] runs in the completion handler (e.g. the
   bounce-buffer blit of the bytes read path) so no extra promise is allocated
   on the per-operation hot path. Cancelling the promise cancels the in-flight
   submission. *)
let submit_io op_name post make =
  let ring = get_ring () in
  let waiter, wakener = Lwt.task () in
  let job = ref None in
  let handler result =
    job := None;
    if result < 0 then
      Lwt.wakeup_exn wakener
        (Unix.Unix_error (U.error_of_errno result, op_name, ""))
    else Lwt.wakeup wakener (post result)
  in
  (match make ring (Io handler) with
   | Some j -> job := Some j
   | None ->
     ignore (U.submit ring);
     (match make ring (Io handler) with
      | Some j -> job := Some j
      | None -> failwith "Lwt_uring.Io: submission queue full"));
  Lwt.on_cancel waiter (fun () ->
    match !job with
    | Some j -> (try ignore (U.cancel ring j Cancel) with Invalid_argument _ -> ())
    | None -> ());
  waiter

(* Result adapters for [submit_io]'s [post] (defined once: the common cases
   allocate no per-operation closure). *)
let int_result : int -> int = fun n -> n
let unit_result : int -> unit = fun _ -> ()

(* Pick the io_uring operation to read into / write from a [Cstruct.t], by
   descriptor kind:
   - sockets use [recv]/[send] (offsetless — the correct op, and reading/writing
     a socket through positioned [read]/[write] with offset -1 can stall);
   - regular files use positioned [read]/[write] at the current offset (-1);
   - other (pipes, ttys, …) are non-seekable, so [read]/[write] with offset 0
     (the kernel ignores it). *)
let read_op kind fd cs =
  match (kind : Unix.file_kind) with
  | Unix.S_SOCK -> fun ring data -> U.recv_msg ring fd (U.Msghdr.create [ cs ]) data
  | Unix.S_REG | Unix.S_BLK ->
    fun ring data -> U.read ring ~file_offset:current_offset fd cs data
  | _ -> fun ring data -> U.read ring ~file_offset:Optint.Int63.zero fd cs data

let write_op kind fd cs =
  match (kind : Unix.file_kind) with
  | Unix.S_SOCK -> fun ring data -> U.send_msg ring fd [ cs ] data
  | Unix.S_REG | Unix.S_BLK ->
    fun ring data -> U.write ring ~file_offset:current_offset fd cs data
  | _ -> fun ring data -> U.write ring ~file_offset:Optint.Int63.zero fd cs data

module Io = struct
  type bigarray =
    (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

  (* Positioned read/write at the descriptor's current offset, suited to regular
     files and general use. For sockets, prefer the transparent {!Lwt_unix} path
     (which uses [recv]/[send]). *)
  let read fd buf pos len =
    let cs = Cstruct.create_unsafe len in
    submit_io "read"
      (fun n -> Cstruct.blit_to_bytes cs 0 buf pos n; n)
      (fun ring data -> U.read ring ~file_offset:current_offset fd cs data)

  let write fd buf pos len =
    let cs = Cstruct.create_unsafe len in
    Cstruct.blit_from_bytes buf pos cs 0 len;
    submit_io "write" int_result (fun ring data ->
      U.write ring ~file_offset:current_offset fd cs data)

  let read_bigarray fd buf pos len =
    let cs = Cstruct.of_bigarray ~off:pos ~len buf in
    submit_io "read" int_result (fun ring data ->
      U.read ring ~file_offset:current_offset fd cs data)

  let write_bigarray fd buf pos len =
    let cs = Cstruct.of_bigarray ~off:pos ~len buf in
    submit_io "write" int_result (fun ring data ->
      U.write ring ~file_offset:current_offset fd cs data)
end

(* Transparent routing of Lwt_unix.{read,write,…} through io_uring. The backend
   self-gates on [the_ring]: it takes over only while a uring engine is
   installed, and declines (so Lwt_unix uses its default path) otherwise. It is
   installed once, when this module is linked; with no uring engine current it
   has no effect. The operation is chosen per descriptor kind (see {!read_op}),
   so sockets, files and pipes are each handled correctly. *)

(* Possible improvement: the [bytes] read/write below allocate a fresh off-heap
   [Cstruct] and memcpy per call (bytes live on the movable GC heap, so io_uring
   needs a stable buffer). At large payloads this copy dominates and makes the
   bytes path slower than libev (the bigarray path used by Lwt_io/cohttp is
   copy-free and faster — see the benchmark matrix). Mitigations, if ever needed:
   a reusable per-fd bounce buffer or registered fixed buffers for the bytes
   path. The bounce buffers use [Cstruct.create_unsafe] (no zero-fill: writes
   fully overwrite [0,len) and reads only expose the [n] bytes the kernel
   wrote), and the result post-processing runs inside the completion handler
   ([submit_io]'s [post]), so a read costs one promise, not two. *)
let completion_backend : Lwt_unix.completion_io =
  let read ch buf pos len =
    match !the_ring with
    | None -> None
    | Some _ ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.create_unsafe len in
      Some
        (submit_io "read"
           (fun n -> Cstruct.blit_to_bytes cs 0 buf pos n; n)
           (read_op kind fd cs))
  in
  let write ch buf pos len =
    match !the_ring with
    | None -> None
    | Some _ ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.create_unsafe len in
      Cstruct.blit_from_bytes buf pos cs 0 len;
      Some (submit_io "write" int_result (write_op kind fd cs))
  in
  let read_bigarray ch buf pos len =
    match !the_ring with
    | None -> None
    | Some _ ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.of_bigarray ~off:pos ~len buf in
      Some (submit_io "read" int_result (read_op kind fd cs))
  in
  let write_bigarray ch buf pos len =
    match !the_ring with
    | None -> None
    | Some _ ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.of_bigarray ~off:pos ~len buf in
      Some (submit_io "write" int_result (write_op kind fd cs))
  in
  (* Completion-based [connect]: submit IORING_OP_CONNECT and resolve when the
     connection completes (result 0) or fails (negative errno, mapped by
     [submit_io]). The descriptor is the user's own socket — no fd is created, so
     no flag policy is involved. *)
  let connect ch addr =
    match !the_ring with
    | None -> None
    | Some _ ->
      let fd = Lwt_unix.unix_file_descr ch in
      Some
        (submit_io "connect" unit_result (fun ring data ->
           U.connect ring fd addr data))
  in
  (* [accept] uses a MULTISHOT accept (IORING_OP_ACCEPT +
     IORING_ACCEPT_MULTISHOT, Linux >= 5.19): one submission per listening
     socket, one completion per accepted connection — no submission, no
     accept(2) and no fcntl per accept (the kernel applies SOCK_NONBLOCK and
     Lwt's no-cloexec default directly). Routing the SINGLE-shot accept had
     measured slower (forced SOCK_CLOEXEC needing a compensating fcntl, no
     batching); multishot removes both objections. On kernels without
     multishot support the first completion is EINVAL: the back end then
     declines for good and Lwt_unix falls back to its readiness path. *)
  let accept ch =
    match !the_ring with
    | None -> None
    | Some ring ->
      if not !multishot_supported then None
      else begin
        let fd = Lwt_unix.unix_file_descr ch in
        let str =
          match Hashtbl.find_opt accept_streams fd with
          | Some str -> str
          | None ->
            let str =
              {
                ls_fd = fd;
                armed = None;
                accepted = Queue.create ();
                acceptors = Queue.create ();
              }
            in
            Hashtbl.add accept_streams fd str;
            str
        in
        if str.armed = None then arm_accept ring str;
        match Queue.take_opt str.accepted with
        | Some afd -> (
          match Unix.getpeername afd with
          | addr -> Some (Lwt.return (afd, addr))
          | exception Unix.Unix_error (_, _, _) ->
            (* The peer vanished while queued: drop it and wait for the next
               connection like the readiness path would. *)
            (try Unix.close afd with _ -> ());
            let p, r = Lwt.task () in
            Queue.push (p, r) str.acceptors;
            Some p)
        | None ->
          let p, r = Lwt.task () in
          Queue.push (p, r) str.acceptors;
          Some p
      end
  in
  (* The descriptor is being closed: tear its accept stream down NOW. The
     armed multishot holds a kernel reference to the socket (it would keep
     accepting after the close), and the table entry would shadow a later
     descriptor reusing the same number. *)
  let on_close fd =
    match Hashtbl.find_opt accept_streams fd with
    | None -> ()
    | Some str ->
      Hashtbl.remove accept_streams fd;
      (match !the_ring, str.armed with
      | Some ring, Some job ->
        str.armed <- None;
        cancel ring job
      | _ -> str.armed <- None);
      Queue.iter (fun afd -> try Unix.close afd with _ -> ()) str.accepted;
      Queue.clear str.accepted;
      reject_acceptors str (Unix.Unix_error (Unix.EBADF, "accept", ""))
  in
  {
    Lwt_unix.read;
    write;
    read_bigarray;
    write_bigarray;
    connect;
    accept;
    on_close;
  }

let () = Lwt_unix.set_completion_io (Some completion_backend)

let available () =
  match U.create ~queue_depth:1 () with
  | ring -> U.exit ring; true
  | exception _ -> false

let set ?queue_depth () =
  Lwt_engine.set (new uring ?queue_depth ())
