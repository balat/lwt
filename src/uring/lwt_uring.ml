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
  | Io of io_req
      (* A completion-based I/O submission (read/write/...). *)
  | Cancel
      (* Completion of an [Uring.cancel] submission; nothing to do. *)

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

and io_req = {
  complete : int -> unit;
      (* Resolves the operation's promise from the syscall result (negative for
         an errno). *)
  mutable io_job : req U.job option;
      (* As [job] above, for cancellation. *)
  mutable slot : int;
      (* The operation's index in its engine's [in_flight] table. *)
  mutable abandoned : bool;
      (* Set when the engine cancels the operation because it is destroyed: a
         failure then rejects the promise with [Lwt.Canceled]. *)
}

(* An engine's ring, with the completion-based operations in flight on it. Polls
   and timers are [Lwt_engine] events, which [Lwt_engine] stops, and so cancels,
   when it destroys the engine; completion-based operations are not, so the
   engine keeps them here to cancel them itself (see [cleanup]). An operation
   knows its slot, and removal moves the last entry into the freed one, so both
   tracking and untracking are O(1). *)
type state = {
  ring : req U.t;
  mutable in_flight : io_req array;
  mutable count : int;
}

let no_io = { complete = ignore; io_job = None; slot = -1; abandoned = false }

let track st r =
  if st.count = Array.length st.in_flight then begin
    let bigger = Array.make (max 16 (2 * st.count)) no_io in
    Array.blit st.in_flight 0 bigger 0 st.count;
    st.in_flight <- bigger
  end;
  r.slot <- st.count;
  st.in_flight.(st.count) <- r;
  st.count <- st.count + 1

let untrack st r =
  let last = st.count - 1 in
  let moved = st.in_flight.(last) in
  st.in_flight.(r.slot) <- moved;
  moved.slot <- r.slot;
  st.in_flight.(last) <- no_io;
  st.count <- last

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
  (* The job was already collected: nothing to cancel. *)

let dispatch st result data =
  match data with
  | Cancel -> ()
  | Io r ->
    r.io_job <- None;
    untrack st r;
    r.complete result
  | Poll pr ->
    pr.job <- None;
    if pr.active then begin
      pr.callback ();
      (* The callback may have stopped the event; only re-arm if still active. *)
      if pr.active then submit_poll st.ring pr
    end
  | Timer tr ->
    tr.t_job <- None;
    if tr.t_active then begin
      tr.t_callback ();
      if tr.repeat && tr.t_active then submit_timer st.ring tr
    end

(* The state of the currently-installed io_uring engine, if any. It is used by
   the completion-based I/O of {!Io}, which must submit to the same ring that the
   engine's [iter] reaps. Set when an engine is created, cleared when it is
   destroyed (using physical equality so that replacing one uring engine with
   another keeps the pointer on the live ring). *)
let installed : state option ref = ref None

class uring ?(queue_depth = 256) () = object
  inherit Lwt_engine.abstract

  val st = { ring = U.create ~queue_depth (); in_flight = [||]; count = 0 }

  (* [false] in the child of a [Lwt_unix.fork]: the ring then belongs to the
     parent (see [fork]) and nothing here may touch it any more, neither to
     cancel, submit nor exit. *)
  val mutable live = true

  (* Set once [cleanup] has released the ring. A callback run by [iter] may
     replace this engine, which destroys it; [iter] must then stop reaping. *)
  val mutable released = false

  initializer installed := Some st

  method id = Engine_id__uring

  (* [U.exit] refuses a ring with requests still in flight, and cancelling in
     io_uring is itself a request: by the time [Lwt_engine] calls this, it has
     stopped every event, and each stop has submitted a cancel. So the
     completion-based operations are cancelled too, then everything is reaped
     until the ring is idle. This terminates: nothing is submitted any more
     (polls and timers are inactive, so they do not re-arm), and the kernel
     completes every cancelled request. An operation the cancellation stopped
     rejects its promise with [Lwt.Canceled], as [Lwt_unix.fork] does for the
     jobs it abandons; one that completed first resolves normally. Either way
     the resolution waits for a later loop iteration, so that its callbacks,
     which may start new I/O, run on the engine that replaces this one rather
     than on this one. *)
  method private cleanup =
    (match !installed with Some s when s == st -> installed := None | _ -> ());
    if live then begin
      for i = 0 to st.count - 1 do
        let r = st.in_flight.(i) in
        r.abandoned <- true;
        match r.io_job with Some job -> cancel st.ring job | None -> ()
      done;
      while U.active_ops st.ring > 0 do
        match U.wait st.ring with
        | U.Some { result; data = Io r } ->
          let result = (result :> int) in
          r.io_job <- None;
          untrack st r;
          Lwt.on_success (Lwt.pause ()) (fun () -> r.complete result)
        | U.Some { data = Poll _ | Timer _ | Cancel; _ } | U.None -> ()
      done;
      U.exit st.ring;
      released <- true
    end

  method private register_readable fd f =
    let pr =
      { fd; mask = U.Poll_mask.pollin; callback = f; active = true; job = None }
    in
    submit_poll st.ring pr;
    lazy (
      pr.active <- false;
      if live then
        match pr.job with Some job -> cancel st.ring job | None -> ())

  method private register_writable fd f =
    let pr =
      { fd; mask = U.Poll_mask.pollout; callback = f; active = true; job = None }
    in
    submit_poll st.ring pr;
    lazy (
      pr.active <- false;
      if live then
        match pr.job with Some job -> cancel st.ring job | None -> ())

  method private register_timer delay repeat f =
    let ns = Int64.of_float (delay *. 1e9) in
    let tr = { ns; repeat; t_callback = f; t_active = true; t_job = None } in
    submit_timer st.ring tr;
    lazy (
      tr.t_active <- false;
      if live then
        match tr.t_job with Some job -> cancel st.ring job | None -> ())

  method iter block =
    ignore (U.submit st.ring);
    (* When [block] is requested and the ring has outstanding operations, wait
       for at least one completion; otherwise just harvest what is ready. With
       nothing outstanding there is nothing to wait for, so we never block. *)
    if block && U.active_ops st.ring > 0 then begin
      match U.wait st.ring with
      | U.Some { result; data } -> dispatch st (result :> int) data
      | U.None -> ()
    end;
    let rec drain () =
      if not released then
        match U.get_cqe_nonblocking st.ring with
        | U.Some { result; data } -> dispatch st (result :> int) data; drain ()
        | U.None -> ()
    in
    drain ()

  (* Called by [Lwt_unix.fork] in the child, before anything else. The child
     inherits the ring's memory, which is shared with the parent, and its own
     copy of liburing's bookkeeping: as soon as the parent submits again, that
     copy is stale and every submission from the child sees a full ring (or
     worse, writes into entries the parent owns). So the child abandons the
     inherited ring without touching it, which the [live] flag guarantees, and
     carries on with a fresh engine on a ring of its own: [Lwt_engine.set]
     re-registers every event on it. The parent's in-flight operations are
     lost to the child, like its pending [Lwt_unix] jobs: the child's copies of
     their promises stay pending. The inherited mapping and descriptor are not
     released in the child; they go with it at exit or exec. *)
  method! fork =
    live <- false;
    Lwt_engine.set ~destroy:false (new uring ~queue_depth ())
end

let get_state () =
  match !installed with
  | Some st -> st
  | None ->
    failwith "Lwt_uring.Io: no io_uring engine installed (use Lwt_uring.set)"

(* Offset [-1] tells io_uring to use the descriptor's current position, like
   [read(2)]/[write(2)] — used for seekable files. *)
let current_offset = Optint.Int63.minus_one

(* Submit a completion-based operation built by [make] on the ring of [st] and
   return a promise resolved with the syscall result, or rejected with the
   corresponding [Unix.Unix_error]. Cancelling the promise cancels the in-flight
   submission. *)
let submit_io st op_name make =
  let waiter, wakener = Lwt.task () in
  let rec r =
    { complete =
        (fun result ->
           if result >= 0 then Lwt.wakeup wakener result
           else if r.abandoned then Lwt.wakeup_exn wakener Lwt.Canceled
           else
             Lwt.wakeup_exn wakener
               (Unix.Unix_error (U.error_of_errno result, op_name, "")));
      io_job = None; slot = -1; abandoned = false }
  in
  r.io_job <- Some (submit st.ring (Io r) make);
  track st r;
  Lwt.on_cancel waiter (fun () ->
    match r.io_job with Some job -> cancel st.ring job | None -> ());
  waiter

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
    let cs = Cstruct.create len in
    Lwt.map
      (fun n -> Cstruct.blit_to_bytes cs 0 buf pos n; n)
      (submit_io (get_state ()) "read" (fun ring data ->
         U.read ring ~file_offset:current_offset fd cs data))

  let write fd buf pos len =
    let cs = Cstruct.create len in
    Cstruct.blit_from_bytes buf pos cs 0 len;
    submit_io (get_state ()) "write" (fun ring data ->
      U.write ring ~file_offset:current_offset fd cs data)

  let read_bigarray fd buf pos len =
    let cs = Cstruct.of_bigarray ~off:pos ~len buf in
    submit_io (get_state ()) "read" (fun ring data ->
      U.read ring ~file_offset:current_offset fd cs data)

  let write_bigarray fd buf pos len =
    let cs = Cstruct.of_bigarray ~off:pos ~len buf in
    submit_io (get_state ()) "write" (fun ring data ->
      U.write ring ~file_offset:current_offset fd cs data)
end

(* Transparent routing of Lwt_unix.{read,write,…} through io_uring. The backend
   self-gates on [installed]: it takes over only while a uring engine is
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
   path; [Cstruct.create_unsafe] (drop the zero-fill) helps only marginally. *)
let completion_backend : Lwt_unix.completion_io =
  let read ch buf pos len =
    match !installed with
    | None -> None
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.create len in
      Some
        (Lwt.map
           (fun n -> Cstruct.blit_to_bytes cs 0 buf pos n; n)
           (submit_io st "read" (read_op kind fd cs)))
  in
  let write ch buf pos len =
    match !installed with
    | None -> None
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.create len in
      Cstruct.blit_from_bytes buf pos cs 0 len;
      Some (submit_io st "write" (write_op kind fd cs))
  in
  let read_bigarray ch buf pos len =
    match !installed with
    | None -> None
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.of_bigarray ~off:pos ~len buf in
      Some (submit_io st "read" (read_op kind fd cs))
  in
  let write_bigarray ch buf pos len =
    match !installed with
    | None -> None
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.of_bigarray ~off:pos ~len buf in
      Some (submit_io st "write" (write_op kind fd cs))
  in
  (* Completion-based [connect]: submit IORING_OP_CONNECT and resolve when the
     connection completes (result 0) or fails (negative errno, mapped by
     [submit_io]). The descriptor is the user's own socket — no fd is created, so
     no flag policy is involved. *)
  let connect ch addr =
    match !installed with
    | None -> None
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch in
      Some
        (Lwt.map
           (fun (_ : int) -> ())
           (submit_io st "connect" (fun ring data -> U.connect ring fd addr data)))
  in
  (* [accept] is deliberately left on Lwt's default path: under the io_uring
     engine its readiness already runs on the ring (poll), and routing single-shot
     IORING_OP_ACCEPT measured slower (the op forces SOCK_CLOEXEC, needing a
     compensating fcntl, and sequential accepts do not batch).
     Possible improvement: a multishot accept (IORING_OP_ACCEPT_MULTI, not in the
     [uring] API surface used here) would batch and might flip that verdict. *)
  { Lwt_unix.read; write; read_bigarray; write_bigarray; connect }

let () = Lwt_unix.set_completion_io (Some completion_backend)

let available () =
  match U.create ~queue_depth:1 () with
  | ring -> U.exit ring; true
  | exception _ -> false

let set ?queue_depth () =
  Lwt_engine.set (new uring ?queue_depth ())
