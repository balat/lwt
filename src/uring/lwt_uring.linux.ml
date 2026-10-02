(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* A readiness-based Lwt engine on top of Linux io_uring.

   The default Lwt engines (libev, select) register file descriptors with the
   kernel and, on each loop iteration, ask "which are ready now?". This engine
   does the same but through io_uring: a descriptor wait becomes a one-shot
   [IORING_OP_POLL_ADD] submission, a timer becomes an [IORING_OP_TIMEOUT]
   submission, and one [io_uring_enter] (via {!Uring.wait}) both flushes all
   pending submissions and reaps the completions. Registrations therefore cost
   no immediate system call: they are batched until the next loop iteration.

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
  desc : desc;
      (* The bookkeeping of its descriptor in the engine that owns it. *)
  make : req U.t -> req -> req U.job option;
      (* Prepares the submission, when the loop submits the operation. *)
  recovered : Cstruct.t;
      (* For a read, the buffer the kernel fills, which still holds the bytes
         of a read cancelled too late (see [Abandoned]); empty for the other
         operations. *)
  complete : int -> unit;
      (* Resolves the operation's promise from the syscall result (negative for
         an errno, and for any negative result once [stopped] is set). *)
  mutable phase : phase;
  mutable job : req U.job option;
      (* The submission, while it is in the kernel, for cancellation. *)
  mutable stopped : exn option;
      (* Set when the operation is cancelled because its engine is destroyed,
         or its descriptor closed or aborted: unless the operation completed
         first, its promise is then rejected with this exception. *)
}

(* An operation is not prepared when it is requested but when the loop next
   submits: one cancelled in between, typically in the same lap, never reaches
   the kernel, so that its cancellation means what it means with the default
   engines, where the system call happens in the loop too. *)
and phase =
  | Queued  (* Requested, in the [pending] array of the engine. *)
  | Held  (* A read waiting, in [held], for a cancelled read to complete. *)
  | Submitted  (* In the kernel. *)
  | Abandoned
      (* Cancelled by the user while in the kernel: its promise is rejected,
         and its completion is still to come. *)
  | Done

(* The operations of one descriptor in one engine. A descriptor has one or two
   operations at a time, a read and a write, so the list is short. A read
   cancelled while in the kernel may still complete, and its bytes are then
   kept for the next read (see [unread]); until it has completed, the next
   reads wait in [held], so that they never take bytes that come after its own,
   nor fill a buffer the kernel may still write. *)
and desc = {
  fd : Unix.file_descr;
  mutable ops : io_req list;
  mutable draining : int;
      (* Reads in the [Abandoned] phase. *)
  held : io_req Queue.t;
  mutable closed : bool;
      (* Set when the descriptor is closed or aborted: what its abandoned reads
         bring is then dropped. *)
}

(* An engine's ring, with the completion-based operations queued or in flight
   on it, by descriptor. Polls and timers are [Lwt_engine] events, which
   [Lwt_engine] stops, and so cancels, when it destroys the engine or when
   [Lwt_unix] closes their descriptor; completion-based operations are not, so
   the engine keeps them here to cancel them itself (see [cleanup] and
   [stop_descriptor]). An entry outlives the operations of a descriptor, which
   saves reallocating it for the next one; it is replaced when the descriptor
   is closed, so that a new descriptor with the same number starts afresh. *)
type state = {
  ring : req U.t;
  descs : (Unix.file_descr, desc) Hashtbl.t;
  mutable pending : io_req array;
  mutable n_pending : int;
      (* The operations to submit at the next lap: an array rather than a queue,
         so that queueing allocates nothing. *)
}

(* One lookup per operation, which allocates nothing once the entry exists. *)
let desc st fd =
  match Hashtbl.find st.descs fd with
  | d -> d
  | exception Not_found ->
    let d =
      { fd; ops = []; draining = 0; held = Queue.create (); closed = false }
    in
    Hashtbl.add st.descs fd d;
    d

let untrack (r : io_req) =
  let d = r.desc in
  match d.ops with
  | [ o ] when o == r -> d.ops <- []
  | o :: rest when o == r -> d.ops <- rest
  | l -> d.ops <- List.filter (fun o -> o != r) l

let is_read (r : io_req) = Cstruct.length r.recovered > 0

(* Fills the free slots of [pending]. *)
let no_io : io_req =
  { desc =
      { fd = Unix.stdin; ops = []; draining = 0; held = Queue.create ();
        closed = true };
    make = (fun _ _ -> None);
    recovered = Cstruct.empty;
    complete = ignore;
    phase = Done;
    job = None;
    stopped = None }

let queue st r =
  if st.n_pending = Array.length st.pending then begin
    let bigger = Array.make (max 16 (2 * st.n_pending)) no_io in
    Array.blit st.pending 0 bigger 0 st.n_pending;
    st.pending <- bigger
  end;
  st.pending.(st.n_pending) <- r;
  st.n_pending <- st.n_pending + 1

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
   which is ignored. A cancel is a submission like any other, so it goes through
   [submit]: dropping it when the queue is full would leave the operation in
   the kernel, holding its file open. *)
let cancel ring job =
  match submit ring Cancel (fun ring data -> U.cancel ring job data) with
  | (_ : req U.job) -> ()
  | exception Invalid_argument _ -> ()
  (* The job was already collected: nothing to cancel. *)

(* The bytes that reads cancelled too late took from their descriptor: the
   kernel performed them after their promise had been rejected with
   [Lwt.Canceled]. The next read of the descriptor gets them first, so that
   cancelling a read loses no data. They are kept outside any engine, so that
   they survive its replacement, and dropped when the descriptor is closed. *)
type unread = { mutable bytes : string; mutable off : int }

let unread : (Unix.file_descr, unread) Hashtbl.t = Hashtbl.create 8

let keep_unread fd s =
  match Hashtbl.find unread fd with
  | u ->
    u.bytes <- String.sub u.bytes u.off (String.length u.bytes - u.off) ^ s;
    u.off <- 0
  | exception Not_found -> Hashtbl.add unread fd { bytes = s; off = 0 }

(* Hand at most [len] of the bytes kept for [fd] to [blit src src_off n], and
   return [n], which is 0 if nothing is kept. Cheap when nothing is kept for any
   descriptor, which is the case but right after a late cancellation. *)
let take_unread fd len blit =
  if Hashtbl.length unread = 0 then 0
  else
    match Hashtbl.find unread fd with
    | exception Not_found -> 0
    | u ->
      let n = min len (String.length u.bytes - u.off) in
      blit u.bytes u.off n;
      u.off <- u.off + n;
      if u.off = String.length u.bytes then Hashtbl.remove unread fd;
      n

(* Start [r]: serve it from the bytes kept for its descriptor, or hold it while
   a cancelled read is still in the kernel, or queue it for submission. *)
let start st (r : io_req) =
  let served =
    if is_read r && Hashtbl.length unread > 0 then
      take_unread r.desc.fd (Cstruct.length r.recovered) (fun s off n ->
        Cstruct.blit_from_string s off r.recovered 0 n)
    else 0
  in
  if served > 0 then begin
    r.phase <- Done;
    untrack r;
    r.complete served
  end
  else if is_read r && r.desc.draining > 0 then begin
    r.phase <- Held;
    Queue.add r r.desc.held
  end
  else begin
    r.phase <- Queued;
    queue st r
  end

let rec release st d =
  if d.draining = 0 && not (Queue.is_empty d.held) then begin
    let r = Queue.pop d.held in
    if r.phase = Held then start st r;
    release st d
  end

(* Prepare the operations requested since the last lap. *)
let submit_pending st =
  for i = 0 to st.n_pending - 1 do
    let r = st.pending.(i) in
    st.pending.(i) <- no_io;
    if r.phase = Queued then begin
      r.job <- Some (submit st.ring (Io r) r.make);
      r.phase <- Submitted
    end
  done;
  st.n_pending <- 0

(* The completion of [r]. An abandoned read keeps what it read for the next
   read, unless its descriptor has been closed, and lets the reads it held go.
   [resolve] settles the promise of an operation that was not abandoned. *)
let finish st (r : io_req) result resolve =
  r.job <- None;
  untrack r;
  match r.phase with
  | Abandoned ->
    r.phase <- Done;
    if is_read r then begin
      let d = r.desc in
      if result > 0 && not d.closed then
        keep_unread r.desc.fd (Cstruct.to_string (Cstruct.sub r.recovered 0 result));
      d.draining <- d.draining - 1;
      release st d
    end
  | Queued | Held | Submitted | Done ->
    r.phase <- Done;
    resolve r result

let resolve_now (r : io_req) result = r.complete result

(* Resolve on a later lap: during a teardown (see [cleanup]), and during a
   close, which runs inside [Lwt_unix.close]; [Lwt_io] closes a channel through
   a lazy value, which a callback run there would force again. *)
let resolve_later (r : io_req) result =
  Lwt.on_success (Lwt.pause ()) (fun () -> r.complete result)

(* Settle [r], which is not in the kernel, with [e], on a later lap. *)
let reject (r : io_req) e =
  r.phase <- Done;
  untrack r;
  r.stopped <- Some e;
  resolve_later r (-1)

let dispatch st result data =
  match data with
  | Cancel -> ()
  | Io r -> finish st r result resolve_now
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

(* [io_uring_setup] fails with ENOSYS where the kernel has no io_uring, and with
   EPERM where it is forbidden: by the [kernel.io_uring_disabled] sysctl, or by
   a seccomp filter such as the default profile of Docker and containerd. *)
let create_ring ~queue_depth =
  match U.create ~queue_depth () with
  | ring -> ring
  | exception Unix.Unix_error ((Unix.ENOSYS | Unix.EPERM), _, _) ->
    raise (Lwt_sys.Not_available "io_uring")

class uring ?(queue_depth = 256) () = object
  inherit Lwt_engine.abstract

  val st =
    { ring = create_ring ~queue_depth;
      descs = Hashtbl.create 64;
      pending = [||];
      n_pending = 0 }

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
     completion-based operations in the kernel are cancelled too, those still
     queued are never submitted, and everything is reaped until the ring is
     idle. This terminates: nothing is submitted any more
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
      Hashtbl.iter
        (fun _ d ->
          List.iter
            (fun (r : io_req) ->
              match r.phase with
              | Queued | Held ->
                r.phase <- Done;
                r.stopped <- Some Lwt.Canceled;
                resolve_later r (-1)
              | Submitted ->
                r.stopped <- Some Lwt.Canceled;
                Option.iter (cancel st.ring) r.job
              | Abandoned | Done -> ())
            d.ops)
        st.descs;
      Array.fill st.pending 0 st.n_pending no_io;
      st.n_pending <- 0;
      while U.active_ops st.ring > 0 do
        match U.wait st.ring with
        | U.Some { result; data = Io r } ->
          finish st r (result :> int) resolve_later
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

  (* The timespec holds int64 nanoseconds, about 292 years: a longer delay,
     [infinity] or [nan] would overflow the conversion and expire at once. It
     never expires instead, as with the other engines: nothing is submitted. A
     negative delay expires at once. *)
  method private register_timer delay repeat f =
    let ns = Int64.of_float (Float.max 0. delay *. 1e9) in
    let tr = { ns; repeat; t_callback = f; t_active = true; t_job = None } in
    if delay < 9e9 then submit_timer st.ring tr;
    lazy (
      tr.t_active <- false;
      if live then
        match tr.t_job with Some job -> cancel st.ring job | None -> ())

  method iter block =
    submit_pending st;
    ignore (U.submit st.ring);
    (* When [block] is requested and the ring has outstanding operations, wait
       for at least one completion; otherwise just harvest what is ready. With
       nothing outstanding there is nothing to wait for, so we never block. *)
    if block && U.active_ops st.ring > 0 then begin
      match U.wait st.ring with
      | U.Some { result; data } -> dispatch st (result :> int) data
      | U.None -> ()
    end;
    (* Bounded to one ring's worth of completions per lap. A poll that fires is
       re-armed at once while its event is still active, and Lwt_unix stops it
       only after a pause, that is after this lap; with more descriptors ready
       than the submission queue holds, re-arming flushed the queue, the polls
       completed at once on descriptors still ready, and an unbounded loop never
       ended. What is left goes to the next lap, which is what a level-triggered
       engine does anyway. *)
    let rec drain budget =
      if budget > 0 && not released then
        match U.get_cqe_nonblocking st.ring with
        | U.Some { result; data } ->
          dispatch st (result :> int) data;
          drain (budget - 1)
        | U.None -> ()
    in
    drain queue_depth

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
   [read(2)]/[write(2)], used for seekable files. *)
let current_offset = Optint.Int63.minus_one

(* Request a completion-based operation, built by [make], on the ring of [st],
   and return a promise resolved with [post result], or rejected with the
   corresponding [Unix.Unix_error]. [post] runs in the completion handler (e.g.
   the bounce-buffer blit of the bytes read path) so no extra promise is
   allocated on the per-operation hot path. [recovered] is the buffer a read
   fills, and is empty for the other operations (see [io_req]).

   Cancelling the promise before the loop has submitted the operation drops it.
   Once it is in the kernel, cancelling rejects the promise at once, as Lwt
   does, and cancels it in the kernel; a read the kernel performs anyway keeps
   its bytes for the next read (see [finish]). *)
let submit_io st fd ~recovered op_name post make =
  let waiter, wakener = Lwt.task () in
  let d = desc st fd in
  let rec r =
    { desc = d;
      make;
      recovered;
      complete =
        (fun result ->
           if result >= 0 then Lwt.wakeup wakener (post result)
           else
             match r.stopped with
             | Some e -> Lwt.wakeup_exn wakener e
             | None ->
               Lwt.wakeup_exn wakener
                 (Unix.Unix_error (U.error_of_errno result, op_name, "")));
      phase = Queued;
      job = None;
      stopped = None }
  in
  d.ops <- r :: d.ops;
  start st r;
  Lwt.on_cancel waiter (fun () ->
    match r.phase with
    | Queued | Held ->
      r.phase <- Done;
      untrack r
    | Submitted ->
      r.phase <- Abandoned;
      if is_read r then r.desc.draining <- r.desc.draining + 1;
      Option.iter (cancel st.ring) r.job
    | Abandoned | Done -> ());
  waiter

(* Fail the operations of [fd] with [exn], unless they complete first, and drop
   the bytes kept for it. [Lwt_unix] calls this before close(2): the operations
   still queued are never submitted, since once the file is closed the number
   may name another one, and the cancels go to the kernel at once, so that it
   lets go of the file. *)
let stop_descriptor fd exn =
  Hashtbl.remove unread fd;
  match !installed with
  | None -> ()
  | Some st ->
    (match Hashtbl.find st.descs fd with
     | exception Not_found -> ()
     | d ->
       Hashtbl.remove st.descs fd;
       d.closed <- true;
       List.iter
         (fun (r : io_req) ->
           match r.phase with
           | Queued | Held -> reject r exn
           | Submitted ->
             r.stopped <- Some exn;
             Option.iter (cancel st.ring) r.job
           | Abandoned | Done -> ())
         d.ops);
    ignore (U.submit st.ring)

(* What the default path raises for an operation on a closed descriptor. *)
let closed = Unix.Unix_error (Unix.EBADF, "check_descriptor", "")

(* Result adapters for [submit_io]'s [post], defined once so that the common
   cases allocate no per-operation closure. *)
let int_result : int -> int = fun n -> n
let unit_result : int -> unit = fun _ -> ()

(* Pick the io_uring operation to read into / write from a [Cstruct.t], by
   descriptor kind:
   - sockets use [recv]/[send] (offsetless: the correct op, and reading/writing
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

  (* The same checks as [Lwt_unix.read] and friends, made before submitting:
     the bytes read path blits in the completion handler, where an index out of
     bounds would raise inside [iter] and leave the promise pending. *)
  let check name length pos len =
    if pos < 0 || len < 0 || pos > length - len then
      invalid_arg ("Lwt_uring.Io." ^ name)

  (* Positioned read/write at the descriptor's current offset, suited to regular
     files and general use. For sockets, prefer the transparent {!Lwt_unix} path
     (which uses [recv]/[send]). *)
  let read fd buf pos len =
    check "read" (Bytes.length buf) pos len;
    let cs = Cstruct.create_unsafe len in
    submit_io (get_state ()) fd ~recovered:cs "read"
      (fun n -> Cstruct.blit_to_bytes cs 0 buf pos n; n)
      (fun ring data -> U.read ring ~file_offset:current_offset fd cs data)

  let write fd buf pos len =
    check "write" (Bytes.length buf) pos len;
    let cs = Cstruct.create_unsafe len in
    Cstruct.blit_from_bytes buf pos cs 0 len;
    submit_io (get_state ()) fd ~recovered:Cstruct.empty "write" int_result (fun ring data ->
      U.write ring ~file_offset:current_offset fd cs data)

  let read_bigarray fd buf pos len =
    check "read_bigarray" (Bigarray.Array1.dim buf) pos len;
    let cs = Cstruct.of_bigarray ~off:pos ~len buf in
    submit_io (get_state ()) fd ~recovered:cs "read" int_result (fun ring data ->
      U.read ring ~file_offset:current_offset fd cs data)

  let write_bigarray fd buf pos len =
    check "write_bigarray" (Bigarray.Array1.dim buf) pos len;
    let cs = Cstruct.of_bigarray ~off:pos ~len buf in
    submit_io (get_state ()) fd ~recovered:Cstruct.empty "write" int_result (fun ring data ->
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
   copy-free and faster: see the benchmark matrix). Mitigations, if ever needed:
   a reusable per-fd bounce buffer or registered fixed buffers for the bytes
   path. The bounce buffers use [Cstruct.create_unsafe] (no zero-fill: writes
   fully overwrite [0,len) and reads only expose the [n] bytes the kernel
   wrote), and the result post-processing runs inside the completion handler
   ([submit_io]'s [post]), so a read costs one promise, not two. *)
let completion_backend : Lwt_unix.completion_io =
  (* Without an engine, a read still gets the bytes a cancelled read kept, if
     the engine that kept them has been replaced. *)
  let read ch buf pos len =
    match !installed with
    | None when Hashtbl.length unread = 0 -> None
    | None ->
      (match
         take_unread (Lwt_unix.unix_file_descr ch) len (fun s off n ->
           Bytes.blit_string s off buf pos n)
       with
       | 0 -> None
       | n -> Some (Lwt.return n))
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.create_unsafe len in
      Some
        (submit_io st fd ~recovered:cs "read"
           (fun n -> Cstruct.blit_to_bytes cs 0 buf pos n; n)
           (read_op kind fd cs))
  in
  let write ch buf pos len =
    match !installed with
    | None -> None
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.create_unsafe len in
      Cstruct.blit_from_bytes buf pos cs 0 len;
      Some (submit_io st fd ~recovered:Cstruct.empty "write" int_result (write_op kind fd cs))
  in
  let read_bigarray ch buf pos len =
    match !installed with
    | None when Hashtbl.length unread = 0 -> None
    | None ->
      (match
         take_unread (Lwt_unix.unix_file_descr ch) len (fun s off n ->
           Cstruct.blit_from_string s off (Cstruct.of_bigarray ~off:pos ~len buf)
             0 n)
       with
       | 0 -> None
       | n -> Some (Lwt.return n))
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.of_bigarray ~off:pos ~len buf in
      Some (submit_io st fd ~recovered:cs "read" int_result (read_op kind fd cs))
  in
  let write_bigarray ch buf pos len =
    match !installed with
    | None -> None
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.of_bigarray ~off:pos ~len buf in
      Some (submit_io st fd ~recovered:Cstruct.empty "write" int_result (write_op kind fd cs))
  in
  (* Completion-based [connect]: submit IORING_OP_CONNECT and resolve when the
     connection completes (result 0) or fails (negative errno, mapped by
     [submit_io]). The descriptor is the user's own socket: no fd is created,
     so no flag policy is involved. *)
  let connect ch addr =
    match !installed with
    | None -> None
    | Some st ->
      let fd = Lwt_unix.unix_file_descr ch in
      Some
        (submit_io st fd ~recovered:Cstruct.empty "connect" unit_result (fun ring data ->
           U.connect ring fd addr data))
  in
  (* [accept] is deliberately left on Lwt's default path: under the io_uring
     engine its readiness already runs on the ring (poll), and routing single-shot
     IORING_OP_ACCEPT measured slower (the op forces SOCK_CLOEXEC, needing a
     compensating fcntl, and sequential accepts do not batch).
     Possible improvement: a multishot accept (IORING_OP_ACCEPT_MULTI, not in the
     [uring] API surface used here) would batch and might flip that verdict. *)
  let on_close fd = stop_descriptor fd closed in
  let on_abort fd e = stop_descriptor fd e in
  { Lwt_unix.read; write; read_bigarray; write_bigarray; connect; on_close;
    on_abort }

let () = Lwt_unix.set_completion_io (Some completion_backend)

let available () =
  match create_ring ~queue_depth:1 with
  | ring -> U.exit ring; true
  | exception (Lwt_sys.Not_available _ | Unix.Unix_error _) -> false

let set ?queue_depth () =
  Lwt_engine.set (new uring ?queue_depth ())

(* [LWT_URING=0] turns io_uring off without recompiling, for instance to
   compare engines or to work around a kernel problem in production. *)
let disabled_by_environment () =
  match Sys.getenv_opt "LWT_URING" with
  | Some "0" -> true
  | Some _ | None -> false

let set_if_available ?queue_depth () =
  (not (disabled_by_environment ()))
  && (match set ?queue_depth () with
      | () -> true
      | exception Lwt_sys.Not_available _ -> false)
