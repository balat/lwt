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

let dispatch ring data =
  match data with
  | Cancel -> ()
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

class uring ?(queue_depth = 256) () = object
  inherit Lwt_engine.abstract

  val ring : req U.t = U.create ~queue_depth ()

  method id = Engine_id__uring

  method private cleanup = U.exit ring

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
      | U.Some { result = _; data } -> dispatch ring data
      | U.None -> ()
    end;
    let rec drain () =
      match U.get_cqe_nonblocking ring with
      | U.Some { result = _; data } -> dispatch ring data; drain ()
      | U.None -> ()
    in
    drain ()
end

let available () =
  match U.create ~queue_depth:1 () with
  | ring -> U.exit ring; true
  | exception _ -> false

let set ?queue_depth () =
  Lwt_engine.set (new uring ?queue_depth ())
