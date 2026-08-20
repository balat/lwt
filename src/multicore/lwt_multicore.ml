(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* This module is part of the Lwt packages, so it may use the per-domain layer. *)
[@@@alert "-lwt_internal"]

(* MULTI-PRODUCER, SINGLE-CONSUMER, which is exactly the shape: any domain posts,
   the owning loop drains. Saturn's queue is lock-free and verified upstream, and
   it brings [close], which is what makes "posting to a terminated loop is
   refused" a property of the data structure rather than a flag of ours. *)
module Inbox = Saturn.Single_consumer_queue

type loop = {
  inbox : (unit -> unit) Inbox.t;
  (* Wakes this loop. A notification id names the channel of the domain that
     created it, so sending it from anywhere wakes the right loop; that machinery
     is [Lwt_unix]'s and predates this module. *)
  notification : Lwt_unix.notification;
}

exception Loop_terminated

(* Runs on the owning loop, from its notification handler, so the thunks run on
   the right domain with nothing held. [pop_opt] raises [Closed] once the queue is
   both closed and empty, which happens only after the domain has gone; there is
   no handler to run then, but the guard keeps the shape obvious. *)
let drain inbox =
  let rec go () =
    match Inbox.pop_opt inbox with
    | Some f -> f (); go ()
    | None -> ()
    | exception Inbox.Closed -> ()
  in
  go ()

let self_slot : loop Lwt_dls.t =
  Lwt_dls.new_key (fun () ->
    let inbox = Inbox.create () in
    let notification = Lwt_unix.make_notification (fun () -> drain inbox) in
    (* Closing the inbox is what makes a later [run_on] fail instead of dropping
       work silently. The notification goes too, so the id stops naming a live
       handler. *)
    Lwt_dls.at_domain_exit (fun () ->
      Inbox.close inbox;
      Lwt_unix.stop_notification notification);
    { inbox; notification })

let self () = Lwt_dls.get self_slot

let run_on loop f =
  (match Inbox.push loop.inbox f with
   | () -> ()
   | exception Inbox.Closed -> raise Loop_terminated);
  (* Outside the push, and after it: the notification is a system call, and the
     work must be visible before the wake-up that announces it. *)
  Lwt_unix.send_notification loop.notification
