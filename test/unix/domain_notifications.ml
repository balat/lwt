(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Every loop now has its own notification channel, and a notification id carries
   the channel it belongs to. What that buys, and what this checks: a
   notification is delivered to the domain that CREATED it, and to no other, so
   two loops can be woken independently.

   Before this, one pipe served the whole process and whichever domain read it
   ran every handler, including handlers belonging to other domains.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

(* Atomic, because the two domains both look at these. *)
let ours = Atomic.make 0
let theirs = Atomic.make 0

let () =
  let n_ours = Lwt_unix.make_notification (fun () -> Atomic.incr ours) in

  (* The other domain makes its own, then sends BOTH and drains its own channel.
     It must see its own notification and must not run ours. *)
  let seen_there =
    Domain.join
      (Domain.spawn (fun () ->
         let n_theirs =
           Lwt_unix.make_notification (fun () -> Atomic.incr theirs)
         in
         Lwt_unix.send_notification n_theirs;
         Lwt_unix.send_notification n_ours;
         Lwt_main.run (Lwt_unix.sleep 0.05);
         Atomic.get theirs))
  in
  check "the other domain ran its own notification" (seen_there = 1);
  check "the other domain did not run ours" (Atomic.get ours = 0);

  (* Ours is still waiting for us, and one lap of our own loop delivers it. *)
  Lwt_main.run (Lwt_unix.sleep 0.05);
  check "our notification is delivered on our own loop" (Atomic.get ours = 1);
  check "and theirs was not run again here" (Atomic.get theirs = 1);

  (* A channel survives its loop stopping and starting again. *)
  Lwt_unix.send_notification n_ours;
  Lwt_main.run (Lwt_unix.sleep 0.05);
  check "the channel still works after the loop restarts"
    (Atomic.get ours = 2);

  (* Sending to a loop that is exiting. Retiring a channel closes its
     descriptor; a sender that looked the channel up just before must not
     write to the closed descriptor (EBADF out of send_notification, which from
     a job worker is a raise with no runtime), nor to whatever reused the
     number. One domain sends as fast as it can to the current loop's id while
     loops come and go: no error may come out, and no handler may run on the
     wrong domain. *)
  let current : Lwt_unix.notification option Atomic.t = Atomic.make None in
  let stop = Atomic.make false in
  let errors = Atomic.make 0 in
  let sent = Atomic.make 0 in
  let wrong_domain = Atomic.make 0 in
  let sender =
    Domain.spawn (fun () ->
      while not (Atomic.get stop) do
        match Atomic.get current with
        | None -> Domain.cpu_relax ()
        | Some id ->
          (match Lwt_unix.send_notification id with
           | () -> Atomic.incr sent
           | exception _ -> Atomic.incr errors)
      done)
  in
  for _ = 1 to 300 do
    Domain.join
      (Domain.spawn (fun () ->
         let me = (Domain.self () :> int) in
         let id =
           Lwt_unix.make_notification (fun () ->
             if (Domain.self () :> int) <> me then Atomic.incr wrong_domain)
         in
         Atomic.set current (Some id);
         Lwt_main.run (Lwt.pause ())))
  done;
  Atomic.set stop true;
  Domain.join sender;
  check "a sender racing with the exit of loops never gets an error"
    (Atomic.get errors = 0);
  check "and it did send" (Atomic.get sent > 0);
  check "no handler ran on another domain than its own"
    (Atomic.get wrong_domain = 0);

  (* A departed loop's notifications go with it. Its entries were left in the
     process-wide table, about forty words per domain that ever ran a loop, for
     ever; and a channel slot's generation is eight bits wide, so once the slot
     had been reused 256 times a stale id named the slot's current owner again,
     and that loop ran the dead domain's handler. The handler below must never
     run: not even when the slot has come round. *)
  let ran_on = Atomic.make (-1) in
  let stale =
    Domain.join
      (Domain.spawn (fun () ->
         Lwt_unix.make_notification (fun () ->
           Atomic.set ran_on (Domain.self () :> int))))
  in
  for _ = 1 to 255 do
    Domain.join
      (Domain.spawn (fun () -> ignore (Lwt_unix.make_notification ignore)))
  done;
  let ready = Atomic.make false in
  let victim =
    Domain.spawn (fun () ->
      ignore (Lwt_unix.make_notification ignore);
      Atomic.set ready true;
      Lwt_main.run (Lwt_unix.sleep 0.1))
  in
  while not (Atomic.get ready) do Domain.cpu_relax () done;
  Lwt_unix.send_notification stale;
  Domain.join victim;
  check "a departed loop's handler never runs again, slot reused 256 times"
    (Atomic.get ran_on = -1);

  (* And the table does not grow with the domains that came and went. Measured
     after a warm-up, so that what is allocated once per process is out of the
     way; a leak per domain showed as forty words each, the budget is well
     under that. *)
  let live () = Gc.full_major (); (Gc.quick_stat ()).Gc.live_words in
  let round () =
    Domain.join
      (Domain.spawn (fun () ->
         Lwt_main.run
           (Lwt.bind (Lwt_unix.stat ".") (fun _ ->
              Lwt.bind (Lwt_preemptive.detach (fun () -> ()) ()) (fun () ->
                Lwt_unix.sleep 0.0001)))))
  in
  for _ = 1 to 20 do round () done;
  let w0 = live () in
  let rounds = 200 in
  for _ = 1 to rounds do round () done;
  let w1 = live () in
  check "the table of notifiers does not grow with departed loops"
    (w1 - w0 < rounds * 8);

  if !failures > 0 then exit 1;
  print_endline "per-domain notification channels: ok"
