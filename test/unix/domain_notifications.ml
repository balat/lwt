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

  if !failures > 0 then exit 1;
  print_endline "per-domain notification channels: ok"
