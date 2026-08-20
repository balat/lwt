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

  if !failures > 0 then exit 1;
  print_endline "per-domain notification channels: ok"
