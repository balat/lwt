(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The commonest program there is: write to Lwt_io.stdout and exit without
   flushing by hand. The flush is an exit hook, and on a blocking descriptor it
   needs a job, so the notification channel of the MAIN domain has to be alive
   while the exit hooks run.

   It was not, for a while, and the process hung at exit: Lwt's exit-hook drain is
   a Stdlib.at_exit on the main domain, deliberately, so that it runs after a
   user's own handlers, while everything registered with Domain.at_exit fires
   BEFORE any Stdlib.at_exit. Retiring the channel there was therefore retiring it
   too early.

   This test is a program, not an assertion: if it exits, it passed. It is run as a
   test so that the case cannot regress unnoticed, because nothing else in the
   suite exercises the main domain's exit path with a job in it. *)

let () =
  (* Written and NOT flushed: the flush must happen in the exit hook. *)
  Lwt_main.run (Lwt_io.write Lwt_io.stdout "");
  (* And an exit hook of our own that needs a job, so the case is explicit rather
     than incidental to Lwt_io's own hook. *)
  Lwt_main.at_exit (fun () ->
    Lwt.bind (Lwt_unix.openfile "/dev/null" [ Unix.O_WRONLY ] 0) (fun fd ->
      Lwt_unix.close fd));
  print_endline "main domain exit hooks with jobs: ok"
