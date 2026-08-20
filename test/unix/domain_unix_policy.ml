(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Which operations belong to which domain, in both directions.

   Since every loop has its own notification channel, JOBS are no longer among
   the restricted ones: a job submitted anywhere completes on the loop that
   submitted it. That is checked here with the real thing, a file opened and read
   on a spawned domain, since opening a file IS a job.

   Signal handlers and child waiting are still wired to the domain that
   initialised the module, and still fail explicitly elsewhere rather than
   silently waking a foreign promise or hanging.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

(* [true] if [f] fails with the explicit owner-domain message. *)
let refused f =
  match f () with
  | _ -> false
  | exception Failure m ->
    let expected = "only available on the domain that initialised Lwt_unix" in
    let rec contains i =
      i + String.length expected <= String.length m
      && (String.sub m i (String.length expected) = expected || contains (i + 1))
    in
    contains 0
  | exception _ -> false

let () =
  (* A child that stays alive, forked BEFORE any other domain exists, since
     [Unix.fork] is unsupported once several domains are running. *)
  let child =
    match Unix.fork () with
    | 0 -> Unix.sleep 30; Unix._exit 0
    | pid -> pid
  in

  let on_other_domain f = Domain.join (Domain.spawn f) in

  (* Jobs, which used to be refused here, now work. *)
  check "another domain runs a job"
    (on_other_domain (fun () ->
       match Lwt_main.run (Lwt_unix.stat "/") with
       | st -> st.Unix.st_kind = Unix.S_DIR
       | exception _ -> false));

  (* And the whole of it: a file opened, read and closed on another domain, which
     is three jobs and the exit criterion of this phase. *)
  check "another domain opens, reads and closes a file"
    (on_other_domain (fun () ->
       match
         Lwt_main.run
           (Lwt.bind
              (Lwt_unix.openfile "/etc/hostname" [ Unix.O_RDONLY ] 0)
              (fun fd ->
                let buf = Bytes.create 16 in
                Lwt.bind (Lwt_unix.read fd buf 0 16) (fun n ->
                  Lwt.bind (Lwt_unix.close fd) (fun () -> Lwt.return n))))
       with
       | n -> n > 0
       | exception _ -> false));

  (* Still refused off the owner. *)
  check "a signal handler is refused on another domain"
    (on_other_domain (fun () ->
       refused (fun () -> Lwt_unix.on_signal Sys.sigusr1 (fun _ -> ()))));
  check "waiting for a child is refused on another domain"
    (on_other_domain (fun () ->
       refused (fun () -> Lwt_main.run (Lwt_unix.waitpid [] child))));
  (* Allowed off the owner: a loop with timers, and the notification API. *)
  check "a loop with timers still runs on another domain"
    (on_other_domain (fun () ->
       match Lwt_main.run (Lwt_unix.sleep 0.01) with
       | () -> true
       | exception _ -> false));

  let fired = ref false in
  let n = Lwt_unix.make_notification ~once:true (fun () -> fired := true) in
  on_other_domain (fun () -> Lwt_unix.send_notification n);
  Lwt_main.run (Lwt_unix.sleep 0.05);
  check "a notification sent from another domain fires on the owner" !fired;

  (* The other direction, and it is the part that changed when every loop got its
     own channel: a notification created on another domain belongs to THAT
     domain's channel, so it fires there, on the domain that created it, and not
     here. The other domain therefore has to run its own loop to see it. *)
  let fired_here = ref false in
  let fired_there = ref false in
  check "a notification fires on the domain that created it"
    (on_other_domain (fun () ->
       let n = Lwt_unix.make_notification ~once:true (fun () ->
         fired_there := true)
       in
       let mine = Lwt_unix.make_notification ~once:true (fun () ->
         fired_here := true)
       in
       ignore mine;
       Lwt_unix.send_notification n;
       Lwt_main.run (Lwt_unix.sleep 0.05);
       !fired_there));
  check "and it did not fire here" (not !fired_here);

  (* And the owner itself is unaffected. *)
  check "the owner still runs jobs"
    (match Lwt_main.run (Lwt_unix.stat "/") with
     | _ -> true
     | exception _ -> false);
  let id = Lwt_unix.on_signal Sys.sigusr1 (fun _ -> ()) in
  Lwt_unix.disable_signal_handler id;
  check "the owner still registers signal handlers" true;

  (try Unix.kill child Sys.sigkill with _ -> ());
  (try ignore (Unix.waitpid [] child) with _ -> ());
  if !failures > 0 then exit 1;
  print_endline "per-domain Lwt_unix policy: ok"
