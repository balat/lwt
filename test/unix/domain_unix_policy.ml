(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Jobs, signal handlers and child waiting all go through one process-wide
   notification file descriptor, whose event is registered on the engine of the
   domain that initialised [Lwt_unix]. Completions therefore run on that domain
   whoever submitted the work, so those three families of operation belong to it.

   What this checks is the policy, in both directions: they fail explicitly
   elsewhere rather than silently waking a foreign promise, while what does NOT
   depend on the descriptor keeps working on any domain -- running a loop with
   timers, and creating and sending a notification, which is the documented way
   to wake Lwt from another thread and now from another domain.

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

  (* Refused off the owner. *)
  check "a job is refused on another domain"
    (on_other_domain (fun () -> refused (fun () -> Lwt_unix.stat "/")));
  check "a signal handler is refused on another domain"
    (on_other_domain (fun () ->
       refused (fun () -> Lwt_unix.on_signal Sys.sigusr1 (fun _ -> ()))));
  check "waiting for a child is refused on another domain"
    (on_other_domain (fun () ->
       refused (fun () -> Lwt_main.run (Lwt_unix.waitpid [] child))));
  check "cancelling jobs is refused on another domain"
    (on_other_domain (fun () -> refused Lwt_unix.cancel_jobs));

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

  let fired_there = ref false in
  let n2 =
    on_other_domain (fun () ->
      Lwt_unix.make_notification ~once:true (fun () -> fired_there := true))
  in
  Lwt_unix.send_notification n2;
  Lwt_main.run (Lwt_unix.sleep 0.05);
  check "a notification created on another domain fires on the owner"
    !fired_there;

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
