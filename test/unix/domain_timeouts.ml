(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Two per-domain stories in one test, since both are step 6 of the S2 log.

   [Lwt_timeout] is a timer wheel driven by an engine timer, so each domain runs
   its own: a timeout fires on the loop of the domain that created it, and taking
   one across a domain boundary is refused rather than spliced into the wrong
   wheel.

   [Lwt_preemptive] is the opposite case. Its pool of system threads is a process
   resource and stays shared, but a detached result comes back through the
   notification pipe, so detaching belongs to the domain that owns the pipe.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let refused_with_failure f =
  match f () with _ -> false | exception Failure _ -> true | exception _ -> false

let refused_with_invalid_arg f =
  match f () with
  | _ -> false
  | exception Invalid_argument _ -> true
  | exception _ -> false

let () =
  (* Each domain's wheel runs on its own loop. The wheel ticks once a second and
     a timeout with the minimum delay of one lands one bucket ahead of the
     current one, so it fires on the SECOND tick; hence the waits below. *)
  let fired_here = ref false in
  let t = Lwt_timeout.create 1 (fun () -> fired_here := true) in
  Lwt_timeout.start t;
  Lwt_main.run (Lwt_unix.sleep 2.5);
  check "our own timeout fires on our loop" !fired_here;

  (* The wheels are independent, and this is where a shared one shows: we start a
     timeout and never run a loop, which leaves OUR wheel with a pending count
     and a loop that will never tick. A domain arriving next must still get its
     own ticking loop, which it only does if the wheel it looks at is its own. *)
  let fired_orphan = ref false in
  let orphan = Lwt_timeout.create 1 (fun () -> fired_orphan := true) in
  Lwt_timeout.start orphan;

  let fired_there = ref false in
  Domain.join
    (Domain.spawn (fun () ->
       let t = Lwt_timeout.create 1 (fun () -> fired_there := true) in
       Lwt_timeout.start t;
       Lwt_main.run (Lwt_unix.sleep 2.5)));
  check "another domain's timeout fires on its own loop" !fired_there;
  check "and ours did not fire, our loop never having run" (not !fired_orphan);

  (* A timeout does not cross a domain boundary. *)
  let ours = Lwt_timeout.create 1 (fun () -> ()) in
  check "starting our timeout on another domain is refused"
    (Domain.join
       (Domain.spawn (fun () ->
          refused_with_invalid_arg (fun () -> Lwt_timeout.start ours))));
  check "stopping our timeout on another domain is refused"
    (Domain.join
       (Domain.spawn (fun () ->
          refused_with_invalid_arg (fun () -> Lwt_timeout.stop ours))));
  check "changing our timeout on another domain is refused"
    (Domain.join
       (Domain.spawn (fun () ->
          refused_with_invalid_arg (fun () -> Lwt_timeout.change ours 2))));

  (* Detaching belongs to the domain that owns the notification pipe. *)
  check "detaching from another domain is refused"
    (Domain.join
       (Domain.spawn (fun () ->
          refused_with_failure (fun () ->
            Lwt_main.run (Lwt_preemptive.detach (fun () -> 1) ())))));
  check "the owner still detaches"
    (match Lwt_main.run (Lwt_preemptive.detach (fun x -> x * 2) 21) with
     | 42 -> true
     | _ -> false
     | exception _ -> false);

  if !failures > 0 then exit 1;
  print_endline "per-domain timeouts, shared thread pool: ok"
