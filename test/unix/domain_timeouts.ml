(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Two per-domain stories in one test, since both are step 6 of the S2 log.

   [Lwt_timeout] is a timer wheel driven by an engine timer, so each domain runs
   its own: a timeout fires on the loop of the domain that created it, and taking
   one across a domain boundary is refused rather than spliced into the wrong
   wheel.

   [Lwt_preemptive] turned out to be the same case, for a reason that has nothing
   to do with promises: a domain does not terminate while any of its threads is
   still running, so a shared pool would pin a spawned domain alive after its work
   was done. Each loop therefore has its own pool, and its workers end with it.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

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

  (* Detaching works from any domain now: the result comes back through the
     detaching loop's own notification channel. *)
  check "another domain detaches blocking work"
    (Domain.join
       (Domain.spawn (fun () ->
          match Lwt_main.run (Lwt_preemptive.detach (fun x -> x * 2) 21) with
          | 42 -> true
          | _ -> false
          | exception _ -> false)));
  check "and so do we"
    (match Lwt_main.run (Lwt_preemptive.detach String.length "hello") with
     | 5 -> true
     | _ -> false
     | exception _ -> false);

  (* Several loops detaching at once, each with more work than the bounds allow,
     so the per-loop queue of waiting clients is exercised too. *)
  check "two domains detach at once, past their bounds"
    (let work () =
       Domain.spawn (fun () ->
         match
           Lwt_main.run
             (Lwt_list.map_p (Lwt_preemptive.detach String.length)
                [ "a"; "bb"; "ccc"; "dddd"; "eeeee"; "ffffff" ])
         with
         | l -> l = [ 1; 2; 3; 4; 5; 6 ]
         | exception _ -> false)
     in
     let a = work () and b = work () in
     Domain.join a && Domain.join b);

  (* An exit hook that detaches, which lands AFTER the pool has been shut down.
     It must still work: the worker it creates leaves after its one task and is
     joined by detach itself, so nothing hangs and nothing is left running. *)
  check "an exit hook may still detach"
    (let got = Atomic.make 0 in
     Domain.join
       (Domain.spawn (fun () ->
          Lwt_main.at_exit (fun () ->
            Lwt.bind (Lwt_preemptive.detach (fun x -> x * 3) 14) (fun v ->
              Atomic.set got v;
              Lwt.return_unit));
          ignore (Lwt_main.run (Lwt_preemptive.detach (fun x -> x) 1))));
     Atomic.get got = 42);

  if !failures > 0 then exit 1;
  print_endline "per-domain timeouts and thread pools: ok"
