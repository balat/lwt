(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* What S1 step 3 actually delivers: the core's scheduler state is per domain, so
   two domains can each drive their own run queue, pause list and fiber-local
   storage without seeing each other's.

   This uses the bare core through [Lwt.Private.scheduler_run], not [Lwt_main]:
   the core needs no engine to serve pauses, and [Lwt_main.run] is still
   process-global by design until S2. So this test says nothing about N event
   loops; it says the CORE is no longer global, which is precisely what this
   phase is for. *)

[@@@alert "-trespassing"]

let check name b = if not b then (Printf.eprintf "FAILED: %s\n" name; exit 1)

(* A chain of [n] pauses, so the run queue and the pause list are both exercised;
   returns how many laps it took, which is per-domain state too. *)
let rec pauses n = if n = 0 then Lwt.return 0 else Lwt.bind (Lwt.pause ()) (fun () -> pauses (n - 1))

let key : int Lwt.key = Lwt.new_key ()

(* Each domain runs its own scheduler and checks that the storage it set is the
   storage it reads back, across suspensions, while the other domain is doing the
   same with a different value. *)
let work mine iterations () =
  let bad = ref 0 in
  for _ = 1 to iterations do
    let seen =
      Lwt.Private.scheduler_run (fun () ->
        Lwt.with_value key (Some mine) (fun () ->
          Lwt.bind (pauses 20) (fun _ -> Lwt.return (Lwt.get key))))
    in
    if seen <> Some mine then incr bad
  done;
  !bad

let () =
  (* the pause list must be empty on both sides at the start and the end *)
  check "no pause pending at start" (Lwt.paused_count () = 0);
  let iterations = 2_000 in
  let other = Domain.spawn (work 1 iterations) in
  let here = work 2 iterations () in
  let there = Domain.join other in
  if here > 0 || there > 0 then begin
    Printf.eprintf
      "the core scheduler is not per domain: %d + %d runs saw the other \
       domain's storage, out of %d each\n"
      here there iterations;
    exit 1
  end;
  check "no pause left pending" (Lwt.paused_count () = 0);
  (* and a fresh domain starts from a clean scheduler rather than inheriting *)
  ignore (Lwt.pause ());
  check "a pause is pending here" (Lwt.paused_count () = 1);
  let seen = Domain.join (Domain.spawn (fun () -> Lwt.paused_count ())) in
  check "a fresh domain sees no pause of ours" (seen = 0);
  Lwt.abandon_paused ();
  print_endline "core scheduler, two domains: ok"
