(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* [run_on] is the one primitive that hands code to another domain, so what has to
   be true of it is: the thunk runs ON THE TARGET's domain, it can resolve that
   loop's promises, a loop that has terminated refuses further posts instead of
   swallowing them, and a loop that never asked for a handle pays nothing.

   Needs a second domain, hence OCaml 5, like the library. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let ran_on = Atomic.make (-1)
let answered = Atomic.make (-1)

let () =
  (* Our own loop's handle, shared with the domain below. *)
  let ours = Lwt_multicore.self () in
  let us = (Domain.self () :> int) in

  (* A promise of OURS, which the posted thunk resolves. That is the point: the
     thunk runs here, so it may touch our promises, which no other domain may. *)
  let waiter, wakener = Lwt.wait () in

  let theirs =
    Domain.spawn (fun () ->
      let them = (Domain.self () :> int) in
      Lwt_multicore.run_on ours (fun () ->
        Atomic.set ran_on (Domain.self () :> int);
        Lwt.wakeup wakener (them * 2));
      them)
  in
  let them = Domain.join theirs in

  (* Drain our inbox by running our loop, and take the answer. *)
  let got = Lwt_main.run waiter in
  check "the posted thunk ran on the target's domain" (Atomic.get ran_on = us);
  check "and it resolved the target's own promise" (got = them * 2);

  (* The other direction, so that neither side is special: we post to a loop that
     then drains it itself. *)
  let round_trip =
    Domain.spawn (fun () ->
      let theirs = Lwt_multicore.self () in
      let waiter, wakener = Lwt.wait () in
      (* Hand our handle back through a plain data channel. *)
      Lwt_multicore.run_on theirs (fun () -> Lwt.wakeup wakener 7);
      Atomic.set answered (Lwt_main.run waiter);
      theirs)
  in
  let dead_loop = Domain.join round_trip in
  check "a loop drains what it posted to itself" (Atomic.get answered = 7);

  (* And that loop's domain is gone now, so posting to it must fail rather than
     accept work nobody will do. *)
  check "posting to a terminated loop is refused"
    (match Lwt_multicore.run_on dead_loop (fun () -> ()) with
     | () -> false
     | exception Lwt_multicore.Loop_terminated -> true
     | exception _ -> false);

  if !failures > 0 then exit 1;
  print_endline "run_on: work crosses, promises do not: ok"
