(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* After a burst of a million tasks, the scheduler's run queue goes back to a
   small array once it is empty. It used to keep the array the burst grew, a
   million slots, for as long as the domain lived. *)

let burst = 1_000_000

let live () =
  Gc.full_major ();
  (Gc.quick_stat ()).Gc.live_words

let () =
  Lwt_main.run (Lwt.return_unit);
  let before = live () in
  (* A million pauses, served at once: a million tasks in the queue. *)
  Lwt_main.run (Lwt.join (List.init burst (fun _ -> Lwt.pause ())));
  let after = live () in
  if after - before > burst / 2 then begin
    Printf.eprintf "FAILED: %d words still live after the burst\n" (after - before);
    exit 1
  end;
  print_endline "the run queue shrinks after a burst: ok"
