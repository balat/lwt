(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A producer waiting for room in a full stream keeps its turn. Room that a
   consumer frees is reserved for it until it retries, on its own loop; a
   producer arriving in between finds the stream full and waits behind it.

   It used to be woken to retry with the room left free, so the newcomer took
   it, and the woken producer queued again at the back: across domains, for as
   long as its loop took to run, which is enough to starve it.

   No loop runs on this domain while the other one takes and pushes, which is
   what keeps the waiting producer's retry pending. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let elsewhere f = Domain.join (Domain.spawn f)

let () =
  ignore (Unix.alarm 30);

  let s = Lwt_multicore.Stream.create ~capacity:1 in
  ignore (Lwt_multicore.Stream.push s "a");
  let waiting = Lwt_multicore.Stream.push s "b" in
  check "the stream is full, the producer waits" (Lwt.is_sleeping waiting);

  (* On another domain: take, which frees the room, then push. *)
  let took, newcomer_waits =
    elsewhere (fun () ->
      let took = Lwt.state (Lwt_multicore.Stream.take s) in
      let late = Lwt_multicore.Stream.push s "c" in
      let waits = Lwt.is_sleeping late in
      (* Withdraw it, so that nothing of this domain is left in the stream. *)
      Lwt.cancel late;
      (took, waits))
  in
  check "the other domain took the first item" (took = Lwt.Return (Some "a"));
  check "a producer arriving later waits: the room is kept" newcomer_waits;
  check "and the room is not counted as an item"
    (Lwt_multicore.Stream.length s = 0);

  (* Our loop runs: the waiting producer retries with the room kept for it. *)
  let got_in =
    Lwt_main.run
      (Lwt.pick
         [ Lwt.map (fun () -> true) waiting;
           Lwt.map (fun () -> false) (Lwt_unix.sleep 2.) ])
  in
  check "the waiting producer got its turn"
    (got_in
     && Lwt_multicore.Stream.length s = 1
     && Lwt.state (Lwt_multicore.Stream.take s) = Lwt.Return (Some "b"));

  (* A waiting producer cancelled after the room was kept for it: the room goes
     back, and is free for the next producer at once. *)
  let s = Lwt_multicore.Stream.create ~capacity:1 in
  ignore (Lwt_multicore.Stream.push s 1);
  let p = Lwt_multicore.Stream.push s 2 in
  elsewhere (fun () -> ignore (Lwt_multicore.Stream.take s));
  Lwt.cancel p;
  check "a cancelled producer gives its room back at once"
    (Lwt.state (Lwt_multicore.Stream.push s 3) = Lwt.Return ());
  Lwt_main.run (Lwt_unix.sleep 0.05);
  check "and its retry never happens"
    (Lwt_multicore.Stream.length s = 1
     && Lwt.state (Lwt_multicore.Stream.take s) = Lwt.Return (Some 3));

  if !failures > 0 then exit 1;
  print_endline "waiting producers keep their turn: ok"
