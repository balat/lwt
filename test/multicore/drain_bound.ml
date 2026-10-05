(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Work that reposts itself to its own loop must not keep the loop to itself.
   A drain of the inbox served everything it found, including what was posted
   during the drain, so such a chain ran in one pass and a timer due meanwhile
   fired only when it ended. A drain now serves what was there when it began.
   Needs OCaml 5 for the library, though it uses one domain. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let limit = 100_000

let () =
  ignore (Unix.alarm 60);
  let loop = Lwt_multicore.self () in
  let count = ref 0 in
  let rec job () =
    incr count;
    if !count < limit then Lwt_multicore.run_on loop job
  in
  let seen_by_timer = ref (-1) in
  Lwt_main.run
    (Lwt_multicore.run_on loop job;
     let timer =
       Lwt.map (fun () -> seen_by_timer := !count) (Lwt_unix.sleep 0.001)
     in
     let rec chain_done () =
       if !count >= limit then Lwt.return_unit
       else Lwt.bind (Lwt.pause ()) chain_done
     in
     Lwt.join [ timer; chain_done () ]);
  check
    (Printf.sprintf "a timer fires while the chain runs (it saw %d of %d)"
       !seen_by_timer limit)
    (!seen_by_timer >= 0 && !seen_by_timer < limit);
  check "and the chain completes" (!count = limit);
  if !failures > 0 then exit 1;
  print_endline "work that reposts itself shares the loop: ok"
