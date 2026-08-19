(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The property the per-domain slot exists for, and the only one that needs more
   than one domain: two domains must see two INDEPENDENT cells, and neither must
   observe the other's writes. Built from OCaml 5 only; the single-domain half is
   in test_lwt_dls.ml and builds everywhere. *)

(* Lwt's own tests may use the internal slot. *)
[@@@alert "-lwt_internal"]

let check name b = if not b then (Printf.eprintf "FAILED: %s\n" name; exit 1)

let () =
  let k = Lwt_dls.new_key (fun () -> 0) in
  (* the initialiser runs per domain, so a fresh domain sees the initial value
     and not whatever the main domain has written *)
  Lwt_dls.set k 1;
  let seen = Domain.join (Domain.spawn (fun () -> Lwt_dls.get k)) in
  check "a fresh domain runs the initialiser" (seen = 0);
  check "the other domain did not disturb us" (Lwt_dls.get k = 1);
  (* and writes stay local: hammer both sides and check neither leaks *)
  let iterations = 200_000 in
  let arrived = Atomic.make 0 in
  let rendezvous () =
    ignore (Atomic.fetch_and_add arrived 1);
    while Atomic.get arrived < 2 do
      Domain.cpu_relax ()
    done
  in
  let hammer mine () =
    Lwt_dls.set k mine;
    rendezvous ();
    let bad = ref 0 in
    for _ = 1 to iterations do
      if Lwt_dls.get k <> mine then incr bad;
      Lwt_dls.set k mine
    done;
    !bad
  in
  let other = Domain.spawn (hammer 2) in
  let here = hammer 3 () in
  let there = Domain.join other in
  if here > 0 || there > 0 then begin
    Printf.eprintf
      "the per-domain slot leaks between domains: %d + %d bad reads out of %d \
       each\n"
      here there iterations;
    exit 1
  end;
  print_endline "lwt_dls, two domains: ok"
