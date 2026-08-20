(* The invariants the browser depends on, and the js_of_ocaml smoke test.

   Under js_of_ocaml there is one domain and no scheduler but Lwt's own: no
   Lwt_main to pump a queue, so what makes Lwt usable there is that resolution is
   SYNCHRONOUS at the wakeup. This checks that, plus that the per-domain layer
   degrades to nothing.

   It runs as an ordinary test everywhere. To run it as the jsoo smoke test, in a
   switch that has js_of_ocaml and with node on the PATH:

     sh test/jsoo/smoke.sh

   That script is what CI runs (job "js_of_ocaml / 5.4"), so the two cannot drift
   apart. It is a script rather than a dune rule because js_of_ocaml is not a
   dependency of lwt and must not become one.

   Why this matters here rather than at verification time: the shim [Lwt_dls]
   keys on the COMPILER version through cppo, not on the backend. Under jsoo the
   compiler is OCaml 5, so the shim takes the Domain.DLS branch, which is only
   correct because js_of_ocaml implements the DLS primitive. Verified on jsoo
   6.3.2 with node 18. If a backend ever lacked it, the shim would silently take
   the wrong branch, and this test is the only thing that would say so. *)

(* Lwt's own tests may use the internal slot and Private. *)
[@@@alert "-trespassing"]
[@@@alert "-lwt_internal"]

let failures = ref 0
let check name b = if not b then (Printf.printf "FAILED: %s\n" name; incr failures)

let () =
  (* the slot itself *)
  let k = Lwt_dls.new_key (fun () -> 41) in
  check "slot initialiser" (Lwt_dls.get k = 41);
  Lwt_dls.set k 42;
  check "slot set/get" (Lwt_dls.get k = 42);

  (* the core: resolved bind, the path jsoo relies on being synchronous *)
  let p = Lwt.bind (Lwt.return 1) (fun v -> Lwt.return (v + 1)) in
  check "resolved bind is synchronous" (Lwt.state p = Lwt.Return 2);

  (* wakeup resolves before returning, which is what keeps Lwt usable in the
     browser: there is no Lwt_main.run to pump a queue *)
  let q, r = Lwt.wait () in
  let seen = ref None in
  Lwt.on_success q (fun v -> seen := Some v);
  Lwt.wakeup r 7;
  check "wakeup resolves synchronously" (Lwt.state q = Lwt.Return 7);
  check "and ran the callback" (!seen = Some 7);

  (* fiber-local storage across a suspension, driven by the core's own scheduler *)
  let key : string Lwt.key = Lwt.new_key () in
  let got =
    Lwt.Private.scheduler_run (fun () ->
      Lwt.with_value key (Some "hello") (fun () ->
        Lwt.bind (Lwt.pause ()) (fun () -> Lwt.return (Lwt.get key))))
  in
  check "storage survives a pause" (got = Some "hello");

  (* the ownership check must not fire in a single-domain world *)
  let a, ra = Lwt.wait () in
  (match Lwt.bind a (fun v -> Lwt.return v) with
   | _ -> ()
   | exception e -> Printf.printf "FAILED: bind raised %s\n" (Printexc.to_string e); incr failures);
  Lwt.wakeup ra ();

  if !failures = 0 then print_string "browser invariants: ok\n"
  else (Printf.printf "%d checks failed\n" !failures; exit 1)
