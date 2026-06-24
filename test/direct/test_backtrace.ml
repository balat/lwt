(* Regression test for the backtrace-preservation fix in the core [run].

   A direct-style exception that escapes the top-level [Lwt_main.run] must keep
   its native backtrace: [run] re-raises rejected outcomes with
   [Printexc.raise_with_backtrace], not a bare [raise] that would reset the trace
   to [run] itself. The fiber frame [bt_regression_level] (below the run boundary)
   only survives if the preserved stack is propagated faithfully out of [run]. *)

let contains ~needle haystack =
  let nl = String.length needle and hl = String.length haystack in
  let rec go i = i + nl <= hl && (String.sub haystack i nl = needle || go (i + 1)) in
  nl = 0 || go 0

let[@inline never] bt_regression_boom () = failwith "bt-regression"

let[@inline never] bt_regression_level () =
  Lwt_direct.yield ();
  (* non-tail call so this frame survives on the fiber stack *)
  let x = bt_regression_boom () in
  Sys.opaque_identity x

let () =
  Printexc.record_backtrace true;
  let bt =
    try
      ignore (Lwt_main.run (Lwt_direct.spawn (fun () -> bt_regression_level ())));
      ""
    with Failure _ -> Printexc.get_backtrace ()
  in
  if contains ~needle:"bt_regression_level" bt then
    print_string "ok   - direct-style backtrace survives top-level run\n"
  else begin
    Printf.printf
      "FAIL - direct-style backtrace survives top-level run\n--- backtrace ---\n%s\n"
      bt;
    exit 1
  end
