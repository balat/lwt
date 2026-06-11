(* Run Lwt's own react suites (unchanged) against the effect-backed candidate. *)

let () =
  Test.run "react-on-effects" [ Test_lwt_event.suite; Test_lwt_signal.suite ]
