(* Run Lwt's own core suite (unchanged) against the effect-backed candidate. *)

let () = Test.run "core-on-effects" Test_lwt.suites
