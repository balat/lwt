(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* Runs Lwt's existing [test/unix] suites with the io_uring engine installed,
   to confirm that the engine is a correct drop-in for libev/select. *)

open Tester

let () =
  Lwt_uring.set ();
  (* [Test_lwt_engine] is intentionally omitted: it asserts engine-identity
     details (e.g. that the current engine is the compile-time default), which
     do not hold once a different engine has been installed. All other suites
     are engine-agnostic and must pass unchanged on io_uring. *)
  Test.concurrent "unix-on-uring" [
    Test_lwt_unix.suite;
    Test_lwt_io.suite;
    Test_lwt_io_non_block.suite;
    Test_lwt_timeout.suite;
    Test_lwt_bytes.suite;
    Test_sleep_and_timeout.suite;
  ]
