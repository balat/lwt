(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* Standalone smoke test for the io_uring Lwt engine. It drives real [Lwt_unix]
   operations (timers, descriptor readiness, cancellation) through the engine,
   checking that it is a correct drop-in for libev/select. *)

open Lwt.Infix

let failures = ref 0

let check name cond =
  if cond then Printf.printf "ok   - %s\n%!" name
  else begin
    incr failures;
    Printf.printf "FAIL - %s\n%!" name
  end

(* A timer resolves after roughly the requested delay. *)
let test_timer () =
  let t0 = Unix.gettimeofday () in
  Lwt_main.run (Lwt_unix.sleep 0.05);
  let dt = Unix.gettimeofday () -. t0 in
  check "timer fires after its delay" (dt >= 0.03 && dt <= 1.0)

(* Concurrent timers resolve in time order. *)
let test_timer_order () =
  let order = ref [] in
  let record n () = Lwt_unix.sleep n >|= fun () -> order := n :: !order in
  Lwt_main.run (Lwt.join [record 0.06 (); record 0.01 (); record 0.03 ()]);
  check "timers resolve in delay order" (!order = [0.06; 0.03; 0.01])

(* Descriptor readiness: a socketpair round-trip exercises both
   register_readable and register_writable. *)
let test_socketpair () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a;
  Unix.set_nonblock b;
  let a = Lwt_unix.of_unix_file_descr a in
  let b = Lwt_unix.of_unix_file_descr b in
  let msg = Bytes.of_string "ping-pong" in
  let n = Bytes.length msg in
  let received = Bytes.create n in
  let result =
    Lwt_main.run begin
      let writer = Lwt_unix.write a msg 0 n in
      let reader = Lwt_unix.read b received 0 n in
      writer >>= fun written ->
      reader >>= fun read ->
      Lwt.return (written, read)
    end
  in
  let written, read = result in
  check "socketpair write/read round-trip"
    (written = n && read = n && Bytes.equal received msg);
  Lwt_main.run (Lwt_unix.close a >>= fun () -> Lwt_unix.close b)

(* A cancelled timer rejects with [Lwt.Canceled] and does not delay the loop. *)
let test_cancel () =
  let cancelled = ref false in
  Lwt_main.run begin
    let long = Lwt_unix.sleep 10. in
    Lwt.on_failure long (fun _ -> cancelled := true);
    Lwt.cancel long;
    Lwt.catch (fun () -> long) (function
      | Lwt.Canceled -> Lwt.return_unit
      | exn -> Lwt.fail exn)
  end;
  check "cancelling a timer rejects with Canceled" !cancelled

(* Paused promises and engine I/O cooperate (Lwt_main alternates iter false /
   iter true). *)
let test_pause_and_timer () =
  let steps = ref 0 in
  Lwt_main.run begin
    Lwt.pause () >>= fun () ->
    incr steps;
    Lwt_unix.sleep 0.01 >>= fun () ->
    incr steps;
    Lwt.pause () >>= fun () ->
    incr steps;
    Lwt.return_unit
  end;
  check "pause and timers interleave correctly" (!steps = 3)

let () =
  check "io_uring is available" (Lwt_uring.available ());
  Lwt_uring.set ();
  (match Lwt_engine.id () with
   | Lwt_uring.Engine_id__uring -> check "uring engine installed" true
   | _ -> check "uring engine installed" false);
  test_timer ();
  test_timer_order ();
  test_socketpair ();
  test_cancel ();
  test_pause_and_timer ();
  if !failures = 0 then Printf.printf "\nAll tests passed.\n%!"
  else begin
    Printf.printf "\n%d test(s) failed.\n%!" !failures;
    exit 1
  end
