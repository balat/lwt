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

(* Completion-based I/O (Lwt_uring.Io): the kernel performs the transfer; no
   readiness wait. *)
let test_io_socketpair () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let msg = Bytes.of_string "completion!" in
  let n = Bytes.length msg in
  let got = Bytes.create n in
  let written, read =
    Lwt_main.run begin
      Lwt_uring.Io.write a msg 0 n >>= fun written ->
      Lwt_uring.Io.read b got 0 n >>= fun read ->
      Lwt.return (written, read)
    end
  in
  check "Io completion-based socketpair round-trip"
    (written = n && read = n && Bytes.equal got msg);
  Unix.close a;
  Unix.close b

(* io_uring can read a regular file asynchronously — readiness engines cannot
   poll regular files at all. *)
let test_io_regular_file () =
  let path = Filename.temp_file "lwt_uring_test" ".dat" in
  let contents = "the quick brown fox" in
  let oc = open_out_bin path in
  output_string oc contents;
  close_out oc;
  let n = String.length contents in
  let fd = Unix.openfile path [ Unix.O_RDONLY ] 0 in
  let buf = Bytes.create n in
  let read = Lwt_main.run (Lwt_uring.Io.read fd buf 0 n) in
  Unix.close fd;
  Sys.remove path;
  check "Io reads a regular file asynchronously"
    (read = n && Bytes.equal buf (Bytes.of_string contents))

let test_io_bigarray () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let n = 8 in
  let src = Bigarray.Array1.create Bigarray.char Bigarray.c_layout n in
  for i = 0 to n - 1 do Bigarray.Array1.set src i (Char.chr (65 + i)) done;
  let dst = Bigarray.Array1.create Bigarray.char Bigarray.c_layout n in
  let ok =
    Lwt_main.run begin
      Lwt_uring.Io.write_bigarray a src 0 n >>= fun _ ->
      Lwt_uring.Io.read_bigarray b dst 0 n >>= fun r ->
      Lwt.return (r = n)
    end
  in
  let same = ref ok in
  for i = 0 to n - 1 do
    if Bigarray.Array1.get src i <> Bigarray.Array1.get dst i then same := false
  done;
  check "Io zero-copy bigarray round-trip" !same;
  Unix.close a;
  Unix.close b

(* Lwt_unix.connect routed through io_uring (IORING_OP_CONNECT): a loopback TCP
   connection is established through the ring (the accept side uses the default
   path, whose readiness already runs on the engine), then exchanges data. *)
let test_connect () =
  let lsock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt lsock Unix.SO_REUSEADDR true;
  let msg = Bytes.of_string "uring-connect" in
  let n = Bytes.length msg in
  let got = Bytes.create n in
  let read =
    Lwt_main.run begin
      Lwt_unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0))
      >>= fun () ->
      Lwt_unix.listen lsock 1;
      let addr = Lwt_unix.getsockname lsock in
      let server =
        Lwt_unix.accept lsock >>= fun (fd, _) ->
        Lwt_unix.read fd got 0 n >>= fun r ->
        Lwt_unix.close fd >>= fun () -> Lwt.return r
      in
      let client =
        let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
        Lwt_unix.connect fd addr >>= fun () ->
        Lwt_unix.write fd msg 0 n >>= fun _ -> Lwt_unix.close fd
      in
      Lwt.both server client >>= fun (r, ()) -> Lwt.return r
    end
  in
  check "connect routed through io_uring" (read = n && Bytes.equal got msg);
  Lwt_main.run (Lwt_unix.close lsock)

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
  test_io_socketpair ();
  test_io_regular_file ();
  test_io_bigarray ();
  test_connect ();
  if !failures = 0 then Printf.printf "\nAll tests passed.\n%!"
  else begin
    Printf.printf "\n%d test(s) failed.\n%!" !failures;
    exit 1
  end
