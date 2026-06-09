(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* A socketpair ping-pong micro-benchmark, run under the default engine and then
   under the io_uring engine, to compare per-round-trip latency. This is the
   stage-1 (readiness) engine, so the actual read/write syscalls are still done
   by Lwt_unix; the comparison mainly reflects the cost of the readiness
   mechanism (io_uring poll vs epoll/select). *)

open Lwt.Infix

let payload = 64
let round_trips = 50_000

(* One side sends [round_trips] messages and waits for each echo. *)
let pingpong () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a;
  Unix.set_nonblock b;
  let a = Lwt_unix.of_unix_file_descr a in
  let b = Lwt_unix.of_unix_file_descr b in
  let buf_a = Bytes.create payload in
  let buf_b = Bytes.create payload in
  let rec write_all fd buf off len =
    if len = 0 then Lwt.return_unit
    else Lwt_unix.write fd buf off len >>= fun n -> write_all fd buf (off + n) (len - n)
  in
  let rec read_all fd buf off len =
    if len = 0 then Lwt.return_unit
    else Lwt_unix.read fd buf off len >>= fun n ->
      if n = 0 then Lwt.return_unit
      else read_all fd buf (off + n) (len - n)
  in
  (* Echo server on [b]. *)
  let server =
    let rec loop n =
      if n = 0 then Lwt.return_unit
      else
        read_all b buf_b 0 payload >>= fun () ->
        write_all b buf_b 0 payload >>= fun () ->
        loop (n - 1)
    in
    loop round_trips
  in
  (* Client on [a]. *)
  let client =
    let rec loop n =
      if n = 0 then Lwt.return_unit
      else
        write_all a buf_a 0 payload >>= fun () ->
        read_all a buf_a 0 payload >>= fun () ->
        loop (n - 1)
    in
    loop round_trips
  in
  Lwt.join [server; client] >>= fun () ->
  Lwt_unix.close a >>= fun () ->
  Lwt_unix.close b

let measure name =
  (* Warm up, then measure. *)
  Lwt_main.run (pingpong ());
  let t0 = Unix.gettimeofday () in
  Lwt_main.run (pingpong ());
  let dt = Unix.gettimeofday () -. t0 in
  Printf.printf "%-22s %8.2f us/round-trip  (%.0f round-trips/s)\n%!"
    name (dt /. float_of_int round_trips *. 1e6)
    (float_of_int round_trips /. dt)

let () =
  Printf.printf "ping-pong: %d round-trips, %d-byte payload\n%!"
    round_trips payload;
  measure "default engine";
  Lwt_uring.set ();
  measure "io_uring engine";
