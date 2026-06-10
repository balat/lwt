(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* A new-connection benchmark: each request opens a fresh TCP connection (connect
   + accept), exchanges one small request/response, and closes. Unlike the
   keep-alive server benchmark, the cost here is dominated by connection setup —
   so it isolates the benefit of routing accept/connect through io_uring
   (IORING_OP_ACCEPT / IORING_OP_CONNECT) rather than the data path. The same
   workload is measured under the default engine and then under io_uring. *)

open Lwt.Infix

let total = 20000
let concurrency = 50
let request = "ping"
let response = "pong"

let serve_one fd =
  let buf = Bytes.create (String.length request) in
  Lwt_unix.read fd buf 0 (Bytes.length buf) >>= fun _ ->
  Lwt_unix.write_string fd response 0 (String.length response) >>= fun _ ->
  Lwt_unix.close fd

let one_request addr =
  let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.connect fd addr >>= fun () ->
  Lwt_unix.write_string fd request 0 (String.length request) >>= fun _ ->
  let buf = Bytes.create (String.length response) in
  Lwt_unix.read fd buf 0 (Bytes.length buf) >>= fun _ -> Lwt_unix.close fd

let run_once () =
  let lsock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Lwt_unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0)) >>= fun () ->
  Lwt_unix.listen lsock concurrency;
  let addr = Lwt_unix.getsockname lsock in
  (* Server: accept [total] connections, serving each. *)
  let server =
    let rec loop remaining acc =
      if remaining = 0 then Lwt.return acc
      else
        Lwt_unix.accept lsock >>= fun (fd, _) ->
        loop (remaining - 1) (serve_one fd :: acc)
    in
    loop total [] >>= Lwt.join
  in
  (* Clients: keep [concurrency] new-connection requests in flight at a time. *)
  let issued = ref 0 in
  let rec worker () =
    if !issued >= total then Lwt.return_unit
    else begin
      incr issued;
      one_request addr >>= worker
    end
  in
  let clients = List.init concurrency (fun _ -> worker ()) in
  Lwt.join (server :: clients) >>= fun () -> Lwt_unix.close lsock

let measure name =
  Lwt_main.run (run_once ());
  (* warm up *)
  let t0 = Unix.gettimeofday () in
  Lwt_main.run (run_once ());
  let dt = Unix.gettimeofday () -. t0 in
  Printf.printf "%-26s %8.0f conn/s  (%d in %.3f s)\n%!" name
    (float_of_int total /. dt) total dt

let () =
  Printf.printf "new-connection rate: %d connections, %d concurrent\n%!" total
    concurrency;
  measure "default engine (libev)";
  Lwt_uring.set ();
  measure "io_uring (connect routed)"
