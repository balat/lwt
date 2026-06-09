(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* A request/response server benchmark over Lwt_io channels — the same I/O path
   that cohttp-lwt-unix uses (Lwt_io reads/writes go through
   Lwt_unix.read_bigarray/write_bigarray, which the io_uring engine routes to
   completion-based recv/send). It measures throughput under the default engine
   and then under the io_uring engine, with no change to the workload — so the
   speedup, if any, is entirely from transparently routing the existing Lwt_io
   code through io_uring. *)

open Lwt.Infix

let connections = 50
let requests_per_connection = 400
let request = "GET / HTTP/1.1\r\n\r\n"
let response = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"

(* One connection handler: read a request (terminated by a blank line) and write
   the response, [requests_per_connection] times (keep-alive). *)
let serve_connection ic oc =
  let rec loop n =
    if n = 0 then Lwt.return_unit
    else
      (* Read request headers up to the blank line. *)
      let rec read_headers () =
        Lwt_io.read_line_opt ic >>= function
        | None -> Lwt.return_false
        | Some "" -> Lwt.return_true
        | Some _ -> read_headers ()
      in
      read_headers () >>= function
      | false -> Lwt.return_unit
      | true ->
        Lwt_io.write oc response >>= fun () ->
        Lwt_io.flush oc >>= fun () ->
        loop (n - 1)
  in
  loop requests_per_connection

(* One client: connect, then issue [requests_per_connection] requests, reading
   each response. *)
let client addr =
  let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.connect fd addr >>= fun () ->
  (* Two channels share one fd; only the output channel closes it. *)
  let ic =
    Lwt_io.of_fd ~mode:Lwt_io.input ~close:(fun () -> Lwt.return_unit) fd
  in
  let oc = Lwt_io.of_fd ~mode:Lwt_io.output fd in
  let response_len = String.length response in
  let buf = Bytes.create response_len in
  let rec loop n =
    if n = 0 then Lwt.return_unit
    else
      Lwt_io.write oc request >>= fun () ->
      Lwt_io.flush oc >>= fun () ->
      Lwt_io.read_into_exactly ic buf 0 response_len >>= fun () ->
      loop (n - 1)
  in
  loop requests_per_connection >>= fun () ->
  Lwt_io.close oc >>= fun () ->
  Lwt_io.close ic

let run_once () =
  (* Listening socket on an ephemeral loopback port. *)
  let lsock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Lwt_unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0)) >>= fun () ->
  Lwt_unix.listen lsock connections;
  let addr = Lwt_unix.getsockname lsock in
  (* Accept [connections] connections and serve each. *)
  let server =
    let rec accept_loop remaining acc =
      if remaining = 0 then Lwt.return acc
      else
        Lwt_unix.accept lsock >>= fun (fd, _) ->
        let ic =
          Lwt_io.of_fd ~mode:Lwt_io.input ~close:(fun () -> Lwt.return_unit) fd
        in
        let oc = Lwt_io.of_fd ~mode:Lwt_io.output fd in
        let served =
          serve_connection ic oc >>= fun () ->
          Lwt_io.close oc >>= fun () ->
          Lwt_io.close ic
        in
        accept_loop (remaining - 1) (served :: acc)
    in
    accept_loop connections [] >>= Lwt.join
  in
  let clients = List.init connections (fun _ -> client addr) in
  Lwt.join (server :: clients) >>= fun () ->
  Lwt_unix.close lsock

let measure name =
  Lwt_main.run (run_once ());
  (* warm up *)
  let t0 = Unix.gettimeofday () in
  Lwt_main.run (run_once ());
  let dt = Unix.gettimeofday () -. t0 in
  let total = connections * requests_per_connection in
  Printf.printf "%-26s %8.0f req/s  (%d reqs in %.3f s)\n%!" name
    (float_of_int total /. dt) total dt

let () =
  Printf.printf "request/response over Lwt_io: %d connections x %d requests\n%!"
    connections requests_per_connection;
  measure "default engine (libev)";
  Lwt_uring.set ();
  measure "io_uring (transparent)"
