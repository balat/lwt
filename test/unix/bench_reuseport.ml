(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The scaling benchmark for the server recipe: the same trivial HTTP server run
   on N domains, each with its own listening socket on the same port through
   SO_REUSEPORT, each with its own Lwt loop. Measure it from OUTSIDE, with wrk or
   any other load generator, so that the client is not the bottleneck:

     dune exec test/unix/bench_reuseport.exe -- 4 8080
     wrk -t4 -c100 -d10s http://127.0.0.1:8080/

   Run it with 1, then 2, then 4 domains and compare. What is being measured is
   whether N Lwt loops in one process actually use N cores, which is the whole
   point of the multicore work; the absolute numbers say more about the machine
   than about Lwt.

   Optionally installs the io_uring engine per domain, with LWT_BENCH_URING=1. *)

open Lwt.Infix

let response = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: keep-alive\r\n\r\nok"

let serve fd =
  let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd
  and oc = Lwt_io.of_fd ~close:(fun () -> Lwt.return_unit) ~mode:Lwt_io.output fd
  in
  let rec loop () =
    (* Read the request head, up to the blank line. *)
    let rec headers () =
      Lwt_io.read_line_opt ic >>= function
      | None -> Lwt.return_false
      | Some "" -> Lwt.return_true
      | Some _ -> headers ()
    in
    headers () >>= function
    | false -> Lwt.return_unit
    | true ->
      Lwt_io.write oc response >>= fun () -> Lwt_io.flush oc >>= loop
  in
  Lwt.finalize loop (fun () ->
    Lwt.catch (fun () -> Lwt_io.close ic) (fun _ -> Lwt.return_unit))

let listener port =
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  Lwt_unix.setsockopt sock Unix.SO_REUSEPORT true;
  Lwt_unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, port)) >>= fun () ->
  Lwt_unix.listen sock 1024;
  Lwt.return sock

let worker port =
  if (try Sys.getenv "LWT_BENCH_URING" = "1" with Not_found -> false) then
    Lwt_uring.set ();
  Lwt_main.run
    (listener port >>= fun sock ->
     let rec accept_loop () =
       Lwt_unix.accept sock >>= fun (fd, _) ->
       Lwt.async (fun () ->
         Lwt.catch (fun () -> serve fd) (fun _ -> Lwt.return_unit));
       accept_loop ()
     in
     accept_loop ())

let () =
  let domains = if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 1
  and port = if Array.length Sys.argv > 2 then int_of_string Sys.argv.(2) else 8080 in
  Printf.printf "serving on port %d from %d domain(s); ^C to stop\n%!" port
    domains;
  (* One domain per listener, including this one, so that N means N. *)
  let others =
    List.init (domains - 1) (fun _ -> Domain.spawn (fun () -> worker port))
  in
  ignore (worker port : unit);
  List.iter Domain.join others
