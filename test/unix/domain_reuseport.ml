(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* THE SERVER RECIPE for several loops, and a test that it works.

   This is the shape to reach for first, because it needs no communication at all:
   one domain per core, each with its OWN listening socket on the SAME port
   through SO_REUSEPORT, each with its own Lwt loop, its own engine and its own
   connections. The kernel spreads incoming connections between them. Nothing is
   shared, so nothing has to be synchronised, and the ownership check has nothing
   to complain about.

   The recipe, in the order it has to be done:

   1. Bind the port ONCE first, in the parent, only to learn the port number when
      you asked the kernel for any (port 0). Skip this if the port is fixed.
   2. In each domain: create a socket, set SO_REUSEPORT on it, BEFORE binding,
      then bind and listen. SO_REUSEPORT after the bind is too late.
   3. Also set SO_REUSEADDR, so that a restart does not wait out TIME_WAIT.
   4. In each domain: run its own Lwt_main.run, accepting and serving on its own
      socket. Do not share a listening socket between domains: a descriptor
      belongs to the loop that created it, and Lwt will say so.
   5. Choose the engine per domain if you want to: Lwt_uring.set () on one and the
      default on another is legitimate, each gets its own ring.

   What this test checks: the connections really are spread, so more than one
   domain served, and every connection got its answer.

   Needs a second domain, hence OCaml 5. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let listeners = 2
let connections = 24
let served_by = Array.init listeners (fun _ -> Atomic.make 0)

(* Step 2 and 3 of the recipe: the option goes on before the bind. *)
let listen_on port =
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  Lwt_unix.setsockopt sock Unix.SO_REUSEPORT true;
  Lwt_unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_loopback, port))
  >>= fun () ->
  Lwt_unix.listen sock connections;
  Lwt.return sock

let serve which sock =
  let rec accept_loop () =
    Lwt_unix.accept sock >>= fun (fd, _) ->
    Atomic.incr served_by.(which);
    let oc = Lwt_io.of_fd ~mode:Lwt_io.output fd in
    Lwt_io.write oc "ok" >>= fun () ->
    Lwt_io.flush oc >>= fun () ->
    Lwt_unix.close fd >>= fun () ->
    accept_loop ()
  in
  accept_loop ()

let () =
  (* Step 1: learn the port. This socket is closed before the workers bind. *)
  let port =
    let probe = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.setsockopt probe Unix.SO_REUSEADDR true;
    Unix.bind probe (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
    let port =
      match Unix.getsockname probe with
      | Unix.ADDR_INET (_, p) -> p
      | Unix.ADDR_UNIX _ -> assert false
    in
    Unix.close probe;
    port
  in
  let ready = Atomic.make 0 in
  let stop = Atomic.make false in

  (* Step 4: one domain, one socket, one loop. *)
  let worker which =
    Domain.spawn (fun () ->
      Lwt_main.run
        (listen_on port >>= fun sock ->
         Atomic.incr ready;
         (* Serve until told to stop; the accept loop is cancelled by closing. *)
         Lwt.pick
           [ serve which sock;
             (let rec wait () =
                if Atomic.get stop then Lwt.return_unit
                else Lwt_unix.sleep 0.01 >>= wait
              in
              wait ()) ]
         >>= fun () -> Lwt_unix.close sock))
  in
  let workers = List.init listeners worker in
  while Atomic.get ready < listeners do
    Domain.cpu_relax ()
  done;

  (* The client side, on this domain: plain blocking sockets, to keep the test
     about the server. *)
  let answers = ref 0 in
  for _ = 1 to connections do
    let c = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.connect c (Unix.ADDR_INET (Unix.inet_addr_loopback, port));
    let buf = Bytes.create 2 in
    let n = Unix.read c buf 0 2 in
    if n = 2 && Bytes.to_string buf = "ok" then incr answers;
    Unix.close c
  done;
  Atomic.set stop true;
  List.iter Domain.join workers;

  check "every connection was answered" (!answers = connections);
  let used = Array.fold_left (fun n a -> if Atomic.get a > 0 then n + 1 else n) 0
      served_by
  in
  check "the kernel spread the connections over both listeners" (used = listeners);
  Printf.printf "served: %s\n"
    (String.concat " " (Array.to_list (Array.map (fun a ->
       string_of_int (Atomic.get a)) served_by)));

  if !failures > 0 then exit 1;
  print_endline "SO_REUSEPORT across domains: ok"
