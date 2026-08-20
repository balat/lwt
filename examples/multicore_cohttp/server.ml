(* A cohttp-lwt-unix server, library UNCHANGED, served by N Lwt loops, one per
   domain, each with its own SO_REUSEPORT listener.

   The point is not the throughput. The point is that a real HTTP stack, written
   years before any of this and knowing nothing about domains, runs on N loops
   with no source change: the recipe is entirely on the application's side.

     dune exec ./server.exe -- 4 8080
     wrk -t4 -c64 -d8s http://127.0.0.1:8080/ *)

let ( let* ) = Lwt.bind

let listener port =
  let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt fd Unix.SO_REUSEADDR true;
  Lwt_unix.setsockopt fd Unix.SO_REUSEPORT true;
  let* () = Lwt_unix.bind fd (Unix.ADDR_INET (Unix.inet_addr_any, port)) in
  Lwt_unix.listen fd 1024;
  Lwt.return fd

let served = ref 0

let callback _conn _req _body =
  incr served;
  Cohttp_lwt_unix.Server.respond_string ~status:`OK ~body:"hello\n" ()

let serve port =
  Lwt_main.run
    (let* fd = listener port in
     let mode = `TCP (`Socket fd) in
     Cohttp_lwt_unix.Server.create ~mode
       (Cohttp_lwt_unix.Server.make ~callback ()))

let () =
  let domains = try int_of_string Sys.argv.(1) with _ -> 1 in
  let port = try int_of_string Sys.argv.(2) with _ -> 8080 in
  Printf.printf "cohttp-lwt-unix on %d domain(s), port %d\n%!" domains port;
  let others =
    List.init (domains - 1) (fun i ->
      Domain.spawn (fun () ->
        try serve port
        with e ->
          Printf.printf "domain %d died: %s\n%!" (i + 1) (Printexc.to_string e)))
  in
  serve port;
  List.iter Domain.join others
