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

(* Forced ONCE, on the main domain, before any other domain exists, and passed to
   every loop afterwards. This is the second thing to know about running an existing
   library on N loops, and it is a general rule rather than a cohttp detail: a
   process-wide [lazy] forced by two domains at the same moment raises
   [CamlinternalLazy.Undefined] in one of them.

   Cohttp's default context is exactly that, three lazies deep: Net.default_ctx
   forces Conduit_lwt_unix.default_ctx, which forces the TLS authenticator, which
   reads the system certificate store. Leave it to the default argument of
   [Server.create] and each domain races to force it on its first call. The failure
   is a startup crash that appears roughly one run in ten, which is the worst kind. *)
let shared_ctx = Lazy.force Cohttp_lwt_unix.Net.default_ctx

let serve port =
  Lwt_main.run
    (let* fd = listener port in
     let mode = `TCP (`Socket fd) in
     Cohttp_lwt_unix.Server.create ~ctx:shared_ctx ~mode
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
