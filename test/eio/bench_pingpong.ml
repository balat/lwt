(* Ping-pong over a socketpair, payload sweep, comparing:
     - Lwt + libev            (baseline, readiness + Unix.read/write)
     - Lwt on Eio, Level A    (shared Eio loop, readiness: no completion I/O)
     - Lwt on Eio, Level B    (completion I/O on Eio's shared io_uring ring)
     - Eio native             (Eio.Flow on io_uring, the ceiling)

   The three Lwt rows use ONLY the public Lwt API and the SAME ping-pong body;
   the only thing that changes is what drives I/O. Level A vs Level B isolates
   the value of routing Lwt's own read/write through Eio's ring. Two buffer
   variants per Lwt row: bytes (Lwt_unix, a copy under completion) and bigarray
   (Lwt_bytes, the Lwt_io/cohttp path, copy-free under completion).

   For the Lwt + io_uring (Lwt's OWN ring, lwt_uring) reference, see the bench
   repo's pingpong/ row; it is not linked here to keep the completion backend
   unambiguous (only Eio's is installed). *)

let sizes = [ 1; 64; 1024; 16384; 262144 ]
let round_trips size = max 2000 (min 50000 (100_000_000 / size))

type result = { us_per_rt : float }

let measure ~size ~rt (f : size:int -> rt:int -> unit) : result =
  f ~size ~rt:(min rt 200);
  (* warm up *)
  let t0 = Unix.gettimeofday () in
  f ~size ~rt;
  let dt = Unix.gettimeofday () -. t0 in
  { us_per_rt = dt /. float_of_int rt *. 1e6 }

let pair () = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0

(* ---- Lwt ping-pong bodies (unit Lwt.t), bytes and bigarray ------------- *)

let lwt_bytes_pp la lb ~size ~rt =
  let open Lwt.Infix in
  let msg = Bytes.make size 'x' in
  let cbuf = Bytes.create size and sbuf = Bytes.create size in
  let rec write_all fd buf off len =
    if len = 0 then Lwt.return_unit
    else Lwt_unix.write fd buf off len >>= fun n -> write_all fd buf (off + n) (len - n)
  in
  let rec read_exact fd buf off len =
    if len = 0 then Lwt.return_unit
    else
      Lwt_unix.read fd buf off len >>= fun n ->
      if n = 0 then Lwt.fail End_of_file else read_exact fd buf (off + n) (len - n)
  in
  let rec client n =
    if n = 0 then Lwt.return_unit
    else write_all la msg 0 size >>= fun () -> read_exact la cbuf 0 size >>= fun () -> client (n - 1)
  in
  let rec server n =
    if n = 0 then Lwt.return_unit
    else read_exact lb sbuf 0 size >>= fun () -> write_all lb msg 0 size >>= fun () -> server (n - 1)
  in
  Lwt.join [ client rt; server rt ]

let lwt_ba_pp la lb ~size ~rt =
  let open Lwt.Infix in
  let msg = Lwt_bytes.create size in
  Lwt_bytes.fill msg 0 size 'x';
  let cbuf = Lwt_bytes.create size and sbuf = Lwt_bytes.create size in
  let rec write_all fd buf off len =
    if len = 0 then Lwt.return_unit
    else Lwt_bytes.write fd buf off len >>= fun n -> write_all fd buf (off + n) (len - n)
  in
  let rec read_exact fd buf off len =
    if len = 0 then Lwt.return_unit
    else
      Lwt_bytes.read fd buf off len >>= fun n ->
      if n = 0 then Lwt.fail End_of_file else read_exact fd buf (off + n) (len - n)
  in
  let rec client n =
    if n = 0 then Lwt.return_unit
    else write_all la msg 0 size >>= fun () -> read_exact la cbuf 0 size >>= fun () -> client (n - 1)
  in
  let rec server n =
    if n = 0 then Lwt.return_unit
    else read_exact lb sbuf 0 size >>= fun () -> write_all lb msg 0 size >>= fun () -> server (n - 1)
  in
  Lwt.join [ client rt; server rt ]

(* ---- Drivers ----------------------------------------------------------- *)

let with_libev body ~size ~rt =
  let a, b = pair () in
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  Lwt_main.run (body la lb ~size ~rt);
  Unix.close a;
  Unix.close b

let with_eio ~completion body ~size ~rt =
  let a, b = pair () in
  Eio_linux.run @@ fun env ->
  Lwt_eio_backend.with_event_loop ~clock:(Eio.Stdenv.clock env) @@ fun () ->
  if completion then Lwt_eio_backend.enable_completion_io ();
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  Lwt_eio_backend.run_lwt (fun () -> body la lb ~size ~rt);
  if completion then Lwt_eio_backend.disable_completion_io ();
  Unix.close a;
  Unix.close b

let bench_eio_native ~size ~rt =
  let a, b = pair () in
  Eio_linux.run @@ fun _env ->
  Eio.Switch.run @@ fun sw ->
  let fa = Eio_unix.Net.import_socket_stream ~sw ~close_unix:true a in
  let fb = Eio_unix.Net.import_socket_stream ~sw ~close_unix:true b in
  let msg = Cstruct.create size in
  Cstruct.memset msg (Char.code 'x');
  let cbuf = Cstruct.create size and sbuf = Cstruct.create size in
  let client () =
    for _ = 1 to rt do
      Eio.Flow.write fa [ msg ];
      Eio.Flow.read_exact fa cbuf
    done
  in
  let server () =
    for _ = 1 to rt do
      Eio.Flow.read_exact fb sbuf;
      Eio.Flow.write fb [ msg ]
    done
  in
  Eio.Fiber.both client server

(* ---- Runner ------------------------------------------------------------ *)

let run_config name f =
  List.iter
    (fun size ->
      let rt = round_trips size in
      let r = measure ~size ~rt f in
      Printf.printf "%-34s %10d %14.2f\n%!" name size r.us_per_rt)
    sizes

let () =
  Printf.printf "Ping-pong socketpair, payload sweep (lower us/round-trip is better)\n\n%!";
  Printf.printf "%-34s %10s %14s\n" "backend" "size" "us/round-trip";
  run_config "Lwt + libev (bytes)" (with_libev lwt_bytes_pp);
  run_config "Lwt + libev (bigarray)" (with_libev lwt_ba_pp);
  run_config "Lwt on Eio, Level A (bytes)" (with_eio ~completion:false lwt_bytes_pp);
  run_config "Lwt on Eio, Level A (bigarray)" (with_eio ~completion:false lwt_ba_pp);
  run_config "Lwt on Eio, Level B (bytes)" (with_eio ~completion:true lwt_bytes_pp);
  run_config "Lwt on Eio, Level B (bigarray)" (with_eio ~completion:true lwt_ba_pp);
  run_config "Eio native (io_uring)" bench_eio_native
