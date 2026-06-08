(* Three-way I/O ping-pong benchmark over a socketpair (Linux only).

   The same workload — a client and a server fiber exchanging one byte N times —
   is run on three back ends:
   - classic Lwt over Lwt_unix (epoll/select);
   - Lwt_effects over Lwt_engine (readiness-based, the default back end);
   - Lwt_effects over io_uring (completion-based).

   The point is to see whether io_uring closes (or reverses) the time gap the
   readiness-based effect scheduler showed against classic Lwt. *)

let round_trips =
  if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 50_000

let measure name f =
  ignore (f ());
  Gc.full_major ();
  let w0 = Gc.minor_words () in
  let t0 = Unix.gettimeofday () in
  f ();
  let t1 = Unix.gettimeofday () in
  let w1 = Gc.minor_words () in
  let dt = t1 -. t0 in
  Printf.printf "  %-24s  %8.3f s   %7.2f us/rt   %7.2f words/rt\n%!" name dt
    (dt /. float_of_int round_trips *. 1e6)
    ((w1 -. w0) /. float_of_int round_trips)

(* classic Lwt + Lwt_unix *)
let bench_lwt () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  let byte = Bytes.make 1 'x' in
  let cbuf = Bytes.create 1 and sbuf = Bytes.create 1 in
  let open Lwt.Infix in
  let rec client n =
    if n = 0 then Lwt.return_unit
    else
      Lwt_unix.write la byte 0 1 >>= fun _ ->
      Lwt_unix.read la cbuf 0 1 >>= fun _ -> client (n - 1)
  in
  let rec server n =
    if n = 0 then Lwt.return_unit
    else
      Lwt_unix.read lb sbuf 0 1 >>= fun _ ->
      Lwt_unix.write lb byte 0 1 >>= fun _ -> server (n - 1)
  in
  Lwt_main.run (Lwt.join [ client round_trips; server round_trips ]);
  Unix.close a;
  Unix.close b

(* Lwt_effects over Lwt_engine *)
let bench_eff () =
  let open Lwt_effects in
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a;
  Unix.set_nonblock b;
  let byte = Bytes.make 1 'x' in
  let cbuf = Bytes.create 1 and sbuf = Bytes.create 1 in
  let rec client n =
    if n > 0 then begin
      ignore (Io.write a byte 0 1);
      ignore (Io.read a cbuf 0 1);
      client (n - 1)
    end
  in
  let rec server n =
    if n > 0 then begin
      ignore (Io.read b sbuf 0 1);
      ignore (Io.write b byte 0 1);
      server (n - 1)
    end
  in
  run (fun () ->
    let c = async (fun () -> client round_trips; return_unit) in
    let s = async (fun () -> server round_trips; return_unit) in
    both c s >>= fun _ -> return_unit);
  Unix.close a;
  Unix.close b

(* Lwt_effects over io_uring *)
let bench_uring () =
  let open Lwt_effects in
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let wbuf = Cstruct.create 1 in
  Cstruct.set_char wbuf 0 'x';
  let cbuf = Cstruct.create 1 and sbuf = Cstruct.create 1 in
  let rec client n =
    if n > 0 then begin
      ignore (Lwt_effects_uring.Io.write a wbuf);
      ignore (Lwt_effects_uring.Io.read a cbuf);
      client (n - 1)
    end
  in
  let rec server n =
    if n > 0 then begin
      ignore (Lwt_effects_uring.Io.read b sbuf);
      ignore (Lwt_effects_uring.Io.write b wbuf);
      server (n - 1)
    end
  in
  Lwt_effects_uring.run (fun () ->
    let c = async (fun () -> client round_trips; return_unit) in
    let s = async (fun () -> server round_trips; return_unit) in
    both c s >>= fun _ -> return_unit);
  Unix.close a;
  Unix.close b

let () =
  Printf.printf "I/O ping-pong over a socketpair (%d round trips)\n%!"
    round_trips;
  (* Optional second argument selects a single back end, for isolated profiling
     (e.g. under strace -c). *)
  match if Array.length Sys.argv > 2 then Sys.argv.(2) else "all" with
  | "lwt" -> measure "Lwt (epoll)" bench_lwt
  | "eff" -> measure "Lwt_effects (epoll)" bench_eff
  | "uring" -> measure "Lwt_effects (io_uring)" bench_uring
  | _ ->
    measure "Lwt (epoll)" bench_lwt;
    measure "Lwt_effects (epoll)" bench_eff;
    measure "Lwt_effects (io_uring)" bench_uring
