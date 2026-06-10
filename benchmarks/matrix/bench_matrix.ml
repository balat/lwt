(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* Unified comparative benchmark harness.

   The same workloads run under each configuration so that the contribution of
   each independent feature can be read off the matrix:

   - {b io_uring} vs libev/epoll      (the I/O engine),
   - {b effects} vs Lwt's monadic core (the scheduler / bind), and, within
     effects, {b semantics-preserving} vs {b semantics-breaking}:
     - {e monadic} (mbind / [Compat], [read_m]/[write_m] returning [_ t]) keeps
       Lwt's implicit concurrency — the form intended to be mergeable;
     - {e direct} (the cheap effect [bind] + [await] + [Io] returning plain
       values) is faster but changes the semantics — kept only for comparison.

   Columns:
   - [Lwt/libev]    : stock Lwt, default engine                        (baseline)
   - [Lwt/uring]    : stock Lwt, [Lwt_uring.set ()] routes I/O to io_uring
   - [effMon/epoll] : effects, semantics-preserving (monadic), readiness I/O
   - [effMon/uring] : effects, semantics-preserving (monadic), io_uring I/O
   - [effDir/epoll] : effects, semantics-breaking (direct), readiness I/O
   - [effDir/uring] : effects, semantics-breaking (direct), io_uring I/O
   - [ldir/libev]   : [Lwt_direct] (effect await over the real Lwt core)
   - [ldir/uring]   : [Lwt_direct] over the real Lwt core, io_uring engine

   Workloads: scheduling (yields, no I/O), bind chain (suspending bind, no I/O),
   ping-pong (socketpair round-trip, size sweep), keep-alive request/response
   server.

   {b Global-engine ordering.} [Lwt_uring.set ()] installs the io_uring engine as
   the process-wide [Lwt_engine] with no clean way back, so the harness runs in
   two passes: every default-engine configuration first (and the effect io_uring
   back end, which uses its own private ring), then [Lwt_uring.set ()] once, then
   the configurations that rely on the global io_uring engine ([Lwt/uring],
   [ldir/uring]). *)

(* ------------------------------------------------------------------ *)
(* Sizing (override with the "quick" argument for a fast smoke run)    *)
(* ------------------------------------------------------------------ *)

let quick = Array.exists (( = ) "quick") Sys.argv
let sc n = if quick then max 1 (n / 10) else n

(* ------------------------------------------------------------------ *)
(* Result table                                                       *)
(* ------------------------------------------------------------------ *)

let c_lwt_libev = 0
let c_lwt_uring = 1
let c_eff_mon_epoll = 2
let c_eff_mon_uring = 3
let c_eff_dir_epoll = 4
let c_eff_dir_uring = 5
let c_ldir_libev = 6
let c_ldir_uring = 7

let columns =
  [| "Lwt/libev"; "Lwt/uring"; "effMon/epoll"; "effMon/uring";
     "effDir/epoll"; "effDir/uring"; "ldir/libev"; "ldir/uring" |]

let n_columns = Array.length columns

let cells : (string, string option array) Hashtbl.t = Hashtbl.create 16
let row_order : string list ref = ref []

let row label =
  match Hashtbl.find_opt cells label with
  | Some a -> a
  | None ->
    let a = Array.make n_columns None in
    Hashtbl.add cells label a;
    row_order := label :: !row_order;
    a

let put label col value = (row label).(col) <- Some value
let na label col = (row label).(col) <- Some "—"

(* ------------------------------------------------------------------ *)
(* Measurement                                                        *)
(* ------------------------------------------------------------------ *)

let measure f =
  ignore (f ());
  Gc.full_major ();
  let w0 = Gc.minor_words () in
  let t0 = Unix.gettimeofday () in
  f ();
  let t1 = Unix.gettimeofday () in
  let w1 = Gc.minor_words () in
  (t1 -. t0, w1 -. w0)

let bench_per_op label col ~ops f =
  let dt, words = measure f in
  let ns = dt /. float_of_int ops *. 1e9 in
  Printf.printf "  %-22s  %-14s  %8.1f ns/op  %7.2f words/op\n%!" label
    columns.(col) ns (words /. float_of_int ops);
  put label col (Printf.sprintf "%.0f" ns)

let bench_rt label col ~round_trips f =
  let dt, words = measure f in
  let us = dt /. float_of_int round_trips *. 1e6 in
  Printf.printf "  %-26s  %-14s  %8.2f us/rt  %8.1f words/rt\n%!" label
    columns.(col) us (words /. float_of_int round_trips);
  put label col (Printf.sprintf "%.2f" us)

let bench_throughput label col ~total f =
  let dt, _ = measure f in
  let rps = float_of_int total /. dt in
  Printf.printf "  %-26s  %-14s  %8.0f req/s  (%d in %.3f s)\n%!" label
    columns.(col) rps total dt;
  put label col (Printf.sprintf "%.0f" rps)

(* ------------------------------------------------------------------ *)
(* I/O helpers, one set per buffer / engine / style flavour           *)
(* ------------------------------------------------------------------ *)

open Lwt.Infix
module LE = Lwt_effects
module EU = Lwt_effects_uring

(* -- Lwt_unix (bytes), monadic — Lwt and lwt_direct configs. -- *)
let rec lwt_write_all fd buf off len =
  if len = 0 then Lwt.return_unit
  else Lwt_unix.write fd buf off len >>= fun n -> lwt_write_all fd buf (off + n) (len - n)

let rec lwt_read_exactly fd buf off len =
  if len = 0 then Lwt.return_unit
  else
    Lwt_unix.read fd buf off len >>= fun n ->
    if n = 0 then Lwt.fail End_of_file else lwt_read_exactly fd buf (off + n) (len - n)

(* -- Lwt_bytes (bigarray), monadic — the copy-free Lwt_io / cohttp path. -- *)
let rec lba_write_all fd buf off len =
  if len = 0 then Lwt.return_unit
  else Lwt_bytes.write fd buf off len >>= fun n -> lba_write_all fd buf (off + n) (len - n)

let rec lba_read_exactly fd buf off len =
  if len = 0 then Lwt.return_unit
  else
    Lwt_bytes.read fd buf off len >>= fun n ->
    if n = 0 then Lwt.fail End_of_file else lba_read_exactly fd buf (off + n) (len - n)

(* -- lwt_direct (bytes / bigarray), direct style over Lwt_unix / Lwt_bytes. -- *)
let direct_write_all fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_direct.await (Lwt_unix.write fd buf !off !len) in
    off := !off + n; len := !len - n
  done

let direct_read_exactly fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_direct.await (Lwt_unix.read fd buf !off !len) in
    if n = 0 then raise End_of_file;
    off := !off + n; len := !len - n
  done

let direct_ba_write_all fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_direct.await (Lwt_bytes.write fd buf !off !len) in
    off := !off + n; len := !len - n
  done

let direct_ba_read_exactly fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_direct.await (Lwt_bytes.read fd buf !off !len) in
    if n = 0 then raise End_of_file;
    off := !off + n; len := !len - n
  done

(* -- effects, DIRECT style (semantics-breaking): plain-value Io. -- *)
let eff_write_all fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = LE.Io.write fd buf !off !len in
    off := !off + n; len := !len - n
  done

let eff_read_exactly fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = LE.Io.read fd buf !off !len in
    if n = 0 then raise End_of_file;
    off := !off + n; len := !len - n
  done

let eu_write_all fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = EU.Io.write fd (Cstruct.sub buf !off !len) in
    off := !off + n; len := !len - n
  done

let eu_read_exactly fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = EU.Io.read fd (Cstruct.sub buf !off !len) in
    if n = 0 then raise End_of_file;
    off := !off + n; len := !len - n
  done

(* -- effects, MONADIC style (semantics-preserving): [_ t] + Compat.bind. -- *)
let ( let*! ) = LE.Compat.bind

let rec eff_m_write_all fd buf off len =
  if len = 0 then LE.return_unit
  else
    let*! n = LE.Io.write_m fd buf off len in
    eff_m_write_all fd buf (off + n) (len - n)

let rec eff_m_read_exactly fd buf off len =
  if len = 0 then LE.return_unit
  else
    let*! n = LE.Io.read_m fd buf off len in
    if n = 0 then LE.fail End_of_file else eff_m_read_exactly fd buf (off + n) (len - n)

let rec eu_m_write_all fd buf off len =
  if len = 0 then LE.return_unit
  else
    let*! n = EU.Io.write_m fd (Cstruct.sub buf off len) in
    eu_m_write_all fd buf (off + n) (len - n)

let rec eu_m_read_exactly fd buf off len =
  if len = 0 then LE.return_unit
  else
    let*! n = EU.Io.read_m fd (Cstruct.sub buf off len) in
    if n = 0 then LE.fail End_of_file else eu_m_read_exactly fd buf (off + n) (len - n)

(* ------------------------------------------------------------------ *)
(* Workload: scheduling (a burst of yields, no I/O)                   *)
(* ------------------------------------------------------------------ *)

let sched_fibers = sc 1000
let sched_yields = sc 1000
let sched_ops = sched_fibers * sched_yields

let sched_lwt () =
  let rec pauses n =
    if n = 0 then Lwt.return_unit else Lwt.pause () >>= fun () -> pauses (n - 1)
  in
  Lwt_main.run (Lwt.join (List.init sched_fibers (fun _ -> pauses sched_yields)))

let sched_effects () =
  LE.run (fun () ->
    let fs =
      List.init sched_fibers (fun _ ->
        LE.async (fun () ->
          for _ = 1 to sched_yields do LE.yield () done;
          LE.return_unit))
    in
    LE.await (LE.join fs);
    LE.return_unit)

let sched_direct () =
  Lwt_main.run
    (Lwt.join
       (List.init sched_fibers (fun _ ->
            Lwt_direct.spawn (fun () ->
                for _ = 1 to sched_yields do Lwt_direct.yield () done))))

let run_scheduling () =
  let label = Printf.sprintf "scheduling %dx%d (ns/yield)" sched_fibers sched_yields in
  Printf.printf "\n== %s ==\n%!" label;
  bench_per_op label c_lwt_libev ~ops:sched_ops sched_lwt;
  (* The effect scheduler's [yield] is the same regardless of bind flavour or I/O
     engine; measured once, reported under both effect-epoll columns. *)
  let dt, words = measure sched_effects in
  let ns = dt /. float_of_int sched_ops *. 1e9 in
  Printf.printf "  %-22s  %-14s  %8.1f ns/op  %7.2f words/op\n%!" label
    "effMon=effDir" ns (words /. float_of_int sched_ops);
  put label c_eff_mon_epoll (Printf.sprintf "%.0f" ns);
  put label c_eff_dir_epoll (Printf.sprintf "%.0f" ns);
  bench_per_op label c_ldir_libev ~ops:sched_ops sched_direct;
  List.iter (na label) [ c_lwt_uring; c_eff_mon_uring; c_eff_dir_uring; c_ldir_uring ]

(* ------------------------------------------------------------------ *)
(* Workload: bind chain (suspending bind between steps, no I/O)       *)
(* ------------------------------------------------------------------ *)

let bind_len = sc 1000
let bind_repeats = sc 500
let bind_ops = bind_len * bind_repeats

let bind_lwt () =
  let rec pauses n =
    if n = 0 then Lwt.return_unit else Lwt.bind (Lwt.pause ()) (fun () -> pauses (n - 1))
  in
  for _ = 1 to bind_repeats do Lwt_main.run (pauses bind_len) done

(* Semantics-preserving (non-blocking) bind. *)
let bind_eff_mon () =
  let rec pauses n =
    if n = 0 then LE.return_unit
    else LE.Compat.bind (LE.pause ()) (fun () -> pauses (n - 1))
  in
  for _ = 1 to bind_repeats do LE.run (fun () -> pauses bind_len) done

(* Semantics-breaking (cheap, suspending) effect bind. *)
let bind_eff_dir () =
  let rec pauses n =
    if n = 0 then LE.return_unit else LE.bind (LE.pause ()) (fun () -> pauses (n - 1))
  in
  for _ = 1 to bind_repeats do LE.run (fun () -> pauses bind_len) done

let bind_direct () =
  for _ = 1 to bind_repeats do
    Lwt_main.run
      (Lwt_direct.spawn (fun () ->
           for _ = 1 to bind_len do Lwt_direct.await (Lwt.pause ()) done))
  done

let run_bind () =
  let label = Printf.sprintf "bind chain %dx%d (ns/op)" bind_len bind_repeats in
  Printf.printf "\n== %s ==\n%!" label;
  bench_per_op label c_lwt_libev ~ops:bind_ops bind_lwt;
  bench_per_op label c_eff_mon_epoll ~ops:bind_ops bind_eff_mon;
  bench_per_op label c_eff_dir_epoll ~ops:bind_ops bind_eff_dir;
  bench_per_op label c_ldir_libev ~ops:bind_ops bind_direct;
  List.iter (na label) [ c_lwt_uring; c_eff_mon_uring; c_eff_dir_uring; c_ldir_uring ]

(* ------------------------------------------------------------------ *)
(* Workload: ping-pong over a socketpair, swept over payload sizes    *)
(* ------------------------------------------------------------------ *)

let sizes = [ 1; 64; 1024; 16384; 262144 ]

let human size =
  if size >= 1024 then Printf.sprintf "%dKB" (size / 1024) else Printf.sprintf "%dB" size

let round_trips_for size =
  sc (if size <= 1024 then 20000 else if size <= 16384 then 5000 else 1000)

let pingpong_lwt_bytes size rt () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a; Unix.set_nonblock b;
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  let ba = Bytes.create size and bb = Bytes.create size in
  let rec client n =
    if n = 0 then Lwt.return_unit
    else lwt_write_all la ba 0 size >>= fun () -> lwt_read_exactly la ba 0 size >>= fun () -> client (n - 1)
  and server n =
    if n = 0 then Lwt.return_unit
    else lwt_read_exactly lb bb 0 size >>= fun () -> lwt_write_all lb bb 0 size >>= fun () -> server (n - 1)
  in
  Lwt_main.run (Lwt.join [ client rt; server rt ]);
  Unix.close a; Unix.close b

let pingpong_lwt_ba size rt () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a; Unix.set_nonblock b;
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  let ba = Lwt_bytes.create size and bb = Lwt_bytes.create size in
  let rec client n =
    if n = 0 then Lwt.return_unit
    else lba_write_all la ba 0 size >>= fun () -> lba_read_exactly la ba 0 size >>= fun () -> client (n - 1)
  and server n =
    if n = 0 then Lwt.return_unit
    else lba_read_exactly lb bb 0 size >>= fun () -> lba_write_all lb bb 0 size >>= fun () -> server (n - 1)
  in
  Lwt_main.run (Lwt.join [ client rt; server rt ]);
  Unix.close a; Unix.close b

let pingpong_direct_bytes size rt () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a; Unix.set_nonblock b;
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  let ba = Bytes.create size and bb = Bytes.create size in
  let client () = for _ = 1 to rt do direct_write_all la ba 0 size; direct_read_exactly la ba 0 size done in
  let server () = for _ = 1 to rt do direct_read_exactly lb bb 0 size; direct_write_all lb bb 0 size done in
  Lwt_main.run (Lwt.join [ Lwt_direct.spawn client; Lwt_direct.spawn server ]);
  Unix.close a; Unix.close b

let pingpong_direct_ba size rt () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a; Unix.set_nonblock b;
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  let ba = Lwt_bytes.create size and bb = Lwt_bytes.create size in
  let client () = for _ = 1 to rt do direct_ba_write_all la ba 0 size; direct_ba_read_exactly la ba 0 size done in
  let server () = for _ = 1 to rt do direct_ba_read_exactly lb bb 0 size; direct_ba_write_all lb bb 0 size done in
  Lwt_main.run (Lwt.join [ Lwt_direct.spawn client; Lwt_direct.spawn server ]);
  Unix.close a; Unix.close b

(* effects, direct style (plain-value Io). *)
let pingpong_eff_dir size rt () =
  let open Lwt_effects in
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a; Unix.set_nonblock b;
  let ba = Bytes.create size and bb = Bytes.create size in
  let rec client n = if n > 0 then (eff_write_all a ba 0 size; eff_read_exactly a ba 0 size; client (n - 1)) in
  let rec server n = if n > 0 then (eff_read_exactly b bb 0 size; eff_write_all b bb 0 size; server (n - 1)) in
  run (fun () ->
    let c = async (fun () -> client rt; return_unit) in
    let s = async (fun () -> server rt; return_unit) in
    await (join [ c; s ]); return_unit);
  Unix.close a; Unix.close b

let pingpong_eff_dir_uring size rt () =
  let open Lwt_effects in
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let ca = Cstruct.create size and cb = Cstruct.create size in
  let rec client n = if n > 0 then (eu_write_all a ca 0 size; eu_read_exactly a ca 0 size; client (n - 1)) in
  let rec server n = if n > 0 then (eu_read_exactly b cb 0 size; eu_write_all b cb 0 size; server (n - 1)) in
  EU.run (fun () ->
    let c = async (fun () -> client rt; return_unit) in
    let s = async (fun () -> server rt; return_unit) in
    await (join [ c; s ]); return_unit);
  Unix.close a; Unix.close b

(* effects, monadic style (semantics-preserving, [_ t] + Compat.bind). *)
let pingpong_eff_mon size rt () =
  let open Lwt_effects in
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a; Unix.set_nonblock b;
  let ba = Bytes.create size and bb = Bytes.create size in
  let rec client n =
    if n = 0 then return_unit
    else Compat.bind (eff_m_write_all a ba 0 size) (fun () ->
         Compat.bind (eff_m_read_exactly a ba 0 size) (fun () -> client (n - 1)))
  in
  let rec server n =
    if n = 0 then return_unit
    else Compat.bind (eff_m_read_exactly b bb 0 size) (fun () ->
         Compat.bind (eff_m_write_all b bb 0 size) (fun () -> server (n - 1)))
  in
  run (fun () ->
    let c = async (fun () -> client rt) in
    let s = async (fun () -> server rt) in
    await (join [ c; s ]); return_unit);
  Unix.close a; Unix.close b

let pingpong_eff_mon_uring size rt () =
  let open Lwt_effects in
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let ca = Cstruct.create size and cb = Cstruct.create size in
  let rec client n =
    if n = 0 then return_unit
    else Compat.bind (eu_m_write_all a ca 0 size) (fun () ->
         Compat.bind (eu_m_read_exactly a ca 0 size) (fun () -> client (n - 1)))
  in
  let rec server n =
    if n = 0 then return_unit
    else Compat.bind (eu_m_read_exactly b cb 0 size) (fun () ->
         Compat.bind (eu_m_write_all b cb 0 size) (fun () -> server (n - 1)))
  in
  EU.run (fun () ->
    let c = async (fun () -> client rt) in
    let s = async (fun () -> server rt) in
    await (join [ c; s ]); return_unit);
  Unix.close a; Unix.close b

let bytes_label size = Printf.sprintf "ping-pong %s bytes (us/rt)" (human size)
let ba_label size = Printf.sprintf "ping-pong %s bigarray (us/rt)" (human size)

let run_pingpong_phase_a () =
  Printf.printf "\n== ping-pong socketpair (us/round-trip) ==\n%!";
  List.iter
    (fun size ->
      let rt = round_trips_for size in
      let bl = bytes_label size in
      bench_rt bl c_lwt_libev ~round_trips:rt (pingpong_lwt_bytes size rt);
      bench_rt bl c_eff_mon_epoll ~round_trips:rt (pingpong_eff_mon size rt);
      bench_rt bl c_eff_mon_uring ~round_trips:rt (pingpong_eff_mon_uring size rt);
      bench_rt bl c_eff_dir_epoll ~round_trips:rt (pingpong_eff_dir size rt);
      bench_rt bl c_eff_dir_uring ~round_trips:rt (pingpong_eff_dir_uring size rt);
      bench_rt bl c_ldir_libev ~round_trips:rt (pingpong_direct_bytes size rt);
      let al = ba_label size in
      bench_rt al c_lwt_libev ~round_trips:rt (pingpong_lwt_ba size rt);
      bench_rt al c_ldir_libev ~round_trips:rt (pingpong_direct_ba size rt))
    sizes

let run_pingpong_phase_b () =
  List.iter
    (fun size ->
      let rt = round_trips_for size in
      bench_rt (bytes_label size) c_lwt_uring ~round_trips:rt (pingpong_lwt_bytes size rt);
      bench_rt (bytes_label size) c_ldir_uring ~round_trips:rt (pingpong_direct_bytes size rt);
      bench_rt (ba_label size) c_lwt_uring ~round_trips:rt (pingpong_lwt_ba size rt);
      bench_rt (ba_label size) c_ldir_uring ~round_trips:rt (pingpong_direct_ba size rt))
    sizes

(* ------------------------------------------------------------------ *)
(* Workload: keep-alive request/response server (raw fixed-size frames) *)
(* ------------------------------------------------------------------ *)

let srv_connections = sc 50
let srv_requests = sc 400
let req_len = 64
let resp_len = 64
let srv_total = srv_connections * srv_requests

let server_lwt () =
  let lsock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Lwt_main.run
    ( Lwt_unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0)) >>= fun () ->
      Lwt_unix.listen lsock srv_connections;
      let addr = Lwt_unix.getsockname lsock in
      let serve fd =
        let reqbuf = Bytes.create req_len and resp = Bytes.create resp_len in
        let rec loop n =
          if n = 0 then Lwt.return_unit
          else lwt_read_exactly fd reqbuf 0 req_len >>= fun () ->
               lwt_write_all fd resp 0 resp_len >>= fun () -> loop (n - 1)
        in
        loop srv_requests >>= fun () -> Lwt_unix.close fd
      in
      let server =
        let rec accept_loop rem acc =
          if rem = 0 then Lwt.return acc
          else Lwt_unix.accept lsock >>= fun (fd, _) -> accept_loop (rem - 1) (serve fd :: acc)
        in
        accept_loop srv_connections [] >>= Lwt.join
      in
      let client () =
        let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
        Lwt_unix.connect fd addr >>= fun () ->
        let req = Bytes.create req_len and respbuf = Bytes.create resp_len in
        let rec loop n =
          if n = 0 then Lwt.return_unit
          else lwt_write_all fd req 0 req_len >>= fun () ->
               lwt_read_exactly fd respbuf 0 resp_len >>= fun () -> loop (n - 1)
        in
        loop srv_requests >>= fun () -> Lwt_unix.close fd
      in
      Lwt.join (server :: List.init srv_connections (fun _ -> client ())) );
  Lwt_main.run (Lwt_unix.close lsock)

let server_direct () =
  let lsock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Lwt_main.run
    ( Lwt_unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0)) >>= fun () ->
      Lwt_unix.listen lsock srv_connections;
      let addr = Lwt_unix.getsockname lsock in
      let serve fd () =
        let reqbuf = Bytes.create req_len and resp = Bytes.create resp_len in
        for _ = 1 to srv_requests do
          direct_read_exactly fd reqbuf 0 req_len;
          direct_write_all fd resp 0 resp_len
        done;
        Lwt_direct.await (Lwt_unix.close fd)
      in
      let server () =
        let handlers = ref [] in
        for _ = 1 to srv_connections do
          let fd, _ = Lwt_direct.await (Lwt_unix.accept lsock) in
          handlers := Lwt_direct.spawn (serve fd) :: !handlers
        done;
        List.iter (fun p -> Lwt_direct.await p) !handlers
      in
      let client () =
        let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
        Lwt_direct.await (Lwt_unix.connect fd addr);
        let req = Bytes.create req_len and respbuf = Bytes.create resp_len in
        for _ = 1 to srv_requests do
          direct_write_all fd req 0 req_len;
          direct_read_exactly fd respbuf 0 resp_len
        done;
        Lwt_direct.await (Lwt_unix.close fd)
      in
      Lwt.join
        (Lwt_direct.spawn server :: List.init srv_connections (fun _ -> Lwt_direct.spawn client)) );
  Lwt_main.run (Lwt_unix.close lsock)

(* effects, direct style. *)
let server_eff_dir () =
  let open Lwt_effects in
  let lsock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
  Unix.listen lsock srv_connections;
  Unix.set_nonblock lsock;
  let addr = Unix.getsockname lsock in
  run (fun () ->
    let serve fd () =
      Unix.set_nonblock fd;
      let reqbuf = Bytes.create req_len and resp = Bytes.create resp_len in
      for _ = 1 to srv_requests do
        eff_read_exactly fd reqbuf 0 req_len;
        eff_write_all fd resp 0 resp_len
      done;
      Unix.close fd; return_unit
    in
    let server () =
      let handlers = ref [] in
      for _ = 1 to srv_connections do
        let fd, _ = Io.accept lsock in
        handlers := async (serve fd) :: !handlers
      done;
      await (join !handlers); return_unit
    in
    let client () =
      let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Unix.set_nonblock fd; Io.connect fd addr;
      let req = Bytes.create req_len and respbuf = Bytes.create resp_len in
      for _ = 1 to srv_requests do
        eff_write_all fd req 0 req_len;
        eff_read_exactly fd respbuf 0 resp_len
      done;
      Unix.close fd; return_unit
    in
    let sp = async server in
    let cps = List.init srv_connections (fun _ -> async client) in
    await (join (sp :: cps)); return_unit);
  Unix.close lsock

let server_eff_dir_uring () =
  let open Lwt_effects in
  let lsock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
  Unix.listen lsock srv_connections;
  Unix.set_nonblock lsock;
  let addr = Unix.getsockname lsock in
  EU.run (fun () ->
    let serve fd () =
      Unix.set_nonblock fd;
      let reqbuf = Cstruct.create req_len and resp = Cstruct.create resp_len in
      for _ = 1 to srv_requests do
        eu_read_exactly fd reqbuf 0 req_len;
        eu_write_all fd resp 0 resp_len
      done;
      Unix.close fd; return_unit
    in
    let server () =
      let handlers = ref [] in
      for _ = 1 to srv_connections do
        let fd, _ = EU.Io.accept lsock in
        handlers := async (serve fd) :: !handlers
      done;
      await (join !handlers); return_unit
    in
    let client () =
      let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Unix.set_nonblock fd; EU.Io.connect fd addr;
      let req = Cstruct.create req_len and respbuf = Cstruct.create resp_len in
      for _ = 1 to srv_requests do
        eu_write_all fd req 0 req_len;
        eu_read_exactly fd respbuf 0 resp_len
      done;
      Unix.close fd; return_unit
    in
    let sp = async server in
    let cps = List.init srv_connections (fun _ -> async client) in
    await (join (sp :: cps)); return_unit);
  Unix.close lsock

(* effects, monadic style (semantics-preserving). *)
let server_eff_mon () =
  let open Lwt_effects in
  let lsock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
  Unix.listen lsock srv_connections;
  Unix.set_nonblock lsock;
  let addr = Unix.getsockname lsock in
  run (fun () ->
    let serve fd () =
      Unix.set_nonblock fd;
      let reqbuf = Bytes.create req_len and resp = Bytes.create resp_len in
      let rec loop n =
        if n = 0 then return_unit
        else Compat.bind (eff_m_read_exactly fd reqbuf 0 req_len) (fun () ->
             Compat.bind (eff_m_write_all fd resp 0 resp_len) (fun () -> loop (n - 1)))
      in
      Compat.bind (loop srv_requests) (fun () -> Unix.close fd; return_unit)
    in
    let server () =
      let rec accept_loop rem acc =
        if rem = 0 then return acc
        else Compat.bind (Io.accept_m lsock) (fun (fd, _) ->
             accept_loop (rem - 1) (async (serve fd) :: acc))
      in
      (* Monadic style: return [join hs] rather than [await]ing it — an [await]
         in an mbind continuation runs outside the fiber handler. *)
      Compat.bind (accept_loop srv_connections []) join
    in
    let client () =
      let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Unix.set_nonblock fd;
      Compat.bind (Io.connect_m fd addr) (fun () ->
        let req = Bytes.create req_len and respbuf = Bytes.create resp_len in
        let rec loop n =
          if n = 0 then return_unit
          else Compat.bind (eff_m_write_all fd req 0 req_len) (fun () ->
               Compat.bind (eff_m_read_exactly fd respbuf 0 resp_len) (fun () -> loop (n - 1)))
        in
        Compat.bind (loop srv_requests) (fun () -> Unix.close fd; return_unit))
    in
    let sp = async server in
    let cps = List.init srv_connections (fun _ -> async client) in
    await (join (sp :: cps)); return_unit);
  Unix.close lsock

let server_eff_mon_uring () =
  let open Lwt_effects in
  let lsock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
  Unix.listen lsock srv_connections;
  Unix.set_nonblock lsock;
  let addr = Unix.getsockname lsock in
  EU.run (fun () ->
    let serve fd () =
      Unix.set_nonblock fd;
      let reqbuf = Cstruct.create req_len and resp = Cstruct.create resp_len in
      let rec loop n =
        if n = 0 then return_unit
        else Compat.bind (eu_m_read_exactly fd reqbuf 0 req_len) (fun () ->
             Compat.bind (eu_m_write_all fd resp 0 resp_len) (fun () -> loop (n - 1)))
      in
      Compat.bind (loop srv_requests) (fun () -> Unix.close fd; return_unit)
    in
    let server () =
      let rec accept_loop rem acc =
        if rem = 0 then return acc
        else Compat.bind (EU.Io.accept_m lsock) (fun (fd, _) ->
             accept_loop (rem - 1) (async (serve fd) :: acc))
      in
      Compat.bind (accept_loop srv_connections []) join
    in
    let client () =
      let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Unix.set_nonblock fd;
      Compat.bind (EU.Io.connect_m fd addr) (fun () ->
        let req = Cstruct.create req_len and respbuf = Cstruct.create resp_len in
        let rec loop n =
          if n = 0 then return_unit
          else Compat.bind (eu_m_write_all fd req 0 req_len) (fun () ->
               Compat.bind (eu_m_read_exactly fd respbuf 0 resp_len) (fun () -> loop (n - 1)))
        in
        Compat.bind (loop srv_requests) (fun () -> Unix.close fd; return_unit))
    in
    let sp = async server in
    let cps = List.init srv_connections (fun _ -> async client) in
    await (join (sp :: cps)); return_unit);
  Unix.close lsock

let server_label =
  Printf.sprintf "server %dx%d keep-alive (req/s)" srv_connections srv_requests

let run_server_phase_a () =
  Printf.printf "\n== %s ==\n%!" server_label;
  bench_throughput server_label c_lwt_libev ~total:srv_total server_lwt;
  bench_throughput server_label c_eff_mon_epoll ~total:srv_total server_eff_mon;
  bench_throughput server_label c_eff_mon_uring ~total:srv_total server_eff_mon_uring;
  bench_throughput server_label c_eff_dir_epoll ~total:srv_total server_eff_dir;
  bench_throughput server_label c_eff_dir_uring ~total:srv_total server_eff_dir_uring;
  bench_throughput server_label c_ldir_libev ~total:srv_total server_direct

let run_server_phase_b () =
  bench_throughput server_label c_lwt_uring ~total:srv_total server_lwt;
  bench_throughput server_label c_ldir_uring ~total:srv_total server_direct

(* ------------------------------------------------------------------ *)
(* Rendering                                                          *)
(* ------------------------------------------------------------------ *)

let render () =
  let rows = List.rev !row_order in
  let label_w = List.fold_left (fun acc l -> max acc (String.length l)) 5 rows in
  let col_w i = max 8 (String.length columns.(i)) in
  let cell label i = match (row label).(i) with Some s -> s | None -> "·" in
  let pad s w = s ^ String.make (max 0 (w - String.length s)) ' ' in
  Printf.printf "\n\n# Comparative matrix%s\n\n"
    (if quick then "  (quick mode — indicative only)" else "");
  Printf.printf "| %s " (pad "workload" label_w);
  Array.iteri (fun i c -> Printf.printf "| %s " (pad c (col_w i))) columns;
  print_string "|\n";
  Printf.printf "|%s" (String.make (label_w + 2) '-');
  Array.iteri (fun i _ -> Printf.printf "|%s" (String.make (col_w i + 2) '-')) columns;
  print_string "|\n";
  List.iter
    (fun label ->
      Printf.printf "| %s " (pad label label_w);
      for i = 0 to n_columns - 1 do
        Printf.printf "| %s " (pad (cell label i) (col_w i))
      done;
      print_string "|\n")
    rows;
  print_string
    "\n· = not measured;  — = not applicable (io_uring is irrelevant with no I/O).\n\
     Lower is better except the server row (req/s, higher is better).\n\
     effMon = semantics-preserving effects (monadic mbind/Compat, read_m/write_m);\n\
     effDir = semantics-breaking effects (cheap bind + direct await/Io).\n\
     ping-pong bytes/bigarray = the Lwt & ldir buffer API; the effect columns use\n\
     bytes (epoll) / Cstruct (uring), reported on the bytes rows. The bytes io_uring\n\
     path copies per I/O (regresses at large sizes); bigarray (Lwt_io/cohttp) is\n\
     copy-free.\n"

let () =
  Printf.printf "Unified comparative benchmark (Lwt / io_uring / effects)%s\n%!"
    (if quick then "  [quick mode]" else "");
  run_scheduling ();
  run_bind ();
  run_pingpong_phase_a ();
  run_server_phase_a ();
  Printf.printf "\n-- installing the global io_uring engine (Lwt_uring.set) --\n%!";
  Lwt_uring.set ();
  run_pingpong_phase_b ();
  run_server_phase_b ();
  render ()
