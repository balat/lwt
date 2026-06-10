(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* Unified comparative benchmark harness.

   The same set of workloads is run under each available configuration, and the
   results are collected into one matrix. The point is to separate the two
   independent contributions being studied:

   - {b io_uring} (a faster I/O engine), and
   - {b effects} (a cheaper scheduler / bind),

   by holding everything else constant across columns.

   Columns (configurations):
   - [Lwt+libev]       : stock Lwt on the default engine                (baseline)
   - [Lwt+uring]       : stock Lwt, [Lwt_uring.set ()] routes I/O to io_uring
   - [lwt_eff]         : the effect scheduler, readiness (epoll) I/O
   - [lwt_eff+uring]   : the effect scheduler, completion-based io_uring I/O
   - [direct+libev]    : [Lwt_direct] (effect await over the real Lwt core)
   - [direct+uring]    : [Lwt_direct] over the real Lwt core, io_uring engine

   Rows (workloads):
   - scheduling : a burst of yields, no I/O  (isolates the scheduler)
   - bind       : a chain of suspending binds, no I/O  (isolates bind/suspension)
   - ping-pong  : a socketpair round-trip, swept over payload sizes  (I/O latency)
   - server     : a keep-alive request/response server  (I/O under concurrency)

   {b Global-engine ordering.} [Lwt_uring.set ()] installs the io_uring engine as
   the process-wide [Lwt_engine] and there is no clean way back, so the harness
   runs in two passes: everything that needs the default engine (libev/epoll, and
   the engine-independent effect scheduler) first, then [Lwt_uring.set ()] once,
   then the configurations that rely on the global io_uring engine. The
   [lwt_effects] io_uring back end uses its own private ring (independent of the
   global engine), so it is measured in the first pass. *)

(* ------------------------------------------------------------------ *)
(* Sizing (override with the "quick" argument for a fast smoke run)    *)
(* ------------------------------------------------------------------ *)

let quick = Array.exists (( = ) "quick") Sys.argv

(* Scale a workload count down in quick mode. *)
let sc n = if quick then max 1 (n / 10) else n

(* ------------------------------------------------------------------ *)
(* Result table                                                       *)
(* ------------------------------------------------------------------ *)

(* Column order. *)
let columns =
  [| "Lwt+libev"; "Lwt+uring"; "lwt_eff"; "lwt_eff+uring";
     "direct+libev"; "direct+uring" |]

let n_columns = Array.length columns

(* row label -> per-column formatted value (or [None] = not filled). *)
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

(* Mark a cell as not-applicable, with a short reason printed once. *)
let na label col = (row label).(col) <- Some "—"

(* ------------------------------------------------------------------ *)
(* Measurement                                                        *)
(* ------------------------------------------------------------------ *)

(* Run [f] once to warm up, then once measured; return (seconds, minor words). *)
let measure f =
  ignore (f ());
  Gc.full_major ();
  let w0 = Gc.minor_words () in
  let t0 = Unix.gettimeofday () in
  f ();
  let t1 = Unix.gettimeofday () in
  let w1 = Gc.minor_words () in
  (t1 -. t0, w1 -. w0)

(* Measure a per-operation workload: report ns/op and words/op, store ns/op. *)
let bench_per_op label col ~ops f =
  let dt, words = measure f in
  let ns = dt /. float_of_int ops *. 1e9 in
  let w = words /. float_of_int ops in
  Printf.printf "  %-14s  %-20s  %8.1f ns/op  %7.2f words/op\n%!" label
    columns.(col) ns w;
  put label col (Printf.sprintf "%.0f" ns)

(* Measure an I/O round-trip workload: report µs/round-trip, store it. *)
let bench_rt label col ~round_trips f =
  let dt, words = measure f in
  let us = dt /. float_of_int round_trips *. 1e6 in
  let w = words /. float_of_int round_trips in
  Printf.printf "  %-14s  %-20s  %8.2f us/rt  %8.1f words/rt\n%!" label
    columns.(col) us w;
  put label col (Printf.sprintf "%.2f" us)

(* Measure a throughput workload: report req/s, store it. *)
let bench_throughput label col ~total f =
  let dt, _ = measure f in
  let rps = float_of_int total /. dt in
  Printf.printf "  %-14s  %-20s  %8.0f req/s  (%d in %.3f s)\n%!" label
    columns.(col) rps total dt;
  put label col (Printf.sprintf "%.0f" rps)

(* ------------------------------------------------------------------ *)
(* I/O helpers, one set per buffer/engine flavour                     *)
(* ------------------------------------------------------------------ *)

(* Lwt_unix (bytes), monadic — used by the Lwt and lwt_direct configs. *)
open Lwt.Infix

let rec lwt_write_all fd buf off len =
  if len = 0 then Lwt.return_unit
  else Lwt_unix.write fd buf off len >>= fun n -> lwt_write_all fd buf (off + n) (len - n)

let rec lwt_read_exactly fd buf off len =
  if len = 0 then Lwt.return_unit
  else
    Lwt_unix.read fd buf off len >>= fun n ->
    if n = 0 then Lwt.fail End_of_file
    else lwt_read_exactly fd buf (off + n) (len - n)

(* Lwt_bytes (bigarray), monadic — the copy-free API that Lwt_io / cohttp use.
   Lwt_bytes.{read,write} call Lwt_unix.{read,write}_bigarray, the functions the
   io_uring backend routes, so this exercises the transparent routing with no
   bytes<->Cstruct copy (unlike the [Lwt_unix.read/write] bytes path above). *)
let rec lba_write_all fd buf off len =
  if len = 0 then Lwt.return_unit
  else Lwt_bytes.write fd buf off len >>= fun n -> lba_write_all fd buf (off + n) (len - n)

let rec lba_read_exactly fd buf off len =
  if len = 0 then Lwt.return_unit
  else
    Lwt_bytes.read fd buf off len >>= fun n ->
    if n = 0 then Lwt.fail End_of_file
    else lba_read_exactly fd buf (off + n) (len - n)

(* lwt_direct (bigarray), direct style over Lwt_bytes promises. *)
let direct_ba_write_all fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_direct.await (Lwt_bytes.write fd buf !off !len) in
    off := !off + n;
    len := !len - n
  done

let direct_ba_read_exactly fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_direct.await (Lwt_bytes.read fd buf !off !len) in
    if n = 0 then raise End_of_file;
    off := !off + n;
    len := !len - n
  done

(* lwt_direct (bytes), direct style over Lwt_unix promises. *)
let direct_write_all fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_direct.await (Lwt_unix.write fd buf !off !len) in
    off := !off + n;
    len := !len - n
  done

let direct_read_exactly fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_direct.await (Lwt_unix.read fd buf !off !len) in
    if n = 0 then raise End_of_file;
    off := !off + n;
    len := !len - n
  done

(* lwt_effects readiness Io (bytes), direct style. *)
let eff_write_all fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_effects.Io.write fd buf !off !len in
    off := !off + n;
    len := !len - n
  done

let eff_read_exactly fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = Lwt_effects.Io.read fd buf !off !len in
    if n = 0 then raise End_of_file;
    off := !off + n;
    len := !len - n
  done

(* lwt_effects io_uring Io (Cstruct), direct style. *)
module EU = Lwt_effects_uring

let eu_write_all fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = EU.Io.write fd (Cstruct.sub buf !off !len) in
    off := !off + n;
    len := !len - n
  done

let eu_read_exactly fd buf off len =
  let off = ref off and len = ref len in
  while !len > 0 do
    let n = EU.Io.read fd (Cstruct.sub buf !off !len) in
    if n = 0 then raise End_of_file;
    off := !off + n;
    len := !len - n
  done

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
  Lwt_effects.run (fun () ->
    let fs =
      List.init sched_fibers (fun _ ->
        Lwt_effects.async (fun () ->
          for _ = 1 to sched_yields do Lwt_effects.yield () done;
          Lwt_effects.return_unit))
    in
    Lwt_effects.await (Lwt_effects.join fs);
    Lwt_effects.return_unit)

let sched_effects_uring () =
  EU.run (fun () ->
    let fs =
      List.init sched_fibers (fun _ ->
        Lwt_effects.async (fun () ->
          for _ = 1 to sched_yields do Lwt_effects.yield () done;
          Lwt_effects.return_unit))
    in
    Lwt_effects.await (Lwt_effects.join fs);
    Lwt_effects.return_unit)

let sched_direct () =
  Lwt_main.run
    (Lwt.join
       (List.init sched_fibers (fun _ ->
            Lwt_direct.spawn (fun () ->
                for _ = 1 to sched_yields do Lwt_direct.yield () done))))

let run_scheduling () =
  let label =
    Printf.sprintf "scheduling %dx%d (ns/yield)" sched_fibers sched_yields
  in
  Printf.printf "\n== %s ==\n%!" label;
  bench_per_op label 0 ~ops:sched_ops sched_lwt;
  na label 1;
  bench_per_op label 2 ~ops:sched_ops sched_effects;
  bench_per_op label 3 ~ops:sched_ops sched_effects_uring;
  bench_per_op label 4 ~ops:sched_ops sched_direct;
  na label 5

(* ------------------------------------------------------------------ *)
(* Workload: bind chain (suspending bind between steps, no I/O)       *)
(* ------------------------------------------------------------------ *)

let bind_len = sc 1000
let bind_repeats = sc 500
let bind_ops = bind_len * bind_repeats

let bind_lwt () =
  let rec pauses n =
    if n = 0 then Lwt.return_unit
    else Lwt.bind (Lwt.pause ()) (fun () -> pauses (n - 1))
  in
  for _ = 1 to bind_repeats do
    Lwt_main.run (pauses bind_len)
  done

(* The semantics-preserving (non-blocking) bind of lwt_effects. *)
let bind_effects () =
  let rec pauses n =
    if n = 0 then Lwt_effects.return_unit
    else Lwt_effects.Compat.bind (Lwt_effects.pause ()) (fun () -> pauses (n - 1))
  in
  for _ = 1 to bind_repeats do
    Lwt_effects.run (fun () -> pauses bind_len)
  done

(* Direct-style equivalent: one suspension per step via await. *)
let bind_direct () =
  for _ = 1 to bind_repeats do
    Lwt_main.run
      (Lwt_direct.spawn (fun () ->
           for _ = 1 to bind_len do Lwt_direct.await (Lwt.pause ()) done))
  done

let run_bind () =
  let label = Printf.sprintf "bind chain %dx%d (ns/op)" bind_len bind_repeats in
  Printf.printf "\n== %s ==\n%!" label;
  bench_per_op label 0 ~ops:bind_ops bind_lwt;
  na label 1;
  bench_per_op label 2 ~ops:bind_ops bind_effects;
  na label 3;
  bench_per_op label 4 ~ops:bind_ops bind_direct;
  na label 5

(* ------------------------------------------------------------------ *)
(* Workload: ping-pong over a socketpair, swept over payload sizes    *)
(* ------------------------------------------------------------------ *)

let sizes = [ 1; 64; 1024; 16384; 262144 ]

let human size =
  if size >= 1024 then Printf.sprintf "%dKB" (size / 1024)
  else Printf.sprintf "%dB" size

let round_trips_for size =
  sc
    (if size <= 1024 then 20000
     else if size <= 16384 then 5000
     else 1000)

let pingpong_lwt size rt () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a;
  Unix.set_nonblock b;
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  let ba = Bytes.create size and bb = Bytes.create size in
  let rec client n =
    if n = 0 then Lwt.return_unit
    else
      lwt_write_all la ba 0 size >>= fun () ->
      lwt_read_exactly la ba 0 size >>= fun () -> client (n - 1)
  in
  let rec server n =
    if n = 0 then Lwt.return_unit
    else
      lwt_read_exactly lb bb 0 size >>= fun () ->
      lwt_write_all lb bb 0 size >>= fun () -> server (n - 1)
  in
  Lwt_main.run (Lwt.join [ client rt; server rt ]);
  Unix.close a;
  Unix.close b

let pingpong_direct size rt () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a;
  Unix.set_nonblock b;
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  let ba = Bytes.create size and bb = Bytes.create size in
  let client () =
    for _ = 1 to rt do
      direct_write_all la ba 0 size;
      direct_read_exactly la ba 0 size
    done
  in
  let server () =
    for _ = 1 to rt do
      direct_read_exactly lb bb 0 size;
      direct_write_all lb bb 0 size
    done
  in
  Lwt_main.run (Lwt.join [ Lwt_direct.spawn client; Lwt_direct.spawn server ]);
  Unix.close a;
  Unix.close b

(* Bigarray (copy-free) variants of the Lwt and lwt_direct ping-pong, to show the
   transparent routing without the bytes<->Cstruct copy (the real cohttp/Lwt_io
   path). *)
let pingpong_lwt_ba size rt () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a;
  Unix.set_nonblock b;
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  let ba = Lwt_bytes.create size and bb = Lwt_bytes.create size in
  let rec client n =
    if n = 0 then Lwt.return_unit
    else
      lba_write_all la ba 0 size >>= fun () ->
      lba_read_exactly la ba 0 size >>= fun () -> client (n - 1)
  in
  let rec server n =
    if n = 0 then Lwt.return_unit
    else
      lba_read_exactly lb bb 0 size >>= fun () ->
      lba_write_all lb bb 0 size >>= fun () -> server (n - 1)
  in
  Lwt_main.run (Lwt.join [ client rt; server rt ]);
  Unix.close a;
  Unix.close b

let pingpong_direct_ba size rt () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a;
  Unix.set_nonblock b;
  let la = Lwt_unix.of_unix_file_descr a and lb = Lwt_unix.of_unix_file_descr b in
  let ba = Lwt_bytes.create size and bb = Lwt_bytes.create size in
  let client () =
    for _ = 1 to rt do
      direct_ba_write_all la ba 0 size;
      direct_ba_read_exactly la ba 0 size
    done
  in
  let server () =
    for _ = 1 to rt do
      direct_ba_read_exactly lb bb 0 size;
      direct_ba_write_all lb bb 0 size
    done
  in
  Lwt_main.run (Lwt.join [ Lwt_direct.spawn client; Lwt_direct.spawn server ]);
  Unix.close a;
  Unix.close b

let pingpong_effects size rt () =
  let open Lwt_effects in
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a;
  Unix.set_nonblock b;
  let ba = Bytes.create size and bb = Bytes.create size in
  let rec client n =
    if n > 0 then begin
      eff_write_all a ba 0 size;
      eff_read_exactly a ba 0 size;
      client (n - 1)
    end
  in
  let rec server n =
    if n > 0 then begin
      eff_read_exactly b bb 0 size;
      eff_write_all b bb 0 size;
      server (n - 1)
    end
  in
  run (fun () ->
    let c = async (fun () -> client rt; return_unit) in
    let s = async (fun () -> server rt; return_unit) in
    await (join [ c; s ]);
    return_unit);
  Unix.close a;
  Unix.close b

let pingpong_effects_uring size rt () =
  let open Lwt_effects in
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let ca = Cstruct.create size and cb = Cstruct.create size in
  let rec client n =
    if n > 0 then begin
      eu_write_all a ca 0 size;
      eu_read_exactly a ca 0 size;
      client (n - 1)
    end
  in
  let rec server n =
    if n > 0 then begin
      eu_read_exactly b cb 0 size;
      eu_write_all b cb 0 size;
      server (n - 1)
    end
  in
  EU.run (fun () ->
    let c = async (fun () -> client rt; return_unit) in
    let s = async (fun () -> server rt; return_unit) in
    await (join [ c; s ]);
    return_unit);
  Unix.close a;
  Unix.close b

let bytes_label size = Printf.sprintf "ping-pong %s bytes (us/rt)" (human size)
let ba_label size = Printf.sprintf "ping-pong %s bigarray (us/rt)" (human size)

(* Phase A: configurations on the default engine + the private io_uring ring. *)
let run_pingpong_phase_a () =
  Printf.printf "\n== ping-pong socketpair (us/round-trip) ==\n%!";
  (* The Lwt / lwt_direct columns use the bytes [Lwt_unix.read/write] API; the
     effect columns natively use Cstruct (copy-free). *)
  List.iter
    (fun size ->
      let rt = round_trips_for size in
      let label = bytes_label size in
      bench_rt label 0 ~round_trips:rt (pingpong_lwt size rt);
      bench_rt label 2 ~round_trips:rt (pingpong_effects size rt);
      bench_rt label 3 ~round_trips:rt (pingpong_effects_uring size rt);
      bench_rt label 4 ~round_trips:rt (pingpong_direct size rt))
    sizes;
  (* Bigarray (copy-free) variant for the Lwt / lwt_direct columns — the real
     Lwt_io / cohttp path. Effect columns are already Cstruct (see bytes rows). *)
  List.iter
    (fun size ->
      let rt = round_trips_for size in
      let label = ba_label size in
      bench_rt label 0 ~round_trips:rt (pingpong_lwt_ba size rt);
      bench_rt label 4 ~round_trips:rt (pingpong_direct_ba size rt))
    sizes

(* Phase B: configurations on the global io_uring engine (after Lwt_uring.set). *)
let run_pingpong_phase_b () =
  List.iter
    (fun size ->
      let rt = round_trips_for size in
      bench_rt (bytes_label size) 1 ~round_trips:rt (pingpong_lwt size rt);
      bench_rt (bytes_label size) 5 ~round_trips:rt (pingpong_direct size rt))
    sizes;
  List.iter
    (fun size ->
      let rt = round_trips_for size in
      bench_rt (ba_label size) 1 ~round_trips:rt (pingpong_lwt_ba size rt);
      bench_rt (ba_label size) 5 ~round_trips:rt (pingpong_direct_ba size rt))
    sizes

(* ------------------------------------------------------------------ *)
(* Workload: keep-alive request/response server (raw fixed-size frames) *)
(* ------------------------------------------------------------------ *)

let srv_connections = sc 50
let srv_requests = sc 400
let req_len = 64
let resp_len = 64
let srv_total = srv_connections * srv_requests

(* Lwt_unix, monadic — used by Lwt and (via await wrappers) shared structure. *)
let server_lwt () =
  let lsock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Lwt_main.run
    ( Lwt_unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0))
      >>= fun () ->
      Lwt_unix.listen lsock srv_connections;
      let addr = Lwt_unix.getsockname lsock in
      let serve fd =
        let reqbuf = Bytes.create req_len and resp = Bytes.create resp_len in
        let rec loop n =
          if n = 0 then Lwt.return_unit
          else
            lwt_read_exactly fd reqbuf 0 req_len >>= fun () ->
            lwt_write_all fd resp 0 resp_len >>= fun () -> loop (n - 1)
        in
        loop srv_requests >>= fun () -> Lwt_unix.close fd
      in
      let server =
        let rec accept_loop rem acc =
          if rem = 0 then Lwt.return acc
          else
            Lwt_unix.accept lsock >>= fun (fd, _) ->
            accept_loop (rem - 1) (serve fd :: acc)
        in
        accept_loop srv_connections [] >>= Lwt.join
      in
      let client () =
        let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
        Lwt_unix.connect fd addr >>= fun () ->
        let req = Bytes.create req_len and respbuf = Bytes.create resp_len in
        let rec loop n =
          if n = 0 then Lwt.return_unit
          else
            lwt_write_all fd req 0 req_len >>= fun () ->
            lwt_read_exactly fd respbuf 0 resp_len >>= fun () -> loop (n - 1)
        in
        loop srv_requests >>= fun () -> Lwt_unix.close fd
      in
      let clients = List.init srv_connections (fun _ -> client ()) in
      Lwt.join (server :: clients) );
  Lwt_main.run (Lwt_unix.close lsock)

let server_direct () =
  let lsock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt lsock Unix.SO_REUSEADDR true;
  Lwt_main.run
    ( Lwt_unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0))
      >>= fun () ->
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
        (Lwt_direct.spawn server
        :: List.init srv_connections (fun _ -> Lwt_direct.spawn client)) );
  Lwt_main.run (Lwt_unix.close lsock)

let server_effects () =
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
      Unix.close fd;
      return_unit
    in
    let server () =
      let handlers = ref [] in
      for _ = 1 to srv_connections do
        let fd, _ = Io.accept lsock in
        handlers := async (serve fd) :: !handlers
      done;
      await (join !handlers);
      return_unit
    in
    let client () =
      let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Unix.set_nonblock fd;
      Io.connect fd addr;
      let req = Bytes.create req_len and respbuf = Bytes.create resp_len in
      for _ = 1 to srv_requests do
        eff_write_all fd req 0 req_len;
        eff_read_exactly fd respbuf 0 resp_len
      done;
      Unix.close fd;
      return_unit
    in
    let sp = async server in
    let cps = List.init srv_connections (fun _ -> async client) in
    await (join (sp :: cps));
    return_unit);
  Unix.close lsock

let server_effects_uring () =
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
      Unix.close fd;
      return_unit
    in
    let server () =
      let handlers = ref [] in
      for _ = 1 to srv_connections do
        let fd, _ = EU.Io.accept lsock in
        handlers := async (serve fd) :: !handlers
      done;
      await (join !handlers);
      return_unit
    in
    let client () =
      let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Unix.set_nonblock fd;
      EU.Io.connect fd addr;
      let req = Cstruct.create req_len and respbuf = Cstruct.create resp_len in
      for _ = 1 to srv_requests do
        eu_write_all fd req 0 req_len;
        eu_read_exactly fd respbuf 0 resp_len
      done;
      Unix.close fd;
      return_unit
    in
    let sp = async server in
    let cps = List.init srv_connections (fun _ -> async client) in
    await (join (sp :: cps));
    return_unit);
  Unix.close lsock

let server_label =
  Printf.sprintf "server %dx%d keep-alive (req/s)" srv_connections srv_requests

let run_server_phase_a () =
  Printf.printf "\n== %s ==\n%!" server_label;
  bench_throughput server_label 0 ~total:srv_total server_lwt;
  bench_throughput server_label 2 ~total:srv_total server_effects;
  bench_throughput server_label 3 ~total:srv_total server_effects_uring;
  bench_throughput server_label 4 ~total:srv_total server_direct

let run_server_phase_b () =
  bench_throughput server_label 1 ~total:srv_total server_lwt;
  bench_throughput server_label 5 ~total:srv_total server_direct

(* ------------------------------------------------------------------ *)
(* Rendering                                                          *)
(* ------------------------------------------------------------------ *)

let render () =
  let rows = List.rev !row_order in
  let label_w =
    List.fold_left (fun acc l -> max acc (String.length l)) 5 rows
  in
  let col_w i = max 8 (String.length columns.(i)) in
  let cell label i =
    match (row label).(i) with Some s -> s | None -> "·"
  in
  let pad s w = s ^ String.make (max 0 (w - String.length s)) ' ' in
  Printf.printf "\n\n# Comparative matrix%s\n\n"
    (if quick then "  (quick mode — indicative only)" else "");
  (* Markdown header. *)
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
  Printf.printf
    "\n· = not measured;  — = not applicable (io_uring is irrelevant to a \
     workload with no I/O).\n";
  Printf.printf
    "Lower is better except the server row (req/s, higher is better).\n";
  Printf.printf
    "ping-pong \"bytes\"/\"bigarray\" = the buffer API of the Lwt & direct \
     columns; the effect\ncolumns always use Cstruct (copy-free), so their \
     bigarray rows are left blank (see the bytes\nrows). The bytes path copies \
     into an off-heap Cstruct per I/O; under io_uring this copy\ndominates at \
     large payloads, while the bigarray path (Lwt_io / cohttp) is copy-free.\n"

(* ------------------------------------------------------------------ *)
(* Main                                                               *)
(* ------------------------------------------------------------------ *)

let () =
  Printf.printf
    "Unified comparative benchmark (Lwt / io_uring / effects)%s\n%!"
    (if quick then "  [quick mode]" else "");
  (* Pass 1: default engine (libev/epoll) + the private io_uring ring. *)
  run_scheduling ();
  run_bind ();
  run_pingpong_phase_a ();
  run_server_phase_a ();
  (* Pass 2: install the global io_uring engine, once, and measure the
     configurations that rely on it. *)
  Printf.printf "\n-- installing the global io_uring engine (Lwt_uring.set) --\n%!";
  Lwt_uring.set ();
  run_pingpong_phase_b ();
  run_server_phase_b ();
  render ()
