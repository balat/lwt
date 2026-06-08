(* Micro-benchmarks comparing the classic Lwt monadic core with the effect-based
   Lwt_effects POC.

   We measure two workloads:

   1. A chain of [bind]s over already-resolved promises (the "fast path"). This
      is where the POC is expected to shine: [bind] becomes a plain application
      with no per-step callback closure.

   2. A chain that suspends ([pause]) between steps. This exercises the
      suspension path: classic Lwt allocates a promise + callback per step,
      while the POC captures an effect continuation.

   For each workload we report wall-clock time and minor-heap words allocated,
   normalised per [bind].

   NOTE: this switch has no flambda. With flambda, the resolved-chain allocation
   for Lwt_effects is expected to drop further, as [bind (return v) f] can be
   simplified to [f v] by inlining (eliminating the result-promise record). *)

let measure name ~ops f =
  (* Warm up once, then measure. *)
  ignore (f ());
  Gc.full_major ();
  let w0 = Gc.minor_words () in
  let t0 = Unix.gettimeofday () in
  let r = f () in
  let t1 = Unix.gettimeofday () in
  let w1 = Gc.minor_words () in
  let dt = t1 -. t0 in
  let words = w1 -. w0 in
  Printf.printf "  %-22s  %8.3f s   %7.1f ns/op   %6.2f words/op\n" name dt
    (dt /. float_of_int ops *. 1e9)
    (words /. float_of_int ops);
  r

(* ------------------------------------------------------------------ *)
(* Workload 1: resolved bind chain                                    *)
(* ------------------------------------------------------------------ *)

let chain_len = 1000
let repeats = 5000
let ops1 = chain_len * repeats

let rec sum_lwt n acc =
  if n = 0 then Lwt.return acc
  else Lwt.bind (Lwt.return (acc + n)) (fun acc -> sum_lwt (n - 1) acc)

let rec sum_eff n acc =
  if n = 0 then Lwt_effects.return acc
  else
    Lwt_effects.bind (Lwt_effects.return (acc + n)) (fun acc ->
      sum_eff (n - 1) acc)

let bench_resolved () =
  Printf.printf "Workload 1: resolved bind chain (%d binds x %d repeats)\n"
    chain_len repeats;
  let total = ref 0 in
  measure "Lwt (classic)" ~ops:ops1 (fun () ->
    for _ = 1 to repeats do
      total := !total + Lwt_main.run (sum_lwt chain_len 0)
    done);
  measure "Lwt_effects" ~ops:ops1 (fun () ->
    for _ = 1 to repeats do
      total := !total + Lwt_effects.run (fun () -> sum_eff chain_len 0)
    done);
  ignore !total

(* ------------------------------------------------------------------ *)
(* Workload 2: suspension chain (pause between steps)                 *)
(* ------------------------------------------------------------------ *)

let pause_len = 1000
let pause_repeats = 1000
let ops2 = pause_len * pause_repeats

let rec pauses_lwt n =
  if n = 0 then Lwt.return_unit
  else Lwt.bind (Lwt.pause ()) (fun () -> pauses_lwt (n - 1))

let rec pauses_eff n =
  if n = 0 then Lwt_effects.return_unit
  else Lwt_effects.bind (Lwt_effects.pause ()) (fun () -> pauses_eff (n - 1))

(* Semantics-preserving (non-blocking) bind — comparable to Lwt's. *)
let rec pauses_compat n =
  if n = 0 then Lwt_effects.return_unit
  else Lwt_effects.Compat.bind (Lwt_effects.pause ()) (fun () -> pauses_compat (n - 1))

let bench_suspension () =
  Printf.printf "\nWorkload 2: suspension chain (%d pauses x %d repeats)\n"
    pause_len pause_repeats;
  measure "Lwt (classic)" ~ops:ops2 (fun () ->
    for _ = 1 to pause_repeats do
      Lwt_main.run (pauses_lwt pause_len)
    done);
  measure "Lwt_effects (effect bind)" ~ops:ops2 (fun () ->
    for _ = 1 to pause_repeats do
      Lwt_effects.run (fun () -> pauses_eff pause_len)
    done);
  measure "Lwt_effects (Compat mbind)" ~ops:ops2 (fun () ->
    for _ = 1 to pause_repeats do
      Lwt_effects.run (fun () -> pauses_compat pause_len)
    done)

let () =
  bench_resolved ();
  bench_suspension ()
