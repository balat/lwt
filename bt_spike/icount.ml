(* Instruction-count probe for S0: the core ONLY, no engine, no lwt.unix, so the
   count is attributable to the promise machinery. Driven by the core's own
   scheduler hook, which serves pauses without any event loop.

   Run twice, with N and 2N, and subtract: the difference divided by N is the
   marginal cost per operation, which cancels startup and the fixed cost of the
   harness. Callgrind makes that difference exact rather than statistical. *)

[@@@alert "-trespassing"]

let chain = 1000

let rec sum n acc =
  if n = 0 then Lwt.return acc else Lwt.bind (Lwt.return (acc + n)) (fun acc -> sum (n - 1) acc)

let rec pauses n =
  if n = 0 then Lwt.return_unit else Lwt.bind (Lwt.pause ()) (fun () -> pauses n)
  [@@warning "-32"]

let rec pauses n = if n = 0 then Lwt.return_unit else Lwt.bind (Lwt.pause ()) (fun () -> pauses (n - 1))

let () =
  let which = Sys.argv.(1) in
  let reps = int_of_string Sys.argv.(2) in
  match which with
  | "resolved" ->
    let total = ref 0 in
    for _ = 1 to reps do
      match Lwt.state (sum chain 0) with Lwt.Return v -> total := !total + v | _ -> assert false
    done;
    Printf.printf "resolved %d ops, checksum %d\n" (reps * chain) !total
  | "suspended" ->
    for _ = 1 to reps do
      Lwt.Private.scheduler_run (fun () -> pauses chain)
    done;
    Printf.printf "suspended %d ops\n" (reps * chain)
  | "wakeup" ->
    (* [wait] then [wakeup]: the shape of an asynchronous completion. *)
    for _ = 1 to reps do
      for _ = 1 to chain do
        let p, u = Lwt.wait () in
        Lwt.wakeup u ();
        match Lwt.state p with Lwt.Return () -> () | _ -> assert false
      done
    done;
    Printf.printf "wakeup %d ops\n" (reps * chain)
  | "io" ->
    (* [wait], a pending [bind] on it, then [wakeup]: what one asynchronous I/O
       operation costs a caller in core terms. *)
    let total = ref 0 in
    for _ = 1 to reps do
      for _ = 1 to chain do
        let p, u = Lwt.wait () in
        let q = Lwt.bind p (fun () -> Lwt.return 1) in
        Lwt.wakeup u ();
        match Lwt.state q with
        | Lwt.Return v -> total := !total + v
        | _ -> assert false
      done
    done;
    Printf.printf "io %d ops, checksum %d\n" (reps * chain) !total
  | _ -> assert false
