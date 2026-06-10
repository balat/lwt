(* Correctness tests for the Lwt_effects POC scheduler.

   Self-contained: a tiny assert harness, no external test framework, since the
   scheduler has its own [run] rather than Lwt's runtime. *)

open Lwt_effects
open Lwt_effects.Syntax

let failures = ref 0

let check name cond =
  if cond then Printf.printf "ok   - %s\n" name
  else begin
    incr failures;
    Printf.printf "FAIL - %s\n" name
  end

(* [return]/[bind] fast path: a long resolved chain. *)
let () =
  let r =
    run (fun () ->
      let* x = return 1 in
      let* y = return 2 in
      let+ z = return 3 in
      x + y + z)
  in
  check "resolved bind chain" (r = 6)

(* Awaiting a promise that only resolves on the next tick (pause). *)
let () =
  let r =
    run (fun () ->
      let* () = pause () in
      return 42)
  in
  check "pause then resolve" (r = 42)

(* Concurrency through [async]: two fibers interleave via [pause]. *)
let () =
  let log = ref [] in
  let push x = log := x :: !log in
  run (fun () ->
    let a =
      async (fun () ->
        push "a1";
        let* () = pause () in
        push "a2";
        return ())
    in
    let b =
      async (fun () ->
        push "b1";
        let* () = pause () in
        push "b2";
        return ())
    in
    let* () = a in
    let* () = b in
    return ());
  check "async interleaving" (List.rev !log = [ "a1"; "b1"; "a2"; "b2" ])

(* [yield] interleaves fibers without allocating a promise. *)
let () =
  let log = ref [] in
  let push x = log := x :: !log in
  run (fun () ->
    let a = async (fun () -> push "a1"; Direct.yield (); push "a2"; return ()) in
    let b = async (fun () -> push "b1"; Direct.yield (); push "b2"; return ()) in
    let* () = a in
    let* () = b in
    return ());
  check "yield interleaving" (List.rev !log = [ "a1"; "b1"; "a2"; "b2" ])

(* [both] collects two concurrently-running promises. *)
let () =
  let r =
    run (fun () ->
      let a = async (fun () -> let+ () = pause () in 10) in
      let b = async (fun () -> let+ () = pause () in 20) in
      let+ x, y = both a b in
      x + y)
  in
  check "both" (r = 30)

(* Exception propagation through bind. *)
let () =
  let raised =
    try
      ignore
        (run (fun () ->
           let* () = pause () in
           fail (Failure "boom")));
      false
    with Failure msg -> msg = "boom"
  in
  check "exception propagates through run" raised

(* A synchronous exception in [f] becomes a rejected promise (not a raise),
   recovered with the [catch] combinator. *)
let () =
  let r =
    run (fun () ->
      catch
        (fun () ->
          let* () = pause () in
          raise (Failure "sync"))
        (fun e ->
          match e with
          | Failure msg -> return ("caught:" ^ msg)
          | _ -> return "wrong"))
  in
  check "catch recovers a rejection" (r = "caught:sync")

(* Direct-style recovery: inside a fiber, [await] on a rejected promise raises,
   so an ordinary [try ... with] works. *)
let () =
  let r =
    run (fun () ->
      let p =
        let* () = pause () in
        fail (Failure "boom")
      in
      return (try ignore (Direct.await p); "no" with Failure msg -> "caught:" ^ msg))
  in
  check "direct-style try/await catches rejection" (r = "caught:boom")

(* [choose] resolves with the first promise to complete. *)
let () =
  let r =
    run (fun () ->
      let slow = async (fun () -> let+ () = sleep 0.05 in "slow") in
      let fast = async (fun () -> let+ () = sleep 0.001 in "fast") in
      choose [ slow; fast ])
  in
  check "choose picks the fastest" (r = "fast")

(* [sleep] actually waits (ordering of two sleeps). *)
let () =
  let log = ref [] in
  run (fun () ->
    let a = async (fun () -> let+ () = sleep 0.02 in log := "late" :: !log) in
    let b = async (fun () -> let+ () = sleep 0.005 in log := "early" :: !log) in
    let* () = a in
    b);
  check "sleep ordering" (List.rev !log = [ "early"; "late" ])

(* [cancel] rejects a pending promise with [Canceled]. *)
let () =
  let r =
    run (fun () ->
      let s = sleep 1.0 in
      cancel s;
      try
        Direct.await s;
        return "not-canceled"
      with Canceled -> return "canceled")
  in
  check "cancel rejects with Canceled" (r = "canceled")

(* [pick] cancels the losers: the 0.5s timer must be stopped when the 0.01s one
   resolves, so the whole run returns quickly rather than after 0.5s. *)
let () =
  let t0 = Unix.gettimeofday () in
  run (fun () -> pick [ sleep 0.5; sleep 0.01 ]);
  let dt = Unix.gettimeofday () -. t0 in
  check "pick cancels the slow loser" (dt < 0.25)

(* Real non-blocking I/O over a socketpair: a writer and a reader fiber. *)
let () =
  let r =
    run (fun () ->
      let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
      Unix.set_nonblock a;
      Unix.set_nonblock b;
      let msg = Bytes.of_string "hello" in
      let reader =
        async (fun () ->
          let buf = Bytes.create 16 in
          let n = Direct.Io.read b buf 0 (Bytes.length buf) in
          return (Bytes.sub_string buf 0 n))
      in
      let writer = async (fun () -> return (Direct.Io.write a msg 0 (Bytes.length msg))) in
      let* s, _ = both reader writer in
      Unix.close a;
      Unix.close b;
      return s)
  in
  check "socketpair read/write" (r = "hello")

(* Lwt-compatibility layer: wait/wakeup resolver, state, join, finalize. *)
let () =
  let r =
    run (fun () ->
      let p, u = wait () in
      let waiter = async (fun () -> let* x = p in return (x * 2)) in
      wakeup u 21;
      waiter)
  in
  check "wait/wakeup resolver" (r = 42)

let () =
  let r = run (fun () -> return (state (return 7))) in
  check "state of resolved" (match r with Return 7 -> true | _ -> false)

let () =
  let order = ref [] in
  run (fun () ->
    let a = async (fun () -> let* () = pause () in order := 1 :: !order; return ()) in
    let b = async (fun () -> order := 2 :: !order; return ()) in
    join [ a; b ]);
  check "join waits all" (List.sort compare !order = [ 1; 2 ])

let () =
  let steps = ref [] in
  let r =
    run (fun () ->
      finalize
        (fun () -> steps := "body" :: !steps; return 99)
        (fun () -> steps := "final" :: !steps; return ()))
  in
  check "finalize runs cleanup" (r = 99 && List.rev !steps = [ "body"; "final" ])

(* Fiber-local storage: scope, survival across suspension, async inheritance,
   per-fiber isolation. *)
let () =
  let k = new_key () in
  let inside, outside =
    run (fun () ->
      let+ inside = with_value k (Some 42) (fun () ->
        let* () = pause () in
        return (get k))
      in
      (inside, get k))
  in
  check "storage in scope (survives suspension)" (inside = Some 42);
  check "storage out of scope" (outside = None)

let () =
  let k = new_key () in
  let r =
    run (fun () ->
      with_value k (Some 7) (fun () ->
        let child = async (fun () -> let* () = pause () in return (get k)) in
        child))
  in
  check "storage inherited by async child" (r = Some 7)

let () =
  let k = new_key () in
  let results = ref [] in
  run (fun () ->
    let a =
      async (fun () ->
        with_value k (Some "a") (fun () ->
          let* () = pause () in
          results := ("a", get k) :: !results;
          return ()))
    in
    let b =
      async (fun () ->
        with_value k (Some "b") (fun () ->
          let* () = pause () in
          results := ("b", get k) :: !results;
          return ()))
    in
    let* () = a in
    b);
  check "storage per-fiber isolation"
    (List.sort compare !results = [ ("a", Some "a"); ("b", Some "b") ])

(* The default bind is non-blocking, so [both (a >>= f) (b >>= g)] preserves
   Lwt's implicit concurrency: the two 0.05s sleeps overlap (~0.05s, not ~0.10s).
   This is the semantics-preserving property of the scheduler. *)
let () =
  let t0 = Unix.gettimeofday () in
  run (fun () ->
    both
      (sleep 0.05 >>= fun () -> return 1)
      (sleep 0.05 >>= fun () -> return 2)
    >>= fun _ -> return_unit);
  let dt = Unix.gettimeofday () -. t0 in
  check "default bind preserves implicit concurrency (~0.05s)" (dt < 0.085)

(* Covariance of [t]: a subtyping coercion only type-checks if [+'a t]. *)
let () =
  let p : [ `A ] t = return `A in
  let _wider : [ `A | `B ] t = (p :> [ `A | `B ] t) in
  check "t is covariant (subtyping coercion type-checks)" true

let () =
  if !failures = 0 then print_endline "\nAll tests passed."
  else begin
    Printf.printf "\n%d test(s) failed.\n" !failures;
    exit 1
  end
