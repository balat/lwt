(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* [await] outside [spawn]: the scheduler loop runs under the effect handler,
   so any callback the loop runs may await. The first tests check that it
   works; the "characterisation" tests pin down what an await in a callback
   does to the rest of the stack, which is the documented rule of thumb. *)

open Test
open Lwt.Infix

let await = Lwt_direct.await

(* The order in which named events happen. *)
let trace = ref []
let mark s = trace := s :: !trace
let reset () = trace := []
let order () = List.rev !trace

(* Resolve [r] from the event loop, [delay] seconds from now. *)
let resolve_later ~delay ~mark:m r =
  Lwt.async (fun () -> Lwt_unix.sleep delay >|= fun () -> mark m; Lwt.wakeup r ())

let anywhere_suite = suite "await anywhere" [
  test "await in a bind continuation" begin fun () ->
    Lwt.pause () >>= fun () ->
    let v = await (Lwt_unix.sleep 1e-6 >|= fun () -> 42) in
    Lwt.return (v = 42)
  end;

  test "await in an on_success callback" begin fun () ->
    let p, r = Lwt.wait () in
    let result, result_r = Lwt.wait () in
    Lwt.on_success p (fun x ->
      let y = await (Lwt_unix.sleep 1e-6 >|= fun () -> x + 1) in
      Lwt.wakeup result_r y);
    Lwt.wakeup r 1;
    result >|= fun y -> y = 2
  end;

  test "100_000 sequential awaits from monadic callbacks" begin fun () ->
    (* Each lap suspends the drainer once: a new loop pass per iteration. Run
       with a small OCAMLRUNPARAM=l to check the stack stays flat. *)
    let rec go n =
      if n = 0 then Lwt.return_true
      else Lwt.pause () >>= fun () -> await (Lwt.pause ()); go (n - 1)
    in
    go 100_000
  end;

  test "a spawned task inside a callback keeps its own handler" begin fun () ->
    Lwt.pause () >>= fun () ->
    let inner = Lwt_direct.spawn (fun () -> await (Lwt_unix.sleep 1e-6); 7) in
    let outer = await (Lwt_unix.sleep 1e-6 >|= fun () -> 35) in
    inner >|= fun v -> v + outer = 42
  end;

  test "storage survives an await in a callback" begin fun () ->
    let k = Lwt.new_key () in
    Lwt.with_value k (Some "v") (fun () ->
      Lwt.pause () >>= fun () ->
      let before = Lwt.get k in
      await (Lwt_unix.sleep 1e-6);
      let after = Lwt.get k in
      Lwt.return (before = Some "v" && after = Some "v"))
  end;

  test "await on a promise rejected later raises at the await" begin fun () ->
    Lwt.pause () >>= fun () ->
    let p = Lwt_unix.sleep 1e-6 >>= fun () -> Lwt.fail Exit in
    match await p with
    | () -> Lwt.return_false
    | exception Exit -> Lwt.return_true
  end;

  test "an exception raised after resumption rejects the bind" begin fun () ->
    let p =
      Lwt.pause () >>= fun () ->
      await (Lwt_unix.sleep 1e-6 >>= fun () -> (Lwt.fail Exit : unit Lwt.t));
      Lwt.return_unit
    in
    Lwt.catch
      (fun () -> p >|= fun () -> false)
      (function Exit -> Lwt.return_true | _ -> Lwt.return_false)
  end;

  (* Characterisation: what else the await suspends. *)

  test "the resolver's continuation waits with the awaiting callback" begin fun () ->
    reset ();
    let p, r = Lwt.wait () in
    let q, rq = Lwt.wait () in
    Lwt.on_success p (fun () ->
      mark "callback: before await"; await q; mark "callback: after await");
    resolve_later ~delay:1e-3 ~mark:"helper: resolves q" rq;
    Lwt.pause () >>= fun () ->
    mark "resolver: wakeup";
    Lwt.wakeup r ();
    mark "resolver: after wakeup";
    Lwt.return
      (order () = [
        "resolver: wakeup"; "callback: before await"; "helper: resolves q";
        "callback: after await"; "resolver: after wakeup" ])
  end;

  test "the other waiters of the same promise do not wait" begin fun () ->
    reset ();
    let p, r = Lwt.wait () in
    let q, rq = Lwt.wait () in
    (* Callbacks run most recently attached first: the awaiter goes first. *)
    Lwt.on_success p (fun () -> mark "sibling");
    Lwt.on_success p (fun () -> mark "awaiter: before"; await q; mark "awaiter: after");
    resolve_later ~delay:1e-3 ~mark:"helper" rq;
    Lwt.pause () >>= fun () ->
    Lwt.wakeup r ();
    Lwt_unix.sleep 3e-3 >|= fun () ->
    order () = ["awaiter: before"; "sibling"; "helper"; "awaiter: after"]
  end;

  test "wakeup_later from a callback defers, so the resolver is not suspended" begin fun () ->
    reset ();
    let c = Lwt_condition.create () in
    let q, rq = Lwt.wait () in
    let waiter =
      Lwt_condition.wait c >|= fun () -> mark "waiter: before"; await q; mark "waiter: after"
    in
    resolve_later ~delay:1e-3 ~mark:"helper" rq;
    Lwt.pause () >>= fun () ->
    (* [signal] is a [wakeup_later] inside a callback (this one): the
       resolution loop defers the waiter, which runs after this callback. *)
    Lwt_condition.signal c ();
    mark "signaller: continues";
    waiter >|= fun () ->
    order () = ["signaller: continues"; "waiter: before"; "helper"; "waiter: after"]
  end;

  (* Isolation: callbacks that belong to different events must not wait for
     each other's awaits. *)

  test "callbacks of distinct paused promises are independent" begin fun () ->
    reset ();
    let q, rq = Lwt.wait () in
    let a = Lwt.pause () >|= fun () -> mark "a: before"; await q; mark "a: after" in
    let b = Lwt.pause () >|= fun () -> mark "b" in
    resolve_later ~delay:1e-3 ~mark:"helper" rq;
    Lwt.both a b >|= fun ((), ()) ->
    order () = ["a: before"; "b"; "helper"; "a: after"]
  end;

  test "callbacks of distinct engine events are independent" begin fun () ->
    reset ();
    let q, rq = Lwt.wait () in
    let r1, w1 = Lwt_unix.pipe () in
    let r2, w2 = Lwt_unix.pipe () in
    let a = Lwt_unix.wait_read r1 >|= fun () -> mark "a: before"; await q; mark "a: after" in
    let b = Lwt_unix.wait_read r2 >|= fun () -> mark "b" in
    (* Both become readable before the engine looks: one lap, two events. *)
    let write w = ignore (Unix.write_substring (Lwt_unix.unix_file_descr w) "x" 0 1) in
    write w1; write w2;
    resolve_later ~delay:2e-3 ~mark:"helper" rq;
    Lwt.both a b >>= fun ((), ()) ->
    Lwt.join [Lwt_unix.close r1; Lwt_unix.close w1; Lwt_unix.close r2; Lwt_unix.close w2] >|= fun () ->
    (* The engine may dispatch the two events in either order; what matters is
       that [b] did not wait for [a]'s await, i.e. ran before the helper. *)
    let rec index s = function [] -> max_int | x :: l -> if x = s then 0 else 1 + index s l in
    let o = order () in
    index "b" o < index "helper" o && index "helper" o < index "a: after" o
    && index "a: before" o < index "helper" o
  end;
]

let no_await_suite = suite "no_await" [
  test "an await on a pending promise raises at the await" begin fun () ->
    Lwt.pause () >|= fun () ->
    Lwt_direct.no_await (fun () ->
      match await (Lwt_unix.sleep 1e-3) with
      | () -> false
      | exception Lwt_direct.Suspension_forbidden -> true)
  end;

  test "an await on a resolved promise is allowed" begin fun () ->
    Lwt.pause () >|= fun () ->
    Lwt_direct.no_await (fun () -> await (Lwt.return 3)) = 3
  end;

  test "yield raises too" begin fun () ->
    Lwt.pause () >|= fun () ->
    Lwt_direct.no_await (fun () ->
      match Lwt_direct.yield () with
      | () -> false
      | exception Lwt_direct.Suspension_forbidden -> true)
  end;

  test "uncaught, the exception leaves the region" begin fun () ->
    Lwt.pause () >|= fun () ->
    match Lwt_direct.no_await (fun () -> await (Lwt.pause ())) with
    | () -> false
    | exception Lwt_direct.Suspension_forbidden -> true
  end;

  test "it applies at any depth, through a callback of a stdlib iterator" begin fun () ->
    Lwt.pause () >|= fun () ->
    let h = Hashtbl.create 3 in
    Hashtbl.replace h 1 (); Hashtbl.replace h 2 ();
    match Lwt_direct.no_await (fun () ->
      Hashtbl.iter (fun _ () -> await (Lwt_unix.sleep 1e-3)) h) with
    | () -> false
    | exception Lwt_direct.Suspension_forbidden -> true
  end;

  test "inside a spawn body, the region still wins" begin fun () ->
    Lwt_direct.spawn (fun () ->
      Lwt_direct.no_await (fun () ->
        match await (Lwt_unix.sleep 1e-3) with
        | () -> false
        | exception Lwt_direct.Suspension_forbidden -> true))
  end;

  test "a task spawned inside the region is not affected" begin fun () ->
    Lwt.pause () >>= fun () ->
    let p = Lwt_direct.no_await (fun () ->
      Lwt_direct.spawn (fun () -> await (Lwt_unix.sleep 1e-3); 5)) in
    p >|= fun v -> v = 5
  end;

  test "the region ends with its function" begin fun () ->
    Lwt.pause () >|= fun () ->
    Lwt_direct.no_await (fun () -> ());
    await (Lwt_unix.sleep 1e-3);
    true
  end;

  test "a setter from Lwt_react.S.create refuses to suspend its propagation" begin fun () ->
    Lwt.pause () >|= fun () ->
    let s, set = Lwt_react.S.create 0 in
    let seen = ref [] in
    let s' =
      React.S.map (fun v ->
        if v = 2 then await (Lwt_unix.sleep 1e-3);
        seen := v :: !seen; v) s
    in
    set 1;
    let refused =
      match set 2 with
      | () -> false
      | exception Lwt_direct.Suspension_forbidden -> true
    in
    ignore (React.S.value s');
    refused && !seen = [1; 0]
  end;

  test "a setter from React.S.create suspends silently; no_await around it refuses" begin fun () ->
    Lwt.pause () >|= fun () ->
    let s, set = React.S.create 0 in
    let s' = React.S.map (fun v -> if v > 0 then await (Lwt_unix.sleep 1e-3); v) s in
    (* Unprotected: the node awaits, this task is suspended with the update
       step in progress, and resumes 1 ms later as if nothing had happened. *)
    set 1;
    let after_silent = React.S.value s' = 1 in
    let refused =
      match Lwt_direct.no_await (fun () -> set 2) with
      | () -> false
      | exception Lwt_direct.Suspension_forbidden -> true
    in
    after_silent && refused
  end;
]

let state_suite = suite "resolution state across suspensions" [
  test "a wakeup_later from another task resolves the awaited promise" begin fun () ->
    (* The suspended callback left the resolution loop at depth 1; the pass
       must not inherit that depth, or this wakeup_later is deferred and never
       drained. *)
    let q, rq = Lwt.wait () in
    Lwt.async (fun () -> Lwt_unix.sleep 1e-3 >|= fun () -> Lwt.wakeup_later rq ());
    Lwt.pause () >|= fun () -> await q; true
  end;

  test "callbacks deferred before an await run while it is suspended" begin fun () ->
    reset ();
    let q, rq = Lwt.wait () in
    let p2, r2 = Lwt.wait () in
    Lwt.on_success p2 (fun () -> mark "deferred");
    resolve_later ~delay:1e-3 ~mark:"helper" rq;
    Lwt.pause () >|= fun () ->
    (* At depth 1: this wakeup_later is deferred to the end of the cascade. *)
    Lwt.wakeup_later r2 ();
    mark "before"; await q; mark "after";
    order () = ["before"; "deferred"; "helper"; "after"]
  end;

  test "siblings of nested cascades run, the resolver still waits" begin fun () ->
    reset ();
    let q, rq = Lwt.wait () in
    let p, r = Lwt.wait () in
    let p2, r2 = Lwt.wait () in
    Lwt.on_success p2 (fun () -> mark "inner sibling");
    Lwt.on_success p2 (fun () ->
      mark "inner awaiter: before"; await q; mark "inner awaiter: after");
    Lwt.on_success p (fun () -> mark "outer sibling");
    Lwt.on_success p (fun () -> Lwt.wakeup r2 (); mark "outer: after wakeup");
    resolve_later ~delay:1e-3 ~mark:"helper" rq;
    Lwt.pause () >>= fun () ->
    Lwt.wakeup r ();
    Lwt_unix.sleep 3e-3 >|= fun () ->
    order () = [
      "inner awaiter: before"; "inner sibling"; "outer sibling"; "helper";
      "inner awaiter: after"; "outer: after wakeup" ]
  end;
]

[@@@alert "-trespassing"]

(* Regression tests for the findings of the review of the branch. *)
let review_suite = suite "review findings" [
  test "an await in an on_cancel callback detaches the rest of the cascade" begin fun () ->
    (* The cleanup the on_cancel callback awaits is done by an on_failure
       callback of the SAME promise, which runs after the cancel callbacks: it
       must be detached and run while the callback is suspended. *)
    let cleanup_done, cleanup_u = Lwt.wait () in
    let p, _ = Lwt.task () in
    Lwt.on_failure p (fun _ -> Lwt.wakeup cleanup_u ());
    Lwt.on_cancel p (fun () -> await cleanup_done);
    let run =
      Lwt.pause () >>= fun () ->
      Lwt.cancel p;
      cleanup_done >|= fun () -> true
    in
    Lwt.pick [run; (Lwt_unix.sleep 1.0 >|= fun () -> false)]
  end;

  test "an exception out of a waiter leaves no stale cascade behind" begin fun () ->
    let ran = ref false in
    let p, r = Lwt.wait () in
    Lwt.on_success p (fun () -> ran := true);
    Lwt.on_success p (fun () -> raise Stack_overflow);
    Lwt.pause () >>= fun () ->
    (match Lwt.wakeup r () with () -> () | exception Stack_overflow -> ());
    let before = !ran in
    (* A suspension used to detach the stale cascade and resurrect the
       abandoned waiter. *)
    await (Lwt_unix.sleep 1e-3);
    Lwt.return (not before && not !ran)
  end;

  test "an await in an iteration hook raises Suspension_forbidden" begin fun () ->
    let seen = ref false in
    let hook =
      Lwt_main.Enter_iter_hooks.add_first (fun () ->
        match await (Lwt_unix.sleep 1e-3) with
        | () -> ()
        | exception Lwt_direct.Suspension_forbidden -> seen := true)
    in
    Lwt_unix.sleep 2e-3 >|= fun () ->
    Lwt_main.Enter_iter_hooks.remove hook;
    !seen
  end;

  test "an await in an engine callback raises Suspension_forbidden, on every engine" begin fun () ->
    let seen = ref false in
    let fired, fired_u = Lwt.wait () in
    let _ = Lwt_engine.on_timer 1e-3 false (fun ev ->
      Lwt_engine.stop_event ev;
      (match await (Lwt_unix.sleep 1e-3) with
       | () -> ()
       | exception Lwt_direct.Suspension_forbidden -> seen := true);
      Lwt.wakeup fired_u ())
    in
    fired >|= fun () -> !seen
  end;

  test "leave hooks run after the callbacks of their lap" begin fun () ->
    (* A leave hook that flushes what a timer callback asked for: it must see
       the request in the same lap, not at the next event. *)
    let pending = ref false in
    let flushed, flushed_u = Lwt.wait () in
    let hook =
      Lwt_main.Leave_iter_hooks.add_first (fun () ->
        if !pending then begin pending := false; Lwt.wakeup flushed_u () end)
    in
    let t0 = Unix.gettimeofday () in
    Lwt.async (fun () -> Lwt_unix.sleep 1e-3 >|= fun () -> pending := true);
    Lwt.pick [flushed; Lwt_unix.sleep 0.5] >|= fun () ->
    Lwt_main.Leave_iter_hooks.remove hook;
    Unix.gettimeofday () -. t0 < 0.1
  end;

  test "await on a pending promise of another domain raises Foreign_promise at the call" begin fun () ->
    let p, _ = Lwt.wait () in
    let d =
      Domain.spawn (fun () ->
        Lwt_main.run
          (Lwt.pause () >|= fun () ->
           match await p with
           | () -> false
           | exception Lwt.Foreign_promise -> true))
    in
    Lwt.return (Domain.join d)
  end;

  test "a loop of yields starves neither the engine nor the pauses" begin fun () ->
    let timer = ref false and paused = ref false in
    Lwt.async (fun () -> Lwt_unix.sleep 1e-3 >|= fun () -> timer := true);
    Lwt.async (fun () -> Lwt.pause () >|= fun () -> paused := true);
    Lwt_direct.spawn (fun () ->
      let t0 = Unix.gettimeofday () in
      while Unix.gettimeofday () -. t0 < 0.05 do Lwt_direct.yield () done;
      !timer && !paused)
  end;

  test "the same, yielding from a callback" begin fun () ->
    let timer = ref false in
    Lwt.async (fun () -> Lwt_unix.sleep 1e-3 >|= fun () -> timer := true);
    Lwt.pause () >|= fun () ->
    let t0 = Unix.gettimeofday () in
    while Unix.gettimeofday () -. t0 < 0.05 do Lwt_direct.yield () done;
    !timer
  end;

  test "abandon_paused also drops the pauses already queued as tasks" begin fun () ->
    let pz = Lwt.pause () in
    Lwt.Private.scheduler_serve_paused ();
    Lwt.abandon_paused ();
    Lwt_unix.sleep 1e-3 >|= fun () -> Lwt.state pz = Lwt.Sleep
  end;
]

let suites = [anywhere_suite; no_await_suite; state_suite; review_suite]
