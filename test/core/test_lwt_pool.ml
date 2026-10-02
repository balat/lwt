(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



open Test
open Lwt.Infix

let rec pauses n =
  if n = 0 then Lwt.return_unit else Lwt.pause () >>= fun () -> pauses (n - 1)

exception Dummy_error

let suite = suite "lwt_pool" [

  test "basic create-use" begin fun () ->
    let gen = fun () -> Lwt.return_unit in
    let p = Lwt_pool.create 1 gen in
    Lwt.return (Lwt.state (Lwt_pool.use p Lwt.return) = Lwt.Return ())
  end;

  test "creator exception" begin fun () ->
    let gen = fun () -> raise Dummy_error in
    let p = Lwt_pool.create 1 gen in
    let u = Lwt_pool.use p (fun _ -> Lwt.return 0) in
    Lwt.return (Lwt.state u = Lwt.Fail Dummy_error)
  end;

  test "pool elements are reused" begin fun () ->
    let gen = (fun () -> let n = ref 0 in Lwt.return n) in
    let p = Lwt_pool.create 1 gen in
    let _ = Lwt_pool.use p (fun n -> n := 1; Lwt.return !n) in
    let u2 = Lwt_pool.use p (fun n -> Lwt.return !n) in
    Lwt.return (Lwt.state u2 = Lwt.Return 1)
  end;

  test "pool elements are validated when returned" begin fun () ->
    let gen = (fun () -> let n = ref 0 in Lwt.return n) in
    let v l = Lwt.return (!l = 0) in
    let p = Lwt_pool.create 1 ~validate:v gen in
    let _ = Lwt_pool.use p (fun n -> n := 1; Lwt.return !n) in
    let u2 = Lwt_pool.use p (fun n -> Lwt.return !n) in
    Lwt.return (Lwt.state u2 = Lwt.Return 0)
  end;

  test "validation exceptions are propagated to users" begin fun () ->
    let c = Lwt_condition.create () in
    let gen = (fun () -> let l = ref 0 in Lwt.return l) in
    let v l = if !l = 0 then Lwt.return_true else raise Dummy_error in
    let p = Lwt_pool.create 1 ~validate:v gen in
    let u1 = Lwt_pool.use p (fun l -> l := 1; Lwt_condition.wait c) in
    let u2 = Lwt_pool.use p (fun l -> Lwt.return !l) in
    let () = Lwt_condition.signal c "done" in
    Lwt.bind u1 (fun v1 ->
    Lwt.try_bind
      (fun () -> u2)
      (fun _ -> Lwt.return_false)
      (fun exn2 ->
        Lwt.return (v1 = "done" && exn2 = Dummy_error)))
  end;

  test "multiple creation" begin fun () ->
    let gen = (fun () -> let n = ref 0 in Lwt.return n) in
    let p = Lwt_pool.create 2 gen in
    let _ = Lwt_pool.use p (fun n -> n := 1; Lwt.pause ()) in
    let u2 = Lwt_pool.use p (fun n -> Lwt.return !n) in
    Lwt.return (Lwt.state u2 = Lwt.Return 0)
  end;

  test "users of an empty pool will wait" begin fun () ->
    let gen = (fun () -> Lwt.return 0) in
    let p = Lwt_pool.create 1 gen in
    let _ = Lwt_pool.use p (fun _ -> Lwt.pause ()) in
    let u2 = Lwt_pool.use p Lwt.return in
    Lwt.return (Lwt.state u2 = Lwt.Sleep)
  end;

  test "on check, good elements are retained" begin fun () ->
    let gen = (fun () -> let n = ref 1 in Lwt.return n) in
    let c = (fun x f -> f (!x > 0)) in
    let p = Lwt_pool.create 1 ~check: c gen in
    let _ = Lwt_pool.use p (fun n -> n := 2; Lwt.fail Dummy_error) in
    let u2 = Lwt_pool.use p (fun n -> Lwt.return !n) in
    Lwt.return (Lwt.state u2 = Lwt.Return 2)
  end;

  test "on check, bad elements are disposed of and replaced" begin fun () ->
    let gen = (fun () -> let n = ref 1 in Lwt.return n) in
    let check = (fun n f -> f (!n > 0)) in
    let disposed = ref false in
    let dispose _ = disposed := true; Lwt.return_unit in
    let p = Lwt_pool.create 1 ~check ~dispose gen in
    let task = (fun n -> incr n; Lwt.return !n) in
    let _ = Lwt_pool.use p (fun n -> n := 0; Lwt.fail Dummy_error) in
    let u2 = Lwt_pool.use p task in
    Lwt.return (Lwt.state u2 = Lwt.Return 2 && !disposed)
  end;

  test "clear disposes of all elements" begin fun () ->
    let gen = (fun () -> let n = ref 1 in Lwt.return n) in
    let count = ref 0 in
    let dispose _ = incr count; Lwt.return_unit in
    let p = Lwt_pool.create 2 ~dispose gen in
    let u = Lwt_pool.use p (fun _ -> Lwt.pause ()) in
    let _ = Lwt_pool.use p (fun _ -> Lwt.return_unit) in
    let _ = Lwt_pool.clear p in
    Lwt.bind u (fun () -> Lwt.return (!count = 2))
  end;

  test "waiter are notified on replacement" begin fun () ->
    let c = Lwt_condition.create () in
    let gen = (fun () -> let l = ref 0 in Lwt.return l) in
    let v l = if !l = 0 then Lwt.return_true else raise Dummy_error in
    let p = Lwt_pool.create 1 ~validate:v gen in
    let u1 = Lwt_pool.use p (fun l -> l := 1; Lwt_condition.wait c) in
    let u2 = Lwt_pool.use p (fun l -> Lwt.return !l) in
    let u3 = Lwt_pool.use p (fun l -> Lwt.return !l) in
    let () = Lwt_condition.signal c "done" in
    Lwt.bind u1 (fun v1 ->
    Lwt.bind u3 (fun v3 ->
    Lwt.try_bind
      (fun () -> u2)
      (fun _ -> Lwt.return_false)
      (fun exn2 ->
        Lwt.return (v1 = "done" && exn2 = Dummy_error && v3 = 0))))
  end;

  test "waiter are notified on replacement exception" begin fun () ->
    let c = Lwt_condition.create () in
    let k = ref true in
    let gen = fun () ->
      if !k then
        let l = ref 0 in Lwt.return l
      else
        raise Dummy_error
    in
    let v l = if !l = 0 then Lwt.return_true else raise Dummy_error in
    let p = Lwt_pool.create 1 ~validate:v gen in
    let u1 = Lwt_pool.use p (fun l -> l := 1; k:= false; Lwt_condition.wait c) in
    let u2 = Lwt_pool.use p (fun l -> Lwt.return !l) in
    let u3 = Lwt_pool.use p (fun l -> Lwt.return !l) in
    let () = Lwt_condition.signal c "done" in
    Lwt.bind u1 (fun v1 ->
    Lwt.try_bind
      (fun () -> u2)
      (fun _ -> Lwt.return_false)
      (fun exn2 ->
        Lwt.try_bind
          (fun () -> u3)
          (fun _ -> Lwt.return_false)
          (fun exn3 ->
            Lwt.return
              (v1 = "done" && exn2 = Dummy_error && exn3 = Dummy_error))))
  end;

  test "check and validate can be used together" begin fun () ->
    let gen = (fun () -> let l = ref 0 in Lwt.return l) in
    let v l = Lwt.return (!l > 0) in
    let c l f = f (!l > 1) in
    let cond = Lwt_condition.create() in
    let p = Lwt_pool.create 1 ~validate:v ~check:c gen in
    let _ = Lwt_pool.use p (fun l -> l := 1; Lwt_condition.wait cond) in
    let _ = Lwt_pool.use p (fun l -> l := 2; raise Dummy_error) in
    let u3 = Lwt_pool.use p (fun l -> Lwt.return !l) in
    let () = Lwt_condition.signal cond "done" in
    Lwt.bind u3 (fun v ->
    Lwt.return (v = 2))
  end;

  test "verify default check behavior" begin fun () ->
    let gen = (fun () -> let l = ref 0 in Lwt.return l) in
    let cond = Lwt_condition.create() in
    let p = Lwt_pool.create 1 gen in
    let _ = Lwt_pool.use p (fun l ->
      Lwt.bind (Lwt_condition.wait cond)
        (fun _ -> l:= 1; raise Dummy_error)) in
    let u2 = Lwt_pool.use p (fun l -> Lwt.return !l) in
    let () = Lwt_condition.signal cond "done" in
    Lwt.bind u2 (fun v ->
    Lwt.return (v = 1))
  end;

  (* Clear while a member is in use and a waiter is queued, then the holder
     succeeds: the member is disposed, and the waiter must get a replacement,
     as it does when the holder fails. It starved. *)
  test "clear while in use: the waiter is served when the holder succeeds"
      begin fun () ->
    let pool = Lwt_pool.create 1 (fun () -> Lwt.return_unit) in
    let gate, open_gate = Lwt.wait () in
    let a = Lwt_pool.use pool (fun () -> gate) in
    let b = Lwt_pool.use pool (fun () -> Lwt.return_true) in
    let cleared = Lwt_pool.clear pool in
    Lwt.wakeup open_gate ();
    a >>= fun () -> cleared >>= fun () ->
    Lwt.pick [ b; pauses 10 >|= fun () -> false ]
  end;

  (* A replacement member must be counted: created outside the count, it let
     the pool hold more members than max. *)
  test "replacement members are counted" begin fun () ->
    let live = ref 0 and max_live = ref 0 in
    let pool =
      Lwt_pool.create 1
        ~check:(fun _ k -> k false)
        ~dispose:(fun () -> decr live; Lwt.return_unit)
        (fun () -> incr live; max_live := max !max_live !live; Lwt.return_unit)
    in
    let gate, open_gate = Lwt.wait () in
    let a = Lwt_pool.use pool (fun () -> gate >>= fun () -> Lwt.fail Exit) in
    let b = Lwt_pool.use pool (fun () -> Lwt.pause ()) in
    Lwt.wakeup open_gate ();
    Lwt.catch (fun () -> a) (fun _ -> Lwt.return_unit) >>= fun () ->
    b >>= fun () ->
    let hold, release = Lwt.wait () in
    let c1 = Lwt_pool.use pool (fun () -> hold) in
    let c2 = Lwt_pool.use pool (fun () -> hold) in
    pauses 3 >>= fun () ->
    Lwt.wakeup release ();
    Lwt.join [ c1; c2 ] >|= fun () ->
    !max_live <= 1
  end;

  (* A waiter cancelled while its replacement is being created: the member
     created for it goes to the pool, rather than being lost. *)
  test "a waiter cancelled during replacement does not leak the member"
      begin fun () ->
    let live = ref 0 in
    let creating, finish_create = Lwt.wait () in
    let first = ref true in
    let pool =
      Lwt_pool.create 1
        ~check:(fun _ k -> k false)
        ~dispose:(fun () -> decr live; Lwt.return_unit)
        (fun () ->
           if !first then (first := false; incr live; Lwt.return_unit)
           else creating >|= fun () -> incr live)
    in
    let gate, open_gate = Lwt.wait () in
    let a = Lwt_pool.use pool (fun () -> gate >>= fun () -> Lwt.fail Exit) in
    let b = Lwt_pool.use pool (fun () -> Lwt.return_unit) in
    Lwt.wakeup open_gate ();
    Lwt.catch (fun () -> a) (fun _ -> Lwt.return_unit) >>= fun () ->
    Lwt.cancel b;
    Lwt.wakeup finish_create ();
    pauses 3 >>= fun () ->
    Lwt_pool.clear pool >|= fun () ->
    !live = 0
  end;

  (* A check that raises: the member is disposed, not lost with its slot. *)
  test "a check that raises does not lose the slot" begin fun () ->
    let pool =
      Lwt_pool.create 1 ~check:(fun _ _ -> failwith "check")
        (fun () -> Lwt.return_unit)
    in
    Lwt.catch
      (fun () -> Lwt_pool.use pool (fun () -> Lwt.fail Exit))
      (fun _ -> Lwt.return_unit)
    >>= fun () ->
    Lwt.pick
      [ (Lwt_pool.use pool (fun () -> Lwt.return_true));
        pauses 10 >|= fun () -> false ]
  end;

  (* A disposal that fails during clear: every member is still disposed and
     the count goes down for each, so the pool can be used to its size. *)
  test "a failing dispose does not lose the slot" begin fun () ->
    let n = ref 0 and disposed = ref 0 in
    let pool =
      Lwt_pool.create 2
        ~dispose:(fun c ->
          incr disposed;
          if c = 2 then Lwt.fail Exit else Lwt.return_unit)
        (fun () -> incr n; Lwt.return !n)
    in
    let gate, open_gate = Lwt.wait () in
    let a = Lwt_pool.use pool (fun _ -> gate)
    and b = Lwt_pool.use pool (fun _ -> gate) in
    Lwt.wakeup open_gate ();
    Lwt.join [ a; b ] >>= fun () ->
    Lwt.catch (fun () -> Lwt_pool.clear pool) (function Exit -> Lwt.return_unit | e -> Lwt.reraise e)
    >>= fun () ->
    let hold, release = Lwt.wait () in
    let c1 = Lwt_pool.use pool (fun _ -> hold)
    and c2 = Lwt_pool.use pool (fun _ -> hold) in
    pauses 3 >>= fun () ->
    let both_running = Lwt.state c1 = Lwt.Sleep && Lwt.state c2 = Lwt.Sleep
                       && Lwt_pool.wait_queue_length pool = 0 in
    Lwt.wakeup release ();
    (* Bounded: with the slot lost, the second use would wait for ever. *)
    Lwt.pick [ (Lwt.join [ c1; c2 ] >|= fun () -> true);
               pauses 10 >|= fun () -> false ]
    >|= fun joined ->
    joined && !disposed = 2 && both_running
  end;

  test "no starvation for waiters if pool member fails" begin fun () ->
    let use p f = Lwt.catch
      (fun () -> Lwt_pool.use p (fun _ -> f ()))
      (fun _ -> Lwt.return_unit)
    in
    let gen = (fun () -> Lwt.return_unit) in
    let p = Lwt_pool.create 1 ~check:(fun _ is_ok -> is_ok false) gen in
    let yielder, stop_yielding = Lwt.wait () in
    let holder = use p (fun _ -> Lwt.bind yielder (fun () -> failwith "op timeout")) in
    let waiter = use p (fun _ -> Lwt.return_unit) in
    let no_starvation = Lwt.join [holder; waiter] in
    Lwt.wakeup stop_yielding ();
    Lwt.bind (Lwt.pause ())
      (fun () -> match Lwt.state no_starvation with
      | Lwt.Return _ -> Lwt.return_true
      | Lwt.Fail _ | Lwt.Sleep -> Lwt.return_false)
  end;

]
