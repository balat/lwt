(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Many waiters cancelled in a row: the ordinary shape of [Lwt.pick [lock m;
   timeout]] under load. Withdrawing a cancelled waiter filtered the whole
   queue, under the lock and with an allocation, so a storm of cancellations
   was quadratic: 16 000 took over two seconds. It is now a mark, and the
   marks are compacted away. The bound below is loose; the quadratic version
   missed it by an order of magnitude at this size.

   Also checks that the containers still work afterwards, and that a queue
   signalled rarely does not keep what was cancelled from it. Needs OCaml 5
   for the library, though it uses one domain. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let n = 40_000

let timed name f =
  let t0 = Unix.gettimeofday () in
  f ();
  let dt = Unix.gettimeofday () -. t0 in
  check (Printf.sprintf "%s: %d cancellations in bounded time (%.2fs)" name n dt)
    (dt < 5.)

let () =
  ignore (Unix.alarm 120);

  let m = Lwt_multicore.Mutex.create () in
  Lwt_main.run (Lwt_multicore.Mutex.lock m);
  timed "mutex" (fun () ->
    let ps = List.init n (fun _ -> Lwt_multicore.Mutex.lock m) in
    List.iter Lwt.cancel ps);
  Lwt_multicore.Mutex.unlock m;
  check "the mutex can be taken after the storm"
    (Lwt_main.run
       (Lwt.pick
          [ Lwt.map (fun () -> true) (Lwt_multicore.Mutex.lock m);
            Lwt.map (fun () -> false) (Lwt_unix.sleep 2.) ]));

  let c = Lwt_multicore.Condition.create () in
  timed "condition" (fun () ->
    let ps = List.init n (fun _ -> Lwt_multicore.Condition.wait c) in
    List.iter Lwt.cancel ps);
  let got = Lwt_multicore.Condition.wait c in
  Lwt_multicore.Condition.signal c 42;
  check "a signal after the storm reaches the live waiter"
    (Lwt_main.run got = 42);

  let v = Lwt_multicore.create () in
  timed "shared value" (fun () ->
    let ps = List.init n (fun _ -> Lwt_multicore.await v) in
    List.iter Lwt.cancel ps);
  let w = Lwt_multicore.await v in
  Lwt_multicore.resolve v "done";
  check "a resolution after the storm reaches the live waiter"
    (Lwt_main.run w = "done");

  (* Cancelled waiters are not kept until a signal comes: cancel many, in
     rounds, with no signal at all, and look at the heap. *)
  let c = Lwt_multicore.Condition.create () in
  let live () = Gc.full_major (); (Gc.quick_stat ()).Gc.live_words in
  let round () =
    let ps = List.init 1000 (fun _ -> Lwt_multicore.Condition.wait c) in
    List.iter Lwt.cancel ps
  in
  round ();
  let w0 = live () in
  for _ = 1 to 50 do round () done;
  let w1 = live () in
  check "cancelled waiters are compacted away without a signal"
    (w1 - w0 < 10_000);

  if !failures > 0 then exit 1;
  print_endline "cancellation storms are linear: ok"
