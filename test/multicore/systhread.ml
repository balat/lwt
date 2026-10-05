(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The shared primitives are callable "from any domain and any thread". From a
   system thread of the waiter's OWN domain, they used to resolve the waiter on
   the spot, since only the domain was compared: the callbacks ran on that
   thread, beside the loop, and the loop, asleep in its engine, was not woken.
   Each wait below would then end only when its timeout woke the loop.

   Lwt_preemptive's workers are the natural callers. Needs OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

(* Runs [act] on a system thread of this domain after a short delay, while the
   loop waits for [p]; reports what [p] gave and how long the wait took. *)
let from_a_thread p act =
  let th = Thread.create (fun () -> Thread.delay 0.1; act ()) () in
  let t0 = Unix.gettimeofday () in
  let got =
    Lwt_main.run
      (Lwt.pick
         [ Lwt.map Option.some p;
           Lwt.map (fun () -> None) (Lwt_unix.sleep 3.) ])
  in
  Thread.join th;
  (got, Unix.gettimeofday () -. t0)

let prompt name (got, dt) expected =
  check (Printf.sprintf "%s: the loop is woken (%.2fs)" name dt)
    (got = Some expected && dt < 1.5)

let () =
  ignore (Unix.alarm 60);

  let v = Lwt_multicore.create () in
  prompt "shared value"
    (from_a_thread (Lwt_multicore.await v)
       (fun () -> Lwt_multicore.resolve v 42))
    42;

  let m = Lwt_multicore.Mutex.create () in
  Lwt_main.run (Lwt_multicore.Mutex.lock m);
  prompt "mutex"
    (from_a_thread
       (Lwt.map (fun () -> "locked") (Lwt_multicore.Mutex.lock m))
       (fun () -> Lwt_multicore.Mutex.unlock m))
    "locked";

  let s = Lwt_multicore.Stream.create ~capacity:1 in
  prompt "stream"
    (from_a_thread (Lwt_multicore.Stream.take s)
       (fun () -> ignore (Lwt_multicore.Stream.push s 7)))
    (Some 7);

  (* And from a worker of Lwt_preemptive, the case this is for. *)
  let v = Lwt_multicore.create () in
  let t0 = Unix.gettimeofday () in
  let got =
    Lwt_main.run
      (Lwt.pick
         [ Lwt.bind
             (Lwt_preemptive.detach
                (fun () -> Thread.delay 0.1; Lwt_multicore.resolve v "done")
                ())
             (fun () -> Lwt.map Option.some (Lwt_multicore.await v));
           Lwt.map (fun () -> None) (Lwt_unix.sleep 3.) ])
  in
  check
    (Printf.sprintf "from a Lwt_preemptive worker (%.2fs)"
       (Unix.gettimeofday () -. t0))
    (got = Some "done");

  if !failures > 0 then exit 1;
  print_endline "resolving from a system thread of the loop's domain: ok"
