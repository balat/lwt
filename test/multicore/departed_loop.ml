(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Work posted to a loop that leaves before running it. A lock handed to a
   waiter on such a loop was lost with it: nobody held it, and it stayed held
   for ever. An adoption whose owner leaves before attaching the callback
   waited for ever. Both must come back: the lock to its queue, the adoption
   as a rejection.

   Needs other domains, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let () =
  ignore (Unix.alarm 20);
  Sys.set_signal Sys.sigalrm
    (Sys.Signal_handle (fun _ ->
       prerr_endline "FAILED: hung waiting for a departed loop";
       Unix._exit 1));

  (* B queues a lock request, leaves its loop with the request still queued,
     and its domain ends a little later. A unlocks in between: the hand-over
     is posted to B's inbox, which B never drains. *)
  let m = Lwt_multicore.Mutex.create () in
  Lwt_main.run (Lwt_multicore.Mutex.lock m);
  let left_run = Atomic.make false and unlocked = Atomic.make false in
  let b =
    Domain.spawn (fun () ->
      Lwt_main.run
        (Lwt.async (fun () ->
           Lwt_multicore.Mutex.with_lock m (fun () -> Lwt.return_unit));
         Lwt.return_unit);
      Atomic.set left_run true;
      while not (Atomic.get unlocked) do Domain.cpu_relax () done;
      Unix.sleepf 0.05)
  in
  while not (Atomic.get left_run) do Domain.cpu_relax () done;
  Lwt_multicore.Mutex.unlock m;
  Atomic.set unlocked true;
  Domain.join b;
  check "a lock handed to a loop that left comes back"
    (not (Lwt_multicore.Mutex.is_locked m));
  let locked =
    Lwt_main.run
      (Lwt.pick
         [ Lwt.map (fun () -> true) (Lwt_multicore.Mutex.lock m);
           Lwt.map (fun () -> false) (Lwt_unix.sleep 2.) ])
  in
  check "and can be taken again" locked;

  (* An adoption whose owner's loop is retired before attaching the callback:
     rejected, not pending for ever. *)
  let ready = Atomic.make false and go = Atomic.make false in
  let p = ref (Lwt.return 0) in
  let owner =
    Domain.spawn (fun () ->
      Lwt_main.run (Lwt.bind (Lwt.pause ()) (fun () ->
        ignore (Lwt_multicore.self ());
        p := fst (Lwt.wait ());
        Lwt.return_unit));
      Atomic.set ready true;
      while not (Atomic.get go) do Domain.cpu_relax () done)
  in
  while not (Atomic.get ready) do Domain.cpu_relax () done;
  let adopted = Lwt_multicore.adopt !p in
  Atomic.set go true;
  Domain.join owner;
  let outcome =
    Lwt_main.run
      (Lwt.catch
         (fun () -> Lwt.map (fun _ -> `Resolved) adopted)
         (function
           | Lwt_multicore.Cannot_adopt -> Lwt.return `Cannot_adopt
           | _ -> Lwt.return `Other))
  in
  check "adopting from an owner that left is rejected with Cannot_adopt"
    (outcome = `Cannot_adopt);

  if !failures > 0 then exit 1;
  print_endline "work posted to a departed loop: ok"
