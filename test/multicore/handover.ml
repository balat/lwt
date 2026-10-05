(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A waiter served by another domain, then cancelled by its own before its loop
   has run: what it was given must come back AT ONCE, and only once.

   It used to stay with the cancelled waiter until that loop drained its inbox
   and ran the posted wake-up, which found the promise cancelled and only then
   handed the resource on: a loop busy in a long callback kept a shared mutex
   held for every domain. Now the race between serving and cancelling is decided
   on the spot, and the posted wake-up finds nothing left to do.

   No loop runs on this domain between the serving and the cancellation, which
   is what keeps the wake-up pending; the loop is run afterwards, to check that
   the late wake-up neither delivers nor hands back a second time. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let elsewhere f = Domain.join (Domain.spawn f)

(* Lets the posted wake-ups run. *)
let drain () = Lwt_main.run (Lwt_unix.sleep 0.05)

let resolved p = match Lwt.state p with Lwt.Return _ -> true | _ -> false

let () =
  ignore (Unix.alarm 30);

  (* A mutex, with nobody else waiting: it is free again at once. *)
  let m = Lwt_multicore.Mutex.create () in
  Lwt_main.run (Lwt_multicore.Mutex.lock m);
  let p = Lwt_multicore.Mutex.lock m in
  elsewhere (fun () -> Lwt_multicore.Mutex.unlock m);
  check "the waiter was served, not yet woken" (Lwt.is_sleeping p);
  Lwt.cancel p;
  check "mutex: free as soon as the served waiter is cancelled"
    (not (Lwt_multicore.Mutex.is_locked m));
  drain ();
  check "mutex: still free once the late wake-up has run"
    (not (Lwt_multicore.Mutex.is_locked m));
  check "mutex: and it can be taken" (resolved (Lwt_multicore.Mutex.lock m));
  Lwt_multicore.Mutex.unlock m;

  (* A mutex with a second waiter: it gets the lock at once. *)
  Lwt_main.run (Lwt_multicore.Mutex.lock m);
  let p1 = Lwt_multicore.Mutex.lock m in
  let p2 = Lwt_multicore.Mutex.lock m in
  elsewhere (fun () -> Lwt_multicore.Mutex.unlock m);
  Lwt.cancel p1;
  check "mutex: the next waiter has the lock at once" (resolved p2);
  drain ();
  check "mutex: and keeps it after the late wake-up"
    (Lwt_multicore.Mutex.is_locked m && resolved p2);
  Lwt_multicore.Mutex.unlock m;
  check "mutex: released once, free" (not (Lwt_multicore.Mutex.is_locked m));

  (* A semaphore: the unit is counted again at once, and only once. *)
  let s = Lwt_multicore.Semaphore.create 0 in
  let p = Lwt_multicore.Semaphore.acquire s in
  elsewhere (fun () -> Lwt_multicore.Semaphore.release s);
  Lwt.cancel p;
  check "semaphore: the unit is back at once"
    (Lwt_multicore.Semaphore.available s = 1);
  drain ();
  check "semaphore: and counted once"
    (Lwt_multicore.Semaphore.available s = 1);

  (* A stream: an item handed to a cancelled taker is back in front at once. *)
  let st = Lwt_multicore.Stream.create ~capacity:2 in
  let p = Lwt_multicore.Stream.take st in
  elsewhere (fun () -> ignore (Lwt_multicore.Stream.push st 42));
  Lwt.cancel p;
  check "stream: the item is back at once" (Lwt_multicore.Stream.length st = 1);
  drain ();
  check "stream: and there once" (Lwt_multicore.Stream.length st = 1);
  check "stream: it is the item that was pushed"
    (Lwt.state (Lwt_multicore.Stream.take st) = Lwt.Return (Some 42));

  if !failures > 0 then exit 1;
  print_endline "a served waiter that is cancelled gives back at once: ok"
