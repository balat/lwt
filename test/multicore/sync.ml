(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Mutex, Semaphore and Condition across domains. What has to be true of all
   three: the promise a loop waits on is its own, resolved on its own domain, and
   the resource is never lost. The last part is the one with a subtlety: serving a
   waiter is choosing it under the lock and waking it outside, and in between its
   promise may be cancelled, in which case the resource must be handed on rather
   than dropped. That case is tested explicitly.

   Needs a second domain, hence OCaml 5. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let m = Lwt_multicore.Mutex.create ()
let sem = Lwt_multicore.Semaphore.create 2
let cond : int Lwt_multicore.Condition.t = Lwt_multicore.Condition.create ()

(* A counter guarded by the shared mutex, incremented by two domains that each
   run their own loop: if the mutex did not exclude, the total would be wrong or
   the interleaving visible. *)
let counter = Atomic.make 0
let inside = Atomic.make 0
let overlaps = Atomic.make 0
let ready = Atomic.make 0

let contend rounds =
  let rec go n =
    if n = 0 then Lwt.return_unit
    else
      Lwt_multicore.Mutex.with_lock m (fun () ->
        (* If two loops are ever in here at once, this sees it. *)
        if Atomic.fetch_and_add inside 1 <> 0 then Atomic.incr overlaps;
        Atomic.incr counter;
        Lwt_unix.sleep 0.001 >>= fun () ->
        Atomic.decr inside;
        Lwt.return_unit)
      >>= fun () -> go (n - 1)
  in
  go rounds

let () =
  let rounds = 20 in
  let other =
    Domain.spawn (fun () ->
      Atomic.incr ready;
      while Atomic.get ready < 2 do Domain.cpu_relax () done;
      Lwt_main.run (contend rounds))
  in
  Atomic.incr ready;
  while Atomic.get ready < 2 do Domain.cpu_relax () done;
  Lwt_main.run (contend rounds);
  Domain.join other;
  check "every critical section ran" (Atomic.get counter = 2 * rounds);
  check "and none overlapped" (Atomic.get overlaps = 0);
  check "the mutex is free afterwards" (not (Lwt_multicore.Mutex.is_locked m));

  (* Cancelling a waiter that has just been handed the mutex must not lose it.
     Take the mutex, queue a waiter, cancel it, then release: the mutex has to
     come back free rather than stay held by nobody. *)
  Lwt_main.run (Lwt_multicore.Mutex.lock m);
  let queued = Lwt_multicore.Mutex.lock m in
  Lwt.cancel queued;
  Lwt_multicore.Mutex.unlock m;
  check "a cancelled waiter does not swallow the mutex"
    (not (Lwt_multicore.Mutex.is_locked m));

  (* The case with the subtlety, provoked deterministically rather than raced.
     A waiter on ANOTHER domain is served, which posts the wake-up to that
     domain's inbox; the waiter cancels its promise BEFORE running its loop, so
     when the posted thunk finally runs it finds nothing to resolve. The mutex has
     been handed to nobody, and must come back rather than stay held for ever. *)
  let stage = Atomic.make 0 in
  Lwt_main.run (Lwt_multicore.Mutex.lock m);
  let waiter_domain =
    Domain.spawn (fun () ->
      let p = Lwt_multicore.Mutex.lock m in
      (* Queued, since we hold the mutex. *)
      Atomic.set stage 1;
      while Atomic.get stage < 2 do Domain.cpu_relax () done;
      (* Served by now: the wake-up is sitting in our inbox, unrun. Cancel
         first. *)
      Lwt.cancel p;
      (* Now run our loop, which executes the posted thunk. *)
      Lwt_main.run (Lwt_unix.sleep 0.05);
      Lwt.state p)
  in
  while Atomic.get stage < 1 do Domain.cpu_relax () done;
  Lwt_multicore.Mutex.unlock m;
  Atomic.set stage 2;
  let final = Domain.join waiter_domain in
  check "the cancelled remote waiter is rejected"
    (match final with Lwt.Fail Lwt.Canceled -> true | _ -> false);
  check "and the mutex it was handed came back"
    (not (Lwt_multicore.Mutex.is_locked m));

  (* The semaphore bounds a shared count across loops. Two units, three takers:
     the third waits. *)
  Lwt_main.run (Lwt_multicore.Semaphore.acquire sem);
  Lwt_main.run (Lwt_multicore.Semaphore.acquire sem);
  check "the semaphore is exhausted" (Lwt_multicore.Semaphore.available sem = 0);
  let third = Lwt_multicore.Semaphore.acquire sem in
  check "and the third taker waits" (Lwt.state third = Lwt.Sleep);
  Lwt_multicore.Semaphore.release sem;
  check "releasing hands the unit straight to the waiter"
    (Lwt.state third = Lwt.Return ());
  check "so the count stays at zero"
    (Lwt_multicore.Semaphore.available sem = 0);
  Lwt_multicore.Semaphore.release sem;
  Lwt_multicore.Semaphore.release sem;
  check "and returns to two" (Lwt_multicore.Semaphore.available sem = 2);

  (* A condition, signalled from another domain, wakes a waiter here. *)
  let got = Lwt_multicore.Condition.wait cond in
  Domain.join
    (Domain.spawn (fun () -> Lwt_multicore.Condition.signal cond 99));
  check "a signal from another domain arrives here"
    (Lwt_main.run got = 99);

  (* And a broadcast reaches several loops, each on its own. *)
  Atomic.set ready 0;
  let listeners =
    List.init 2 (fun _ ->
      Domain.spawn (fun () ->
        let p = Lwt_multicore.Condition.wait cond in
        Atomic.incr ready;
        Lwt_main.run p))
  in
  while Atomic.get ready < 2 do Domain.cpu_relax () done;
  Lwt_multicore.Condition.broadcast cond 7;
  check "a broadcast reaches every waiting loop"
    (List.for_all (fun d -> Domain.join d = 7) listeners);

  if !failures > 0 then exit 1;
  print_endline "shared mutex, semaphore and condition: ok"
