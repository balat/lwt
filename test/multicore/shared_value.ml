(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* [Lwt_multicore.t] is how a result comes back from another domain. What has to
   be true: each waiting loop gets its OWN promise, resolved on its OWN domain;
   several loops can wait for the same value; a rejection travels as a rejection;
   cancelling one waiter leaves the others alone; and a waiter whose domain has
   gone is not the resolver's problem.

   The ownership check of the core is what makes the central claim testable rather
   than asserted: if any of this resolved a promise from the wrong domain, it would
   raise Lwt.Foreign_promise instead of passing.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let resolved_on_a = Atomic.make (-1)
let resolved_on_b = Atomic.make (-1)
let ready = Atomic.make 0

let () =
  (* Two loops waiting for one value, resolved by a third party: us. *)
  let v : int Lwt_multicore.t = Lwt_multicore.create () in

  let waiter which flag =
    Domain.spawn (fun () ->
      let p = Lwt_multicore.await v in
      Atomic.incr ready;
      let got = Lwt_main.run p in
      Atomic.set flag (Domain.self () :> int);
      ignore which;
      got)
  in
  let a = waiter "a" resolved_on_a and b = waiter "b" resolved_on_b in
  (* Both must be waiting before we resolve, so that neither takes the
     already-settled path: that is the case worth testing. *)
  while Atomic.get ready < 2 do
    Domain.cpu_relax ()
  done;
  Lwt_multicore.resolve v 21;
  let ga = Domain.join a and gb = Domain.join b in
  check "both loops received the value" (ga = 21 && gb = 21);
  check "each on its own domain"
    (Atomic.get resolved_on_a <> Atomic.get resolved_on_b
     && Atomic.get resolved_on_a <> (Domain.self () :> int));
  check "and resolving again is refused"
    (match Lwt_multicore.resolve v 1 with
     | () -> false
     | exception Invalid_argument _ -> true
     | exception _ -> false);

  (* Awaiting an already-settled value works too, and here at home. *)
  check "awaiting a settled value is immediate"
    (match Lwt.state (Lwt_multicore.await v) with
     | Lwt.Return 21 -> true
     | _ -> false);

  (* A rejection travels as a rejection. *)
  let bad : int Lwt_multicore.t = Lwt_multicore.create () in
  let waiting = Lwt_multicore.await bad in
  Lwt_multicore.reject bad Not_found;
  check "a rejection arrives as one"
    (match Lwt.state waiting with Lwt.Fail Not_found -> true | _ -> false);

  (* Cancelling one waiter leaves the value and the other waiters alone. *)
  let shared : int Lwt_multicore.t = Lwt_multicore.create () in
  let mine = Lwt_multicore.await shared in
  let other_domain =
    Domain.spawn (fun () ->
      let p = Lwt_multicore.await shared in
      Atomic.incr ready;
      Lwt_main.run p)
  in
  while Atomic.get ready < 3 do
    Domain.cpu_relax ()
  done;
  Lwt.cancel mine;
  check "the cancelled waiter is rejected with Canceled"
    (match Lwt.state mine with Lwt.Fail Lwt.Canceled -> true | _ -> false);
  check "the value is still pending" (Lwt_multicore.is_pending shared);
  Lwt_multicore.resolve shared 5;
  check "and the other waiter still gets it" (Domain.join other_domain = 5);

  (* A waiter whose domain has gone is skipped, not an error. *)
  let orphan : int Lwt_multicore.t = Lwt_multicore.create () in
  Domain.join (Domain.spawn (fun () -> ignore (Lwt_multicore.await orphan)));
  check "resolving with a departed waiter is not an error"
    (match Lwt_multicore.resolve orphan 1 with
     | () -> true
     | exception _ -> false);

  (* cancel is the quiet one: usable to broadcast a shutdown from anywhere. *)
  let stop : unit Lwt_multicore.t = Lwt_multicore.create () in
  Lwt_multicore.cancel stop;
  check "cancel on a settled value is a no-op"
    (match Lwt_multicore.cancel stop with
     | () -> true
     | exception _ -> false);

  if !failures > 0 then exit 1;
  print_endline "shared values: each loop its own promise: ok"
