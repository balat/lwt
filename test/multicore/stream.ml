(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The bounded channel: many producers, many consumers, back-pressure, and an end
   a consumer can observe. The properties worth checking are the ones a channel is
   bought for: nothing is lost, nothing is duplicated, the bound is respected, and
   cancelling either side loses nothing.

   Needs a second domain, hence OCaml 5. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let () =
  (* One producer here, one consumer on another domain, more items than the
     capacity so that the producer really does wait. *)
  let s : int Lwt_multicore.Stream.t = Lwt_multicore.Stream.create ~capacity:2 in
  let n = 50 in
  let sum = Atomic.make 0 in
  let taken = Atomic.make 0 in
  let consumer =
    Domain.spawn (fun () ->
      let rec loop () =
        Lwt_multicore.Stream.take s >>= function
        | None -> Lwt.return_unit
        | Some v ->
          ignore (Atomic.fetch_and_add sum v);
          Atomic.incr taken;
          loop ()
      in
      Lwt_main.run (loop ()))
  in
  Lwt_main.run
    (let rec produce i =
       if i > n then Lwt.return_unit
       else Lwt_multicore.Stream.push s i >>= fun () -> produce (i + 1)
     in
     produce 1);
  Lwt_multicore.Stream.close s;
  Domain.join consumer;
  check "every item arrived exactly once"
    (Atomic.get taken = n && Atomic.get sum = n * (n + 1) / 2);

  (* The bound is real: with nobody taking, a third push must wait. *)
  let b : int Lwt_multicore.Stream.t = Lwt_multicore.Stream.create ~capacity:2 in
  check "two fit" (Lwt.state (Lwt_multicore.Stream.push b 1) = Lwt.Return ()
                   && Lwt.state (Lwt_multicore.Stream.push b 2) = Lwt.Return ());
  let blocked = Lwt_multicore.Stream.push b 3 in
  check "and the third waits" (Lwt.state blocked = Lwt.Sleep);
  check "the stream holds its capacity" (Lwt_multicore.Stream.length b = 2);
  (* Taking one lets the blocked producer through. *)
  check "taking frees a slot"
    (Lwt_main.run (Lwt_multicore.Stream.take b) = Some 1);
  check "and the waiting push goes through"
    (Lwt_main.run (blocked >>= fun () -> Lwt.return_true));
  check "order is preserved"
    (Lwt_main.run (Lwt_multicore.Stream.take b) = Some 2
     && Lwt_main.run (Lwt_multicore.Stream.take b) = Some 3);

  (* Cancelling a consumer that has already been handed an item must not lose it:
     the item goes back to the front. Provoked deterministically, as with the
     mutex: the consumer is on another domain, is served, cancels before running
     its loop, and only then runs it. *)
  let c : int Lwt_multicore.Stream.t = Lwt_multicore.Stream.create ~capacity:4 in
  let stage = Atomic.make 0 in
  let consumer =
    Domain.spawn (fun () ->
      let p = Lwt_multicore.Stream.take c in
      Atomic.set stage 1;
      while Atomic.get stage < 2 do Domain.cpu_relax () done;
      Lwt.cancel p;
      Lwt_main.run (Lwt_unix.sleep 0.05);
      Lwt.state p)
  in
  while Atomic.get stage < 1 do Domain.cpu_relax () done;
  Lwt_main.run (Lwt_multicore.Stream.push c 11);
  Atomic.set stage 2;
  check "the cancelled consumer is rejected"
    (match Domain.join consumer with
     | Lwt.Fail Lwt.Canceled -> true
     | _ -> false);
  check "and the item it was handed came back"
    (Lwt_main.run (Lwt_multicore.Stream.take c) = Some 11);

  (* Closing: consumers learn the end, items already there survive, producers are
     refused. *)
  let d : int Lwt_multicore.Stream.t = Lwt_multicore.Stream.create ~capacity:2 in
  Lwt_main.run (Lwt_multicore.Stream.push d 1);
  Lwt_multicore.Stream.close d;
  check "an item pushed before the close is still there"
    (Lwt_main.run (Lwt_multicore.Stream.take d) = Some 1);
  check "then the end is visible"
    (Lwt_main.run (Lwt_multicore.Stream.take d) = None);
  check "and pushing is refused"
    (match Lwt.state (Lwt_multicore.Stream.push d 2) with
     | Lwt.Fail Lwt_multicore.Stream.Closed -> true
     | _ -> false);
  check "closing twice is harmless"
    (match Lwt_multicore.Stream.close d with () -> true | exception _ -> false);

  (* A producer waiting for room when the stream closes is rejected, not left
     hanging. *)
  let e : int Lwt_multicore.Stream.t = Lwt_multicore.Stream.create ~capacity:1 in
  Lwt_main.run (Lwt_multicore.Stream.push e 1);
  let waiting = Lwt_multicore.Stream.push e 2 in
  check "the producer is waiting" (Lwt.state waiting = Lwt.Sleep);
  Lwt_multicore.Stream.close e;
  check "closing rejects it"
    (match Lwt.state waiting with
     | Lwt.Fail Lwt_multicore.Stream.Closed -> true
     | _ -> false);

  if !failures > 0 then exit 1;
  print_endline "bounded stream between loops: ok"
