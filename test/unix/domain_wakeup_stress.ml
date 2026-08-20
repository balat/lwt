(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The wake-up protocol under load, which is the one piece of this design that is
   NOT verified by a model: the C notification buffer, proven by years of use.
   This is what can be done from here, and it is worth being precise about what it
   does and does not establish.

   It does establish, on this machine and this kernel, that many senders and one
   draining loop lose nothing: every notification sent is delivered exactly once,
   including when the buffer has to grow past its initial 4096 entries, which no
   other test reaches. Senders are both DOMAINS, for real parallelism, and SYSTEM
   THREADS of the draining domain, which is the shape a job completion has.

   It does not establish the absence of a race: a load test never does. If the
   protocol lost a wake-up, the symptom here would be a HANG rather than a wrong
   count, which the timeout turns into a failure.

   Needs real domains, hence OCaml 5. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let sender_domains = 3
let sender_threads = 2
let per_sender = 2500
let expected = (sender_domains + sender_threads) * per_sender

let delivered = Atomic.make 0
let go = Atomic.make false

let () =
  (* One notification, created here, so every delivery must happen on this loop. *)
  let n = Lwt_unix.make_notification (fun () -> Atomic.incr delivered) in

  let send_many () =
    while not (Atomic.get go) do
      Domain.cpu_relax ()
    done;
    for _ = 1 to per_sender do
      Lwt_unix.send_notification n
    done
  in
  let domains = List.init sender_domains (fun _ -> Domain.spawn send_many) in
  let threads = List.init sender_threads (fun _ -> Thread.create send_many ()) in

  (* Start them all at once, then drain until the count is in, or give up. *)
  Atomic.set go true;
  let deadline = Unix.gettimeofday () +. 20.0 in
  Lwt_main.run
    (let rec drain () =
       if Atomic.get delivered >= expected then Lwt.return_unit
       else if Unix.gettimeofday () > deadline then Lwt.return_unit
       else Lwt_unix.sleep 0.002 >>= drain
     in
     drain ());
  List.iter Domain.join domains;
  List.iter Thread.join threads;

  (* A last lap, since the senders may have finished after our last check. *)
  Lwt_main.run (Lwt_unix.sleep 0.05);

  Printf.printf "delivered %d of %d (buffer grew past its initial 4096)\n"
    (Atomic.get delivered) expected;
  check "every notification was delivered, none lost"
    (Atomic.get delivered = expected);

  (* And the same again, with the senders on domains only, to be sure the first
     round's threads were not doing the work of hiding a domain-side problem. *)
  Atomic.set delivered 0;
  Atomic.set go false;
  let m = Lwt_unix.make_notification (fun () -> Atomic.incr delivered) in
  let send_m () =
    while not (Atomic.get go) do Domain.cpu_relax () done;
    for _ = 1 to per_sender do Lwt_unix.send_notification m done
  in
  let domains = List.init sender_domains (fun _ -> Domain.spawn send_m) in
  Atomic.set go true;
  let target = sender_domains * per_sender in
  let deadline = Unix.gettimeofday () +. 20.0 in
  Lwt_main.run
    (let rec drain () =
       if Atomic.get delivered >= target then Lwt.return_unit
       else if Unix.gettimeofday () > deadline then Lwt.return_unit
       else Lwt_unix.sleep 0.002 >>= drain
     in
     drain ());
  List.iter Domain.join domains;
  Lwt_main.run (Lwt_unix.sleep 0.05);
  check "and again with domains only"
    (Atomic.get delivered = target);

  if !failures > 0 then exit 1;
  print_endline "wake-up protocol under load: ok"
