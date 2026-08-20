(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A signal is an event of the whole process, so the question is not which loop
   owns it but which loops asked for it. Every subscribed loop is woken, each on
   its own notification channel, which is what lets signals work without any loop
   relaying to another.

   The discriminating property is exactly that: TWO loops subscribed to the same
   signal both receive it. With one subscriber slot per signal rather than one per
   channel, only the last subscriber would.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let arrived = Atomic.make 0
let got_a = Atomic.make false
let got_b = Atomic.make false

(* Subscribes, waits at the rendezvous, then runs a loop long enough to be woken
   by the signal the main domain sends once both are subscribed. *)
let subscriber flag =
  let id = Lwt_unix.on_signal Sys.sigusr2 (fun _ -> Atomic.set flag true) in
  Atomic.incr arrived;
  while Atomic.get arrived < 2 do
    Domain.cpu_relax ()
  done;
  (* The signal is sent by whoever gets here last, so that neither subscriber can
     miss it. *)
  if Atomic.get arrived = 2 then Unix.kill (Unix.getpid ()) Sys.sigusr2;
  let deadline = 40 in
  let rec spin n =
    if Atomic.get flag || n = 0 then ()
    else begin
      Lwt_main.run (Lwt_unix.sleep 0.05);
      spin (n - 1)
    end
  in
  spin deadline;
  Lwt_unix.disable_signal_handler id;
  Atomic.get flag

let () =
  let a = Domain.spawn (fun () -> subscriber got_a) in
  let b = Domain.spawn (fun () -> subscriber got_b) in
  let ra = Domain.join a and rb = Domain.join b in
  check "the first subscribed loop received the signal" ra;
  check "the second subscribed loop received it too" rb;

  (* Unsubscribing one loop must not unsubscribe the other, so do it again with
     one of them gone. *)
  Atomic.set arrived 0;
  Atomic.set got_a false;
  Atomic.set got_b false;
  let only_a = Domain.spawn (fun () ->
    let id = Lwt_unix.on_signal Sys.sigusr2 (fun _ -> Atomic.set got_a true) in
    Unix.kill (Unix.getpid ()) Sys.sigusr2;
    let rec spin n =
      if Atomic.get got_a || n = 0 then () else begin
        Lwt_main.run (Lwt_unix.sleep 0.05); spin (n - 1)
      end
    in
    spin 40;
    Lwt_unix.disable_signal_handler id;
    Atomic.get got_a)
  in
  check "a loop subscribing after the others have gone still receives"
    (Domain.join only_a);
  check "and the departed loops did not receive anything"
    (not (Atomic.get got_b));

  if !failures > 0 then exit 1;
  print_endline "signals reach every subscribed loop: ok"
