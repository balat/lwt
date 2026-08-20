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

  (* A departed loop's subscription must go with it, and the process-wide handler
     with the last of them. Otherwise the signal keeps being swallowed by a
     handler nobody listens to, which is not a leak you notice until a program
     stops dying on SIGTERM.

     Checked from the outside, because that is the only place it shows: a child
     process subscribes on a spawned domain, lets that domain die, then sends
     itself SIGUSR1. With the subscription dropped, the default action applies and
     the child is killed by the signal. With it left behind, the child survives
     and exits 0. *)
  if Array.length Sys.argv > 1 && Sys.argv.(1) = "orphan-subscription" then begin
    Domain.join
      (Domain.spawn (fun () ->
         ignore (Lwt_unix.on_signal Sys.sigusr1 (fun _ -> ()));
         (* Run a lap so the subscription is certainly in place. *)
         Lwt_main.run (Lwt_unix.sleep 0.01)));
    Unix.kill (Unix.getpid ()) Sys.sigusr1;
    (* If we are still here, the handler outlived its loop. *)
    Unix.sleepf 0.5;
    exit 0
  end;
  let child =
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "orphan-subscription" |]
      Unix.stdin Unix.stdout Unix.stderr
  in
  let _, status = Unix.waitpid [] child in
  check "a departed loop's signal subscription goes with it"
    (match status with
     | Unix.WSIGNALED n -> n = Sys.sigusr1
     | Unix.WEXITED _ | Unix.WSTOPPED _ -> false);

  if !failures > 0 then exit 1;
  print_endline "signals reach every subscribed loop: ok"
