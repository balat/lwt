(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A signal arriving while the loop's notification buffer is full.

   The signal handler runs in async-signal context, where nothing may allocate:
   it used to grow the full buffer with malloc, which deadlocks if the signal
   interrupted malloc itself. It now marks the signal pending on the channel,
   and the next drain delivers it. This checks that the signal still reaches
   its Lwt handler, and that the notifications already buffered are all
   delivered too.

   The buffer starts with 4096 cells and never shrinks, so in a fresh process
   4096 notifications sent while the loop does not run fill it exactly. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let cells = 4096

let () =
  ignore (Unix.alarm 30);
  let received = ref 0 in
  let signalled = ref 0 in
  let id = Lwt_unix.make_notification (fun () -> incr received) in
  let handler =
    Lwt_unix.on_signal Sys.sigusr1 (fun _ -> incr signalled)
  in
  for _ = 1 to cells do
    Lwt_unix.send_notification id
  done;
  (* The buffer is full: the handler cannot store this one. *)
  Unix.kill (Unix.getpid ()) Sys.sigusr1;
  let rec wait n =
    if (!signalled > 0 && !received = cells) || n = 0 then Lwt.return_unit
    else Lwt.bind (Lwt_unix.sleep 0.01) (fun () -> wait (n - 1))
  in
  Lwt_main.run (wait 300);
  check "the signal reaches its handler" (!signalled >= 1);
  check
    (Printf.sprintf "every buffered notification is delivered (%d)" !received)
    (!received = cells);

  (* And the buffer still grows, outside a handler. *)
  for _ = 1 to cells + 1 do
    Lwt_unix.send_notification id
  done;
  received := 0;
  let rec wait n =
    if !received = cells + 1 || n = 0 then Lwt.return_unit
    else Lwt.bind (Lwt_unix.sleep 0.01) (fun () -> wait (n - 1))
  in
  Lwt_main.run (wait 300);
  check "more notifications than the buffer held are all delivered"
    (!received = cells + 1);

  Lwt_unix.disable_signal_handler handler;
  Lwt_unix.stop_notification id;
  if !failures > 0 then exit 1;
  print_endline "a signal with the notification buffer full: ok"
