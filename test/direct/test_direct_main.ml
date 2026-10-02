(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* [Lwt_direct.main], and what [await] does with no event loop around. A
   separate executable: these cases run outside, or end, [Lwt_main.run]. *)

let ok what = print_endline ("ok   - " ^ what)

let fail what =
  print_endline ("FAIL - " ^ what);
  exit 1

let () =
  (* The entry point of a direct-style program. *)
  let v = Lwt_direct.main (fun () -> Lwt_direct.await (Lwt_unix.sleep 1e-3); 42) in
  if v <> 42 then fail "main returns the value";
  (* A second one, once the first loop has returned. *)
  let w = Lwt_direct.main (fun () -> Lwt_direct.await (Lwt.map (fun () -> "ok") (Lwt_unix.sleep 1e-3))) in
  if w <> "ok" then fail "main runs again after the first loop ended";
  ok "main runs the loop until its function returns";
  (* An exception out of the function escapes main. *)
  (match Lwt_direct.main (fun () -> Lwt_direct.await (Lwt_unix.sleep 1e-3); raise Exit) with
   | () -> fail "the exception did not escape main"
   | exception Exit -> ok "an exception out of the function escapes main");
  (* With no loop around, await and yield refuse rather than suspend. *)
  (match Lwt_direct.await (Lwt_unix.sleep 1e-3) with
   | () -> fail "await with no loop returned"
   | exception Failure msg when String.length msg > 16 && String.sub msg 0 16 = "Lwt_direct.await" ->
     ok "await with no loop raises Failure naming itself");
  (match Lwt_direct.yield () with
   | () -> fail "yield with no loop returned"
   | exception Failure msg when String.length msg > 16 && String.sub msg 0 16 = "Lwt_direct.yield" ->
     ok "yield with no loop raises Failure naming itself")

let () =
  (* What escapes a resumed callback goes where it would have gone had the
     callback not suspended. An ordinary exception is caught by the callback's
     own wrapper: [on_success] hands it to [Lwt.async_exception_hook]. *)
  let seen = ref None in
  let hook = !Lwt.async_exception_hook in
  Lwt.async_exception_hook := (fun e -> seen := Some e);
  let p = Lwt.pause () in
  Lwt.on_success p (fun () -> Lwt_direct.await (Lwt_unix.sleep 1e-3); raise Exit);
  Lwt_main.run (Lwt_unix.sleep 0.01);
  Lwt.async_exception_hook := hook;
  (match !seen with
   | Some Exit -> ok "an exception out of a resumed on_success callback reaches the hook"
   | _ -> fail "the exception out of the resumed callback did not reach the hook");
  (* A runtime exception, which the exception filter lets through every
     wrapper, escapes Lwt_main.run as it does in Lwt without any suspension:
     the resumption must not swallow it into the hook. Last, since Lwt_main.run
     cannot clear its running flag on that path. *)
  let p = Lwt.pause () in
  Lwt.on_success p (fun () -> Lwt_direct.await (Lwt_unix.sleep 1e-3); raise Stack_overflow);
  match Lwt_main.run (Lwt_unix.sleep 0.05) with
  | () -> fail "the runtime exception out of the resumed callback did not escape"
  | exception Stack_overflow ->
    ok "a runtime exception out of a resumed callback escapes Lwt_main.run"

let () =
  (* After a runtime exception escaped Lwt_main.run, the next run must work:
     the running flag is cleared on every exception. *)
  match Lwt_main.run (Lwt.return 7) with
  | 7 -> ok "Lwt_main.run works again after a runtime exception escaped it"
  | _ -> fail "unexpected value"
  | exception Failure _ -> fail "Lwt_main.run still thinks it is running"

let () =
  (* Exit hooks run as tasks of the loop, the first one included, so they may
     await. Printed at exit. *)
  Lwt_main.at_exit (fun () ->
    Lwt_direct.await (Lwt_unix.sleep 1e-3);
    ok "an exit hook can await, from the first one on";
    Lwt.return_unit)
