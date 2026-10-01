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
