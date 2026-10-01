(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* [await] with no loop running: it runs the loop until the promise settles,
   so a direct-style program needs no [Lwt_main.run] of its own. *)

let () =
  let v = Lwt_direct.await (Lwt.bind (Lwt_unix.sleep 1e-3) (fun () -> Lwt.return 42)) in
  assert (v = 42);
  (* A second one, once the first loop has returned. *)
  let w = Lwt_direct.await (Lwt.map (fun () -> "ok") (Lwt_unix.sleep 1e-3)) in
  assert (w = "ok");
  print_endline "ok   - top-level await runs the loop until the promise settles"
