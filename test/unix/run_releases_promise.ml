(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Once Lwt_main.run p has returned, nothing of Lwt keeps p. The idle hook it
   installs stayed installed until the next run, and it held p, and with it
   whatever p resolved to. *)

let () =
  let w : bytes Lwt.t Weak.t = Weak.create 1 in
  let run () =
    let p = Lwt.map (fun () -> Bytes.create 1_000_000) (Lwt.pause ()) in
    Weak.set w 0 (Some p);
    ignore (Sys.opaque_identity (Lwt_main.run p))
  in
  run ();
  Gc.full_major ();
  if Weak.check w 0 then begin
    prerr_endline "FAILED: the promise of the last run is still reachable";
    exit 1
  end;
  print_endline "a run releases its promise: ok"
