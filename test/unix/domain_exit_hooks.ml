(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Exit hooks belong to a loop, so they must run ON the domain that registered
   them, and while that domain is still alive.

   Before they were per domain, a hook registered on a spawned domain went into a
   process-wide sequence and was drained by the MAIN domain at process exit: it
   ran, but on the wrong domain and after the registering one had gone. Running an
   exit hook means running an Lwt loop, so the domain matters.

   Needs a second domain, hence OCaml 5. *)

let () =
  let ran_on = Atomic.make (-1) in
  let spawned_id = Atomic.make (-2) in
  let d =
    Domain.spawn (fun () ->
      Atomic.set spawned_id (Domain.self () :> int);
      Lwt_main.at_exit (fun () ->
        Atomic.set ran_on (Domain.self () :> int);
        Lwt.return_unit))
  in
  Domain.join d;
  (* Domain.at_exit fires before the join returns, so by now the hook must have
     run, and it must have run on the spawned domain. *)
  let ran = Atomic.get ran_on and spawned = Atomic.get spawned_id in
  if ran = -1 then begin
    prerr_endline
      "the spawned domain's exit hook did not run when the domain exited";
    exit 1
  end;
  if ran <> spawned then begin
    Printf.eprintf
      "the spawned domain's exit hook ran on domain %d instead of %d\n" ran
      spawned;
    exit 1
  end;
  (* And our own hooks are ours: registering one here must not have been visible
     to the spawned domain, nor the other way round. *)
  let here = ref 0 in
  Lwt_main.at_exit (fun () -> incr here; Lwt.return_unit);
  print_endline "per-domain exit hooks: ok"
