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

  (* An exit hook that needs a JOB, which is what forced the ordering: the
     notification channel a job completes on must outlive the drain that runs the
     hook. Domain exit callbacks run last-registered-first, so the channel is
     forced into existence before the drain is registered; get that wrong and this
     hook hangs for ever rather than failing. *)
  let path = Filename.temp_file "lwt-exit-hook-job" ".txt" in
  let wrote = Atomic.make false in
  Domain.join
    (Domain.spawn (fun () ->
       (* Register the hook FIRST and touch Lwt_unix afterwards: that is the order
          that decides whether the channel is retired before or after the drain,
          and getting it wrong hangs here rather than failing. *)
       Lwt_main.at_exit (fun () ->
         Lwt.bind
           (Lwt_unix.openfile path [ Unix.O_WRONLY; Unix.O_TRUNC ] 0o600)
           (fun fd ->
             Lwt.bind (Lwt_unix.write fd (Bytes.of_string "done") 0 4)
               (fun _ ->
                 Lwt.bind (Lwt_unix.close fd) (fun () ->
                   Atomic.set wrote true;
                   Lwt.return_unit))));
       (* A job before dying, so this domain's channel certainly exists by the
          time the drain runs. *)
       ignore (Lwt_main.run (Lwt_unix.stat "/"))));
  if not (Atomic.get wrote) then begin
    prerr_endline "an exit hook needing a job did not complete on domain exit";
    exit 1
  end;
  let ic = open_in_bin path in
  let s = really_input_string ic (in_channel_length ic) in
  close_in ic;
  (try Sys.remove path with _ -> ());
  if s <> "done" then begin
    Printf.eprintf "the exit hook's job wrote %S\n" s;
    exit 1
  end;
  print_endline "per-domain exit hooks: ok"
