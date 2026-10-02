(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* An exit hook that talks to another domain. Shutting a service down from
   Lwt_main.at_exit is the natural thing to do, and it needs this loop's inbox
   open while the hook runs: the service answers through it. The inbox used to
   be closed by a domain-exit callback, which runs before the drain of the
   hooks on the main domain always, and on a spawned domain whenever the handle
   was first taken inside the loop; the hook then waited for ever and the
   process never exited.

   Both cases are here. The test is that the process exits: a hang is turned
   into a failure by the alarm. Needs other domains, hence OCaml 5. *)

let () =
  ignore (Unix.alarm 20);
  Sys.set_signal Sys.sigalrm
    (Sys.Signal_handle (fun _ ->
       prerr_endline "FAILED: an exit hook waiting for another domain hung";
       Unix._exit 1));

  (* A spawned domain whose handle is first taken INSIDE its loop, and whose
     exit hook awaits a value another domain resolves. *)
  let v = Lwt_multicore.create () in
  let in_hook = Atomic.make false in
  let got = Atomic.make 0 in
  let b =
    Domain.spawn (fun () ->
      Lwt_main.run
        (Lwt.bind (Lwt.pause ()) (fun () ->
           ignore (Lwt_multicore.self ());
           Lwt.return_unit));
      Lwt_main.at_exit (fun () ->
        Atomic.set in_hook true;
        Lwt.map (fun x -> Atomic.set got x) (Lwt_multicore.await v)))
  in
  let resolver =
    Domain.spawn (fun () ->
      while not (Atomic.get in_hook) do Domain.cpu_relax () done;
      Lwt_multicore.resolve v 7)
  in
  Domain.join b;
  Domain.join resolver;
  if Atomic.get got <> 7 then begin
    prerr_endline "FAILED: the spawned domain's exit hook did not get the value";
    exit 1
  end;

  (* The main domain: a service shut down from an exit hook. The hook runs at
     the exit of the process, after this function returns. *)
  let svc = Lwt_multicore.Service.create (fun x -> Lwt.return (x * 2)) in
  let r = Lwt_main.run (Lwt_multicore.Service.call svc 21) in
  if r <> 42 then (prerr_endline "FAILED: the service did not answer"; exit 1);
  Lwt_main.at_exit (fun () ->
    Lwt.map
      (fun () -> print_endline "exit hooks that cross domains: ok")
      (Lwt_multicore.Service.shutdown svc))
