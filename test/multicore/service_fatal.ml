(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A service whose handler lets through an exception the loop does not catch:
   since Lwt 6 the default filter leaves the runtime's exceptions alone, so a
   Stack_overflow out of a handler ends the service's loop. The service used
   to die in silence, with its calls and its shutdown pending for ever.

   Also: work posted with run_on that raises must not take the target loop
   down, nor the work posted after it. Needs other domains, hence OCaml 5. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let within seconds p =
  Lwt.pick [ Lwt.map (fun x -> Some x) p;
             Lwt.map (fun () -> None) (Lwt_unix.sleep seconds) ]

let () =
  ignore (Unix.alarm 30);

  let svc =
    Lwt_multicore.Service.create (fun n ->
      if n = 0 then raise Stack_overflow else Lwt.return (n * 2))
  in
  let outcome p =
    Lwt_main.run
      (within 5.
         (Lwt.catch
            (fun () -> p >|= fun _ -> `Answered)
            (function
              | Stack_overflow -> Lwt.return `Stack_overflow
              | Lwt_multicore.Stream.Closed -> Lwt.return `Closed
              | _ -> Lwt.return `Other)))
  in
  check "a call before the death is answered"
    (outcome (Lwt_multicore.Service.call svc 1) = Some `Answered);
  check "the fatal call is rejected with the exception"
    (outcome (Lwt_multicore.Service.call svc 0) = Some `Stack_overflow);
  check "a later call is refused or rejected, not left pending"
    (match outcome (Lwt_multicore.Service.call svc 2) with
     | Some (`Closed | `Stack_overflow) -> true
     | _ -> false);
  check "shutdown raises the exception that killed the service"
    (outcome (Lwt_multicore.Service.shutdown svc) = Some `Stack_overflow);

  (* run_on work that raises: the loop survives, the next work runs, and the
     exception reaches async_exception_hook on that domain. *)
  let seen = Atomic.make 0 and ran_after = Atomic.make false in
  let handle = Lwt_multicore.create () in
  let worker =
    Domain.spawn (fun () ->
      Lwt.async_exception_hook := (fun _ -> Atomic.incr seen);
      let stop, resolve_stop = Lwt.wait () in
      Lwt_multicore.resolve handle (Lwt_multicore.self (), resolve_stop);
      Lwt_main.run stop)
  in
  let loop, resolve_stop = Lwt_main.run (Lwt_multicore.await handle) in
  Lwt_multicore.run_on loop (fun () -> failwith "boom");
  Lwt_multicore.run_on loop (fun () ->
    Atomic.set ran_after true;
    Lwt.wakeup resolve_stop ());
  Domain.join worker;
  check "the work posted after a raising one still ran" (Atomic.get ran_after);
  check "and the exception went to the hook" (Atomic.get seen = 1);

  if !failures > 0 then exit 1;
  print_endline "a dying service and raising work: ok"
