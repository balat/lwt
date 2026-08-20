(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The service pattern and the domain pool built on it. What has to be true: a
   call gets its answer as a local promise, an exception in the handler comes back
   as a rejection, a shutdown answers what is in flight and refuses what comes
   after, and the pool actually runs work on OTHER domains.

   The last one is checked rather than assumed: the work reports the domain it ran
   on, and the test insists it was not ours.

   Needs a second domain, hence OCaml 5. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let () =
  (* A service holding state of its own: a counter no other domain touches. *)
  let counter = ref 0 in
  let svc =
    Lwt_multicore.Service.create (fun n ->
      counter := !counter + n;
      Lwt_unix.sleep 0.001 >>= fun () -> Lwt.return !counter)
  in
  let a = Lwt_main.run (Lwt_multicore.Service.call svc 5) in
  let b = Lwt_main.run (Lwt_multicore.Service.call svc 7) in
  check "calls are answered, in order, with the service's own state"
    (a = 5 && b = 12);

  (* An exception in the handler comes back as a rejection on this side. *)
  let failing = Lwt_multicore.Service.create (fun () -> Lwt.fail Not_found) in
  check "a handler's exception arrives as a rejection"
    (match Lwt_main.run (Lwt_multicore.Service.call failing ()) with
     | _ -> false
     | exception Not_found -> true
     | exception _ -> false);
  Lwt_main.run (Lwt_multicore.Service.shutdown failing);

  (* Shutdown answers what is in flight, then refuses. *)
  let pending = Lwt_multicore.Service.call svc 1 in
  Lwt_main.run (Lwt_multicore.Service.shutdown svc);
  check "a call in flight was answered" (Lwt.state pending <> Lwt.Sleep);
  check "and a later call is refused"
    (match Lwt_main.run (Lwt_multicore.Service.call svc 1) with
     | _ -> false
     | exception Lwt_multicore.Stream.Closed -> true
     | exception _ -> false);

  (* The pool runs work on other domains, in parallel. *)
  let pool = Lwt_multicore.Pool.create ~count:2 () in
  let us = (Domain.self () :> int) in
  let where =
    Lwt_main.run
      (Lwt_multicore.Pool.detach pool (fun () -> (Domain.self () :> int)) ())
  in
  check "the work ran on another domain" (where <> us);

  (* Several pieces of work, all answered, results not mixed up. *)
  let results =
    Lwt_main.run
      (Lwt_list.map_p
         (fun i -> Lwt_multicore.Pool.detach pool (fun x -> x * x) i)
         [ 1; 2; 3; 4; 5; 6; 7; 8 ])
  in
  check "every piece of work came back with its own result"
    (results = [ 1; 4; 9; 16; 25; 36; 49; 64 ]);

  (* An exception on the worker becomes a rejection here. *)
  check "an exception on the worker arrives as a rejection"
    (match Lwt_main.run (Lwt_multicore.Pool.detach pool (fun () -> raise Exit) ())
     with
     | () -> false
     | exception Exit -> true
     | exception _ -> false);

  check "the pool has the size it was asked for"
    (Lwt_multicore.Pool.size pool = 2);
  Lwt_main.run (Lwt_multicore.Pool.shutdown pool);

  if !failures > 0 then exit 1;
  print_endline "service and domain pool: ok"
