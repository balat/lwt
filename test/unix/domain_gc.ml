(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* [Lwt_gc] needs no per-domain work of its own, and this test is here to say
   that on purpose: it registers a notification and its finaliser sends it, so
   giving every loop its own channel is what already makes the deferred function
   run on the loop that asked for it.

   The second half fixes the honest limit. A finaliser whose registering domain
   has gone is adopted by another domain, and the notification it sends names a
   channel that was retired, so it is dropped: the function is not run, and
   nothing crashes either. Better to have that written down and tested than
   discovered.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let ran_on = Atomic.make (-1)
let ran_after_death = Atomic.make false

let () =
  (* The deferred function runs on the domain that registered it. *)
  let spawned =
    Domain.join
      (Domain.spawn (fun () ->
         let mine = (Domain.self () :> int) in
         (let r = ref 0 in
          Lwt_gc.finalise
            (fun _ ->
              Atomic.set ran_on (Domain.self () :> int);
              Lwt.return_unit)
            r;
          ignore (Sys.opaque_identity r));
         Gc.full_major ();
         Lwt_main.run (Lwt_unix.sleep 0.05);
         mine))
  in
  check "the deferred function ran on the domain that registered it"
    (Atomic.get ran_on = spawned);

  (* And one registered by a domain that then dies is dropped, quietly. *)
  Domain.join
    (Domain.spawn (fun () ->
       let r = ref 0 in
       Lwt_gc.finalise
         (fun _ -> Atomic.set ran_after_death true; Lwt.return_unit)
         r;
       ignore (Sys.opaque_identity r)));
  Gc.full_major ();
  Lwt_main.run (Lwt_unix.sleep 0.05);
  check "a finaliser of a departed domain is dropped rather than misrouted"
    (not (Atomic.get ran_after_death));

  if !failures > 0 then exit 1;
  print_endline "Lwt_gc follows the domain that registered: ok"
