(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* WHAT THE OWNERSHIP CHECK PROTECTS, AND WHAT IT DOES NOT.

   The check is on pending promises: a resolved promise is immutable and has no
   owner, so it is shared and read from anywhere, deliberately. That leaves a
   question a reader is entitled to ask: what if a resolved promise carries
   something that is NOT safe to share?

   This test answers it in all three cases, so that the boundary is written down
   rather than discovered:

   - it carries data: nothing to protect, and nothing is refused;
   - it carries a domain-affine container: the promise crosses, but USING the
     container does not, and the container's own check says so;
   - it carries an ordinary mutable value: NOTHING catches that, and the test
     asserts it, because a known gap is worth more than a surprise.

   The study's level-2 sanitizer (annexe B.8) is what would catch the third case,
   by keeping the owner on promises that were ever pending. It is not implemented;
   see the S6 journal for the reasoning and the cost.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let on_other_domain f = Domain.join (Domain.spawn f)

let refused f =
  match f () with
  | _ -> false
  | exception Invalid_argument _ -> true
  | exception Lwt.Foreign_promise -> true
  | exception _ -> false

let () =
  (* 1. A resolved promise carrying data crosses freely, and that is the point:
     it is how a computed constant is shared between loops. *)
  let answer = Lwt.return 42 in
  check "a resolved promise carrying data is readable anywhere"
    (on_other_domain (fun () ->
       match Lwt.state answer with Lwt.Return 42 -> true | _ -> false));
  check "and binding it elsewhere works too"
    (on_other_domain (fun () ->
       match Lwt.state (Lwt.bind answer (fun v -> Lwt.return (v + 1))) with
       | Lwt.Return 43 -> true
       | _ -> false));

  (* 2. A resolved promise carrying a domain-affine container also crosses. What
     does not cross is USING the container, and that is caught. *)
  let path = Filename.temp_file "lwt-shared" ".txt" in
  let fd =
    Lwt_unix.of_unix_file_descr ~blocking:false ~set_flags:false
      (Unix.openfile path [ Unix.O_WRONLY ] 0o600)
  in
  let channel = Lwt.return (Lwt_io.of_fd ~mode:Lwt_io.output fd) in
  check "the promise carrying a channel crosses"
    (on_other_domain (fun () ->
       match Lwt.state channel with Lwt.Return _ -> true | _ -> false));
  check "but using that channel elsewhere is refused"
    (on_other_domain (fun () ->
       match Lwt.state channel with
       | Lwt.Return oc -> refused (fun () -> Lwt_io.write oc "x")
       | _ -> false));

  (* 3. And the gap, asserted rather than hidden: an ordinary mutable value in a
     resolved promise is shared with no complaint from anyone. *)
  let counter = Lwt.return (ref 0) in
  check "a mutable value in a resolved promise is NOT protected"
    (on_other_domain (fun () ->
       match Lwt.state counter with
       | Lwt.Return r ->
         (* No exception: this is the hole. Whoever writes this has to know. *)
         incr r;
         !r = 1
       | _ -> false));

  (try Lwt_main.run (Lwt_unix.close fd) with _ -> ());
  (try Sys.remove path with _ -> ());
  if !failures > 0 then exit 1;
  print_endline "what ownership protects in a resolved promise: ok"
