(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* An event source belongs to a loop, so each domain must get its own engine and
   neither must see the other's registrations.

   This is safe on the libev side because Lwt's binding calls ev_loop_new rather
   than the default loop, which is what makes several loops in one process
   legitimate. That is what this exercises for real.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let () =
  let mine = Lwt_engine.get () in
  (* a fresh domain builds its own engine rather than inheriting ours *)
  let theirs = Domain.join (Domain.spawn (fun () -> Lwt_engine.get ())) in
  check "a fresh domain gets its own engine" (mine != theirs);

  (* registrations are per engine, so a fresh domain must see none of ours *)
  let r, w = Unix.pipe ~cloexec:true () in
  let event = Lwt_engine.on_readable r (fun _ -> ()) in
  check "we see our own registration" (Lwt_engine.readable_count () = 1);
  let seen =
    Domain.join (Domain.spawn (fun () -> Lwt_engine.readable_count ()))
  in
  check "a fresh domain sees none of our registrations" (seen = 0);

  (* and a registration made over there must not appear here *)
  let r2, w2 = Unix.pipe ~cloexec:true () in
  let over_there =
    Domain.join
      (Domain.spawn (fun () ->
         let _ = Lwt_engine.on_readable r2 (fun _ -> ()) in
         Lwt_engine.readable_count ()))
  in
  check "the other domain sees its own registration" (over_there = 1);
  check "and we still see only ours" (Lwt_engine.readable_count () = 1);

  Lwt_engine.stop_event event;
  List.iter (fun fd -> try Unix.close fd with _ -> ()) [ r; w; r2; w2 ];

  (* A spawned domain that installs its own engine, then exits. What the exit
     must destroy is the engine the domain has THEN, not the one it started
     with: that one is already destroyed by [set], and destroying it again freed
     its libev loop twice (an abort in malloc), while the installed one leaked
     its epoll descriptor. With [~destroy:false] the initial engine is the one
     left aside; the exit destroys both, since nothing of the domain can use
     either any more. Counted on /proc where there is one; elsewhere the test
     still checks that nothing crashes. *)
  let open_descriptors () =
    match Sys.readdir "/proc/self/fd" with
    | entries -> Some (Array.length entries)
    | exception Sys_error _ -> None
  in
  let engines =
    (fun () -> (new Lwt_engine.select :> Lwt_engine.t))
    :: (if Lwt_sys.have `libev then
          [ (fun () -> (new Lwt_engine.libev () :> Lwt_engine.t)) ]
        else [])
  in
  List.iter
    (fun destroy ->
       List.iter
         (fun make ->
            let before = open_descriptors () in
            for _ = 1 to 20 do
              Domain.join
                (Domain.spawn (fun () ->
                   Lwt_engine.set ~destroy (make ());
                   Lwt_main.run (Lwt_unix.sleep 0.001)))
            done;
            let after = open_descriptors () in
            check
              (Printf.sprintf
                 "domains that set their engine (destroy:%b) and exit leak \
                  no descriptor" destroy)
              (match before, after with
               | Some b, Some a -> a <= b
               | _ -> true))
         engines)
    [ true; false ];

  if !failures > 0 then exit 1;
  print_endline "per-domain engines: ok"
