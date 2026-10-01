(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Creating a loop's notification channel with no descriptor left. The attempt
   must fail with EMFILE, and nothing else: in particular, the next loop to be
   created once descriptors are available again must be created. The channel
   table is guarded by a mutex, and a failure that raised while holding it hung
   every loop created or retired from then on.

   The check runs under a low descriptor limit, since exhausting the default
   one would take a while and a lot of memory: the test re-runs itself through
   a shell that lowers the limit. Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let check_under_low_limit () =
  (* Deadlocks must show as a failure, not as a hang. *)
  ignore (Unix.alarm 10);
  let held = ref [] in
  (try
     while true do
       held := Unix.openfile "/dev/null" [Unix.O_RDONLY] 0 :: !held
     done
   with Unix.Unix_error (Unix.EMFILE, _, _) -> ());
  let first =
    Domain.join
      (Domain.spawn (fun () ->
         match Lwt_unix.make_notification (fun () -> ()) with
         | _ -> `Created
         | exception Unix.Unix_error (Unix.EMFILE, _, _) -> `Emfile
         | exception _ -> `Other))
  in
  check "with no descriptor left, creating a loop fails with EMFILE"
    (first = `Emfile);
  List.iter Unix.close !held;
  let second =
    Domain.join
      (Domain.spawn (fun () ->
         match Lwt_unix.make_notification (fun () -> ()) with
         | _ -> true
         | exception _ -> false))
  in
  check "and once descriptors are back, the next loop is created" second;
  if !failures > 0 then exit 1

let () =
  match Sys.argv with
  | [| _; "--under-low-limit" |] -> check_under_low_limit ()
  | _ ->
    let child =
      Unix.create_process "sh"
        [| "sh"; "-c"; "ulimit -n 64 && exec \"$0\" --under-low-limit";
           Sys.executable_name |]
        Unix.stdin Unix.stdout Unix.stderr
    in
    let _, status = Unix.waitpid [] child in
    if status <> Unix.WEXITED 0 then exit 1;
    print_endline "loop creation under EMFILE: ok"
