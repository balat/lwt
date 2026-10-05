(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A domain that never runs Lwt can resolve a shared value, unlock a mutex,
   release a semaphore and push to a stream without becoming a loop. Each of
   these used to build the calling domain's loop handle just to compare it with
   the waiter's: an inbox, a notification, hence a notification channel and an
   engine, two descriptors on Linux, for a domain that will never drain them.

   What a loop costs is visible in the descriptors it opens, so this counts
   /proc/self/fd, where there is one. Needs OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let descriptors () = Array.length (Sys.readdir "/proc/self/fd")

let () =
  if not (Sys.file_exists "/proc/self/fd") then begin
    print_endline "no /proc/self/fd here: skipped";
    exit 0
  end;
  ignore (Unix.alarm 30);
  let v = Lwt_multicore.create () in
  let m = Lwt_multicore.Mutex.create () in
  let s = Lwt_multicore.Semaphore.create 0 in
  let st = Lwt_multicore.Stream.create ~capacity:4 in
  (* Waiters of this loop, so that the other domain serves them across. *)
  Lwt_main.run (Lwt_multicore.Mutex.lock m);
  let got_v = Lwt_multicore.await v in
  let got_m = Lwt_multicore.Mutex.lock m in
  let got_s = Lwt_multicore.Semaphore.acquire s in
  let got_st = Lwt_multicore.Stream.take st in
  let before, after =
    Domain.join
      (Domain.spawn (fun () ->
         let before = descriptors () in
         Lwt_multicore.resolve v 42;
         Lwt_multicore.Mutex.unlock m;
         Lwt_multicore.Semaphore.release s;
         ignore (Lwt_multicore.Stream.push st 7);
         ignore (Lwt_multicore.Stream.push st 8);
         let taken = Lwt_multicore.Stream.take st in
         ignore taken;
         ignore (Lwt_multicore.await v);
         (before, descriptors ())))
  in
  check
    (Printf.sprintf "no descriptor opened by a domain without a loop (%d -> %d)"
       before after)
    (after = before);
  (* And what they did reached this loop. *)
  let all =
    Lwt_main.run
      (Lwt.pick
         [ Lwt.map (fun _ -> true)
             (Lwt.join
                [ Lwt.map ignore got_v; got_m; got_s; Lwt.map ignore got_st ]);
           Lwt.map (fun () -> false) (Lwt_unix.sleep 3.) ])
  in
  check "every waiter here was served" all;
  if !failures > 0 then exit 1;
  print_endline "a domain without a loop builds none: ok"
