(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* SOAK: domains spawned and let die, over and over, each one doing real work
   before it goes. This is the test that finds what a single pass cannot: a
   descriptor leaked per domain, a notification channel never retired, a worker
   thread never joined, a signal subscription outliving its loop. Each of those
   would pass every other test in the suite and then exhaust the process.

   It checks the two things that accumulate, at the end and against the start:
   the number of open descriptors, and the number of live threads.

   The budget is deliberately small so that this actually runs in the suite;
   LWT_SOAK_SECONDS makes it as long as you like:

     LWT_SOAK_SECONDS=60 dune exec test/unix/domain_soak.exe

   Needs real domains, hence OCaml 5. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let budget =
  match Sys.getenv_opt "LWT_SOAK_SECONDS" with
  | Some s -> (try float_of_string s with _ -> 1.5)
  | None -> 1.5

(* Counted from /proc, which is exact and needs no privileges. *)
let open_descriptors () =
  match Sys.readdir "/proc/self/fd" with
  | entries -> Array.length entries
  | exception _ -> -1

let live_threads () =
  match Sys.readdir "/proc/self/task" with
  | entries -> Array.length entries
  | exception _ -> -1

(* One generation: a domain that uses everything a loop can own, then dies. *)
let generation i =
  Domain.join
    (Domain.spawn (fun () ->
       (* Its own channel and its own jobs. *)
       let path = Filename.temp_file "lwt-soak" ".txt" in
       let work =
         Lwt_unix.openfile path [ Unix.O_WRONLY; Unix.O_TRUNC ] 0o600
         >>= fun fd ->
         Lwt_unix.write fd (Bytes.of_string "soak") 0 4 >>= fun _ ->
         Lwt_unix.close fd
       in
       (* Its own socket I/O. *)
       let io =
         let a, b = Lwt_unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
         let buf = Bytes.create 4 in
         Lwt_unix.write b (Bytes.of_string "ping") 0 4 >>= fun _ ->
         Lwt_unix.read a buf 0 4 >>= fun _ ->
         Lwt_unix.close a >>= fun () -> Lwt_unix.close b
       in
       (* Its own preemptive worker, which is the one that pins a domain if it is
          not ended properly. *)
       let detached = Lwt_preemptive.detach (fun x -> x * 2) i in
       (* Its own signal subscription, dropped with the loop. *)
       let sub = Lwt_unix.on_signal Sys.sigusr2 (fun _ -> ()) in
       (* Its own exit hook, which runs an Lwt loop as it goes. *)
       Lwt_main.at_exit (fun () -> Lwt_unix.sleep 0.001);
       let n = Lwt_main.run (Lwt.both work io >>= fun _ -> detached) in
       Lwt_unix.disable_signal_handler sub;
       (try Sys.remove path with _ -> ());
       assert (n = i * 2)))

let () =
  (* One generation first, so that the baseline includes everything that is
     allocated once per process rather than once per domain. *)
  generation 1;
  Gc.full_major ();
  let fd0 = open_descriptors () and th0 = live_threads () in
  let t0 = Unix.gettimeofday () in
  let generations = ref 0 in
  while Unix.gettimeofday () -. t0 < budget do
    incr generations;
    generation !generations
  done;
  Gc.full_major ();
  let fd1 = open_descriptors () and th1 = live_threads () in
  Printf.printf "%d generations in %.1fs: descriptors %d -> %d, threads %d -> %d\n"
    !generations budget fd0 fd1 th0 th1;
  check "several generations ran" (!generations >= 3);
  (* A slack of a couple, since the runtime may keep a thread or a pipe of its own
     around; a leak PER DOMAIN would show as tens or hundreds. *)
  check "descriptors did not accumulate" (fd1 <= fd0 + 2);
  check "threads did not accumulate" (th1 <= th0 + 2);
  if !failures > 0 then exit 1;
  print_endline "soak, domains coming and going: ok"
