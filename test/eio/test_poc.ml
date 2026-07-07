(* End-to-end PoC: Lwt running on top of Eio's runtime (Level B).

   Forces Eio's io_uring backend ([Eio_linux.run]) and, inside a single shared
   event loop, checks:
     1. Lwt's own read/write ride Eio's io_uring ring (completion path).
     2. Lwt calls into Eio (run_eio).
     3. Eio calls into Lwt (await_lwt / run_lwt).
     4. Multicore: offload pure work to another domain via Eio, driven from Lwt.
     5. Cancelling a blocked Lwt read tears down the in-flight Eio op. *)

module L = Lwt_eio_backend

let section name = Printf.printf "\n== %s ==\n%!" name

(* Pure CPU work for the multicore test. *)
let rec fib n = if n < 2 then n else fib (n - 1) + fib (n - 2)

let () =
  Eio_linux.run @@ fun env ->
  let clock = Eio.Stdenv.clock env in
  let dmgr = Eio.Stdenv.domain_mgr env in
  L.with_event_loop ~clock @@ fun () ->
  L.enable_completion_io ();

  (* 1. Lwt's own read/write ride Eio's io_uring ring. *)
  section "1. Lwt I/O on Eio's ring (socketpair ping-pong)";
  L.reset_eio_op_count ();
  L.run_lwt (fun () ->
      let open Lwt.Infix in
      let a, b = Lwt_unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
      Lwt_unix.set_blocking a false;
      Lwt_unix.set_blocking b false;
      let msg = Bytes.of_string "ping!!!!" in
      let buf = Bytes.create 8 in
      let rec loop i =
        if i = 0 then Lwt.return_unit
        else
          Lwt_unix.write a msg 0 8 >>= fun w ->
          assert (w = 8);
          Lwt_unix.read b buf 0 8 >>= fun r ->
          assert (r = 8 && Bytes.equal buf msg);
          loop (i - 1)
      in
      loop 1000 >>= fun () ->
      Lwt_unix.close a >>= fun () -> Lwt_unix.close b);
  Printf.printf "ok: 1000 round-trips; Eio completion ops used = %d\n%!"
    (L.eio_op_count ());
  assert (L.eio_op_count () >= 2000);

  (* 2. Lwt -> Eio: call Eio code from within a Lwt computation. *)
  section "2. Call Eio from Lwt (run_eio)";
  let v =
    L.run_lwt (fun () ->
        let open Lwt.Infix in
        L.run_eio (fun () ->
            Eio.Fiber.yield ();
            41)
        >>= fun x -> Lwt.return (x + 1))
  in
  Printf.printf "ok: run_eio returned %d\n%!" v;
  assert (v = 42);

  (* 3. Eio -> Lwt: await a Lwt promise from the Eio side. *)
  section "3. Call Lwt from Eio (await_lwt)";
  let s = L.Promise.await_lwt (Lwt.return "hello-from-lwt") in
  Printf.printf "ok: await_lwt returned %S\n%!" s;
  assert (s = "hello-from-lwt");

  (* 4. Multicore: run pure work on another domain via Eio, driven from Lwt. *)
  section "4. Multicore offload (Eio Domain_manager) driven from Lwt";
  let n = 34 in
  let r =
    L.run_lwt (fun () ->
        L.run_eio (fun () -> Eio.Domain_manager.run dmgr (fun () -> fib n)))
  in
  Printf.printf "ok: parallel fib %d = %d\n%!" n r;
  assert (r = fib n);

  (* 5. Cancel a blocked Lwt read (backed by an in-flight Eio op). *)
  section "5. Cancel a blocked Lwt read (tears down the Eio op)";
  let outcome =
    L.run_lwt (fun () ->
        let open Lwt.Infix in
        let a, b = Lwt_unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
        Lwt_unix.set_blocking a false;
        Lwt_unix.set_blocking b false;
        let buf = Bytes.create 8 in
        let p = Lwt_unix.read b buf 0 8 in
        (* nothing to read; [p] is pending, backed by an Eio io_uring op *)
        Lwt.cancel p;
        Lwt.catch
          (fun () -> p >>= fun _ -> Lwt.return "read-returned")
          (function
            | Lwt.Canceled -> Lwt.return "canceled"
            | e -> Lwt.reraise e)
        >>= fun res ->
        Lwt_unix.close a >>= fun () ->
        Lwt_unix.close b >>= fun () -> Lwt.return res)
  in
  Printf.printf "ok: cancelled read -> %s\n%!" outcome;
  assert (outcome = "canceled");

  section "ALL PASSED";
  Printf.printf "Lwt on Eio (Level B) validated end to end.\n%!"
