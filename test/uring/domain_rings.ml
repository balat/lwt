(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A ring belongs to the engine that created it, and an engine belongs to a
   domain, so the ring must too: completion-based I/O has to submit to the ring
   that ITS domain's [iter] reaps.

   The interesting case is the mixed process, checked here: one domain running
   io_uring and another running the default engine, each doing real socket I/O.
   With a shared ring the second domain would submit into the first one's ring
   and wait forever for a completion only the first domain could reap.

   Needs a second domain, hence OCaml 5. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

(* Real I/O over a socketpair, through whatever engine is installed here.
   [Lwt_unix.socketpair] rather than [Unix]'s plus [of_unix_file_descr], because
   the latter, given no [~blocking], runs a JOB to guess the mode -- and jobs
   belong to one domain. Lwt's own socket constructors all state the mode, which
   is why real code is unaffected. *)
let socket_roundtrip () =
  let fa, fb = Lwt_unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let ok =
    Lwt_main.run
      (let buf = Bytes.create 4 in
       Lwt_unix.write fb (Bytes.of_string "ping") 0 4 >>= fun _ ->
       Lwt_unix.read fa buf 0 4 >>= fun n ->
       Lwt.return (n = 4 && Bytes.to_string buf = "ping"))
  in
  (try Lwt_main.run (Lwt_unix.close fa) with _ -> ());
  (try Lwt_main.run (Lwt_unix.close fb) with _ -> ());
  ok

let no_ring_here () =
  let path = Filename.temp_file "lwt-uring-domain" ".txt" in
  let fd = Unix.openfile path [ Unix.O_RDONLY ] 0o600 in
  let refused =
    match Lwt_uring.Io.read fd (Bytes.create 4) 0 4 with
    | _ -> false
    | exception Failure m ->
      let expected = "on this domain" in
      let rec contains i =
        i + String.length expected <= String.length m
        && (String.sub m i (String.length expected) = expected
            || contains (i + 1))
      in
      contains 0
    | exception _ -> false
  in
  Unix.close fd;
  (try Sys.remove path with _ -> ());
  refused

let () =
  if not (Lwt_uring.available ()) then
    print_endline "per-domain rings: skipped (no io_uring here)"
  else begin
    Lwt_uring.set ();
    check "io_uring I/O works on the domain that installed it"
      (socket_roundtrip ());

    (* A domain with no ring of its own must not borrow ours. *)
    check "another domain has no ring of ours to submit to"
      (Domain.join (Domain.spawn no_ring_here));

    (* And it must still do real I/O, through its own default engine. *)
    check "another domain does real I/O on its own engine"
      (Domain.join (Domain.spawn socket_roundtrip));

    (* A domain may install a ring of its own; ours must survive it. *)
    check "another domain runs its own ring"
      (Domain.join
         (Domain.spawn (fun () ->
            Lwt_uring.set ();
            let ok = socket_roundtrip () in
            (* Hand the domain back to the default engine, which destroys its
               ring rather than leaking the descriptor. *)
            Lwt_engine.set (new Lwt_engine.select);
            ok)));
    check "and ours still works afterwards" (socket_roundtrip ());

    (* The deferred flags are asked for by default, and a per-domain ring is what
       makes SINGLE_ISSUER legitimate: two domains, two rings, each with one
       submitting task. Checked by doing real I/O on both with the flags on, and
       by checking that asking for a plain ring still works. *)
    check "two domains each run a ring with the deferred flags"
      (Domain.join
         (Domain.spawn (fun () ->
            Lwt_uring.set ();
            let ok = socket_roundtrip () in
            Lwt_engine.set (new Lwt_engine.select);
            ok))
       && socket_roundtrip ());
    check "and a plain ring can still be asked for"
      (Domain.join
         (Domain.spawn (fun () ->
            Lwt_uring.set ~deferred:false ();
            let ok = socket_roundtrip () in
            Lwt_engine.set (new Lwt_engine.select);
            ok)));

    if !failures > 0 then exit 1;
    print_endline "per-domain rings: ok"
  end
