(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* [adopt] is the escape hatch for a promise you cannot change the making of. The
   claim to check is that it works WITHOUT touching the foreign promise from here:
   binding it directly raises Lwt.Foreign_promise, and this test asserts both
   halves side by side, so the difference is the point.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let ready = Atomic.make 0
let their_loop = Atomic.make None

let () =
  (* A promise made and owned by another domain, still pending, plus the resolver
     kept over there. The other domain runs its loop so that it can service what
     adopt posts to it. *)
  let handed : (int Lwt.t * int Lwt.u) option ref = ref None in
  let mutex = Mutex.create () in
  let other =
    Domain.spawn (fun () ->
      (* Take a handle, which is what makes this loop adoptable at all. *)
      Atomic.set their_loop (Some (Lwt_multicore.self ()));
      let p, u = Lwt.wait () in
      Mutex.lock mutex;
      handed := Some (p, u);
      Mutex.unlock mutex;
      Atomic.incr ready;
      (* Serve our loop for a while: this is where adopt's posted thunk runs, and
         where the promise is resolved. *)
      Lwt_main.run
        (Lwt.bind (Lwt_unix.sleep 0.05) (fun () ->
           Lwt.wakeup u 42;
           Lwt_unix.sleep 0.2)))
  in
  while Atomic.get ready < 1 do
    Domain.cpu_relax ()
  done;
  Mutex.lock mutex;
  let theirs = match !handed with Some (p, _) -> p | None -> assert false in
  Mutex.unlock mutex;

  (* Half one: touching it from here is refused, which is why adopt exists. *)
  check "binding a foreign pending promise is refused"
    (match Lwt.bind theirs (fun v -> Lwt.return v) with
     | _ -> false
     | exception Lwt.Foreign_promise -> true
     | exception _ -> false);

  (* Half two: adopting it works, and gives an ordinary local promise. *)
  let mine = Lwt_multicore.adopt theirs in
  let got = Lwt_main.run (Lwt.bind mine (fun v -> Lwt.return (v + 1))) in
  check "the adopted promise carries the value" (got = 43);
  Domain.join other;

  (* An already-resolved foreign promise needs no adopting, and says so by being
     returned unchanged. *)
  let settled = Domain.join (Domain.spawn (fun () -> Lwt.return 7)) in
  check "a resolved foreign promise is returned as is"
    (Lwt_multicore.adopt settled == settled);

  (* Nor does one of ours. *)
  let ours, _ = Lwt.wait () in
  check "one of our own is returned as is" (Lwt_multicore.adopt ours == ours);

  (* And a promise whose owner has gone cannot be adopted, explicitly. *)
  let orphan =
    Domain.join
      (Domain.spawn (fun () ->
         ignore (Lwt_multicore.self ());
         fst (Lwt.wait ())))
  in
  check "a promise whose loop has gone cannot be adopted"
    (match Lwt_multicore.adopt orphan with
     | _ -> false
     | exception Lwt_multicore.Cannot_adopt -> true
     | exception _ -> false);

  if !failures > 0 then exit 1;
  print_endline "adopt: the owner attaches, we wait locally: ok"
