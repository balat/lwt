(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The ownership check, which is ALWAYS ON: mutating a pending promise from a
   domain other than its creator raises Lwt.Foreign_promise instead of silently
   corrupting the owner's waiter list.

   The point of this test is as much what must NOT raise as what must. A resolved
   promise has no owner and stays shareable, which is what makes module-level
   constants such as Lwt.return_unit safe by construction; and reads of a pending
   promise are deliberately not checked. *)

let failures = ref 0

let must_raise name f =
  match f () with
  | _ ->
    Printf.eprintf "FAILED: %s did not raise\n" name;
    incr failures
  | exception Lwt.Foreign_promise -> ()
  | exception e ->
    Printf.eprintf "FAILED: %s raised %s instead of Foreign_promise\n" name
      (Printexc.to_string e);
    incr failures

let must_not_raise name f =
  match f () with
  | _ -> ()
  | exception e ->
    Printf.eprintf "FAILED: %s raised %s\n" name (Printexc.to_string e);
    incr failures

(* Runs [f] on a fresh domain and returns what it produced, so that a raise there
   is reported here rather than being swallowed. *)
let on_another_domain f = Domain.join (Domain.spawn f)

let () =
  (* ---- what must raise: mutating a foreign PENDING promise ---- *)
  let pending, resolver = Lwt.wait () in
  let cancelable, _ = Lwt.task () in
  on_another_domain (fun () ->
    must_raise "bind on a foreign pending promise" (fun () ->
      Lwt.bind pending (fun () -> Lwt.return_unit));
    must_raise "map on a foreign pending promise" (fun () ->
      Lwt.map (fun () -> ()) pending);
    must_raise "wakeup of a foreign pending promise" (fun () ->
      Lwt.wakeup resolver ());
    must_raise "wakeup_later of a foreign pending promise" (fun () ->
      Lwt.wakeup_later resolver ());
    must_raise "try_bind on a foreign pending promise" (fun () ->
      Lwt.try_bind
        (fun () -> pending)
        (fun () -> Lwt.return_unit)
        (fun _ -> Lwt.return_unit));
    must_raise "on_success on a foreign pending promise" (fun () ->
      Lwt.on_success pending (fun () -> ()));
    must_raise "on_cancel on a foreign pending promise" (fun () ->
      Lwt.on_cancel pending (fun () -> ()));
    must_raise "cancel of a foreign cancelable promise" (fun () ->
      Lwt.cancel cancelable);
    must_raise "choose over a foreign pending promise" (fun () ->
      Lwt.choose [ pending ]));

  (* ---- what must NOT raise: the same operations at home ---- *)
  must_not_raise "bind at home" (fun () ->
    Lwt.bind pending (fun () -> Lwt.return_unit));
  must_not_raise "wakeup at home" (fun () -> Lwt.wakeup resolver ());

  (* ---- what must NOT raise: a RESOLVED promise has no owner ---- *)
  let resolved = Lwt.return 42 in
  let rejected = Lwt.fail Exit in
  on_another_domain (fun () ->
    must_not_raise "bind on a foreign resolved promise" (fun () ->
      Lwt.bind resolved (fun v -> Lwt.return (v + 1)));
    must_not_raise "bind on a foreign rejected promise" (fun () ->
      Lwt.catch (fun () -> rejected) (fun _ -> Lwt.return_unit));
    must_not_raise "Lwt.return_unit, the shared constant" (fun () ->
      Lwt.bind Lwt.return_unit (fun () -> Lwt.return_unit));
    (* and the value really does come through *)
    match Lwt.state (Lwt.bind resolved (fun v -> Lwt.return (v + 1))) with
    | Lwt.Return 43 -> ()
    | _ ->
      Printf.eprintf "FAILED: a foreign resolved bind produced the wrong value\n";
      incr failures);

  (* ---- reads of a pending promise are deliberately unchecked ---- *)
  let still_pending, _ = Lwt.wait () in
  on_another_domain (fun () ->
    must_not_raise "Lwt.state on a foreign pending promise" (fun () ->
      Lwt.state still_pending));

  if !failures > 0 then begin
    Printf.eprintf "%d ownership checks behaved wrongly\n" !failures;
    exit 1
  end;
  print_endline "ownership check: ok"
