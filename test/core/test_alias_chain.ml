(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A chain of a million aliases, walked through its oldest handle. Each bind's
   continuation returns the previous accumulator, still pending, which makes
   the promise of that bind an alias of the next one. Walking the chain used
   the stack in proportion to its length, and overflowed it on OCaml 4.14, as
   the historical core did; it now runs in constant stack. Runs on every
   version, 4.14 being the one where the stack is small enough to tell. *)

let n = 1_000_000

let () =
  let p0, u0 = Lwt.wait () in
  let acc = ref p0 in
  let ts = Array.init n (fun _ -> Lwt.wait ()) in
  for k = 0 to n - 1 do
    let prev = !acc in
    acc := Lwt.bind (fst ts.(k)) (fun () -> prev)
  done;
  Array.iter (fun (_, u) -> Lwt.wakeup u ()) ts;
  let ok =
    match Lwt.state p0 with
    | Lwt.Sleep -> (
      match Lwt.wakeup u0 () with
      | () -> Lwt.state !acc = Lwt.Return ()
      | exception Stack_overflow -> false)
    | _ -> false
    | exception Stack_overflow -> false
  in
  if not ok then begin
    prerr_endline "FAILED: a chain of a million aliases";
    exit 1
  end;
  print_endline "a chain of a million aliases: ok"
