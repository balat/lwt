(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A stream source written in direct style, that is one that awaits inside
   its call. Lwt_stream serialised the calls to a monadic source through the
   promise the source returned; a source that suspends before returning it
   has no promise yet, so a second reader arriving meanwhile called the source
   again: elements came out in the order the calls completed, and reaching
   the end twice raised Invalid_argument out of Lwt.wakeup. The source is now
   marked busy from the call to its return. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

(* Three readers on a source whose n-th call sleeps for a time that DECREASES
   with n: unserialised, call 3 finishes first. Concurrent readers of a stream
   at the same position all get the same element, as they always have with a
   monadic source; then the next three elements must come in the order of the
   calls. Reports the elements, and how many calls overlapped. *)
let readers make =
  let calls = ref 0 and inside = ref 0 and overlap = ref 0 in
  let enter () =
    incr calls;
    incr inside;
    overlap := max !overlap !inside;
    !calls
  in
  let leave () = decr inside in
  let s = make enter leave in
  let got = ref [] in
  let reader () =
    Lwt.pause () >>= fun () ->
    Lwt_stream.get s >|= fun x -> got := x :: !got
  in
  Lwt_main.run (Lwt.join [ reader (); reader (); reader () ]);
  let rest = Lwt_main.run (Lwt.pause () >>= fun () -> Lwt_stream.nget 3 s) in
  (!overlap, List.rev !got, rest)

let () =
  let delay n = Lwt_unix.sleep (0.03 /. float n) in
  let overlap, got, rest =
    readers (fun enter leave ->
      Lwt_stream.from (fun () ->
        let n = enter () in
        Lwt_direct.await (delay n);
        leave ();
        Lwt.return (Some n)))
  in
  check "from: an awaiting source is never called twice at once" (overlap = 1);
  check "from: the elements come out in order of the calls"
    (got = [ Some 1; Some 1; Some 1 ] && rest = [ 2; 3; 4 ]);

  let overlap, got, rest =
    readers (fun enter leave ->
      Lwt_stream.from_direct (fun () ->
        let n = enter () in
        Lwt_direct.await (delay n);
        leave ();
        Some n))
  in
  check "from_direct: an awaiting source is never called twice at once"
    (overlap = 1);
  check "from_direct: the elements come out in order of the calls"
    (got = [ Some 1; Some 1; Some 1 ] && rest = [ 2; 3; 4 ]);

  (* Two readers reaching the end of an awaiting source: both get None, and
     nothing raises. *)
  let s =
    Lwt_stream.from_direct (fun () ->
      Lwt_direct.await (Lwt_unix.sleep 0.01);
      None)
  in
  let reader () =
    Lwt.pause () >>= fun () ->
    Lwt.catch
      (fun () -> Lwt_stream.get s >|= fun x -> Ok x)
      (fun e -> Lwt.return (Error e))
  in
  let a, b = Lwt_main.run (Lwt.both (reader ()) (reader ())) in
  check "two readers at the end of an awaiting source both get None"
    (a = Ok None && b = Ok None);

  if !failures > 0 then exit 1;
  print_endline "stream sources that await: ok"
