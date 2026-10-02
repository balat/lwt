(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



open Test
open Lwt.Infix

let timing_tests = [
  test "libev: timer delays are not too short" begin fun () ->
    let start = Unix.gettimeofday () in

    Lwt.catch
      (fun () ->
        (* Block the entire process for one second. If using libev, libev's
           notion of the current time is not updated during this period. *)
        let () = Unix.sleep 1 in

        (* At this point, libev thinks that the time is what it was about one
           second ago. Now schedule exception Lwt_unix.Timeout to be raised in
           0.5 seconds. If the implementation is incorrect, the exception will
           be raised immediately, because the 0.5 seconds will be measured
           relative to libev's "current" time of one second ago. *)
        Lwt_unix.timeout 0.5)

      (function
      | Lwt_unix.Timeout ->
        Lwt.return (Unix.gettimeofday ())
      | exn ->
        Lwt.reraise exn)

    >>= fun stop ->

    Lwt.return (stop -. start >= 1.5)
  end;
]

let tests = timing_tests

let run_tests = [
  test "Lwt_main.run: nested call" ~sequential:true begin fun () ->
    (* The test itself is already running under Lwt_main.run, so we just have to
       call it once and make sure we get an exception. *)

    (* Make sure we are running in a callback called by Lwt_main.run, not
       synchronously when the testing executable is loaded. *)
    Lwt.pause () >>= fun () ->

    try
      Lwt_main.run (Lwt.return_unit);
      Lwt.return_false
    with Failure _ ->
      Lwt.return_true
  end;

  test "Lwt_engine.id gives default" ~sequential:true begin fun () ->
    match Lwt_engine.id () with
    | Lwt_engine.Engine_id__libev eve -> Lwt.return (Lwt_engine.Ev_backend.equal eve Lwt_engine.Ev_backend.default)
    | Lwt_engine.Engine_id__select -> Lwt.return_true
    | Lwt_engine.Engine_id__poll -> Lwt.return_false (* never chosen by default *)
    | _ -> Lwt.return_false (* no way this has been extended in this test suite *)
  end;
]

let tests = tests @ run_tests

let transfer_tests = [
  (* A handle on an event must keep working through transfers to another
     engine, however many. The new engine records the registration under a
     handle of its own, and the original handle is made to stop that one. With
     a copy of it instead, the second transfer updated the new handle only, and
     stopping through the original did nothing: the registration stayed active
     on the engine. The child of a fork under io_uring, whose engine is replaced
     after a first replacement, ran into exactly that. *)
  test "transfer: an event handle survives several transfers" ~sequential:true
      ~only_if:(fun () -> not Sys.win32) begin fun () ->
    let r, w = Unix.pipe ~cloexec:true () in
    let fired = ref 0 in
    let ev = Lwt_engine.on_readable r (fun _ -> incr fired) in
    let before = Lwt_engine.readable_count () in
    let saved = Lwt_engine.get () in
    Lwt_engine.set ~destroy:false (new Lwt_engine.select);
    Lwt_engine.set (new Lwt_engine.select);
    Lwt_engine.stop_event ev;
    let after = Lwt_engine.readable_count () in
    ignore (Unix.write_substring w "x" 0 1);
    Lwt_unix.sleep 0.05 >|= fun () ->
    Lwt_engine.set saved;
    Unix.close r;
    Unix.close w;
    after = before - 1 && !fired = 0
  end;
]

let suite = suite "lwt_engine" (tests @ transfer_tests)
