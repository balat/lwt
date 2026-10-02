(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The ppx's binds ([Lwt.backtrace_bind] and the functions built on
   [Lwt.backtrace_try_bind]) emit a [Begin] span when they have to wait and the
   matching [End] when their callback starts, carrying the context set by
   [Lwt.with_tracing_context]; a bind on a resolved promise emits nothing. The
   spans are read back from the runtime's event ring, so this checks what a
   consumer of lwt_runtime_events actually receives. *)

[@@@alert "-trespassing"]

let file = "test_trace.ml"
let add_loc e = e

let span (t : Lwt_runtime_events.Trace.t) =
  Printf.sprintf "%s %s %d"
    (match t.kind with Begin -> "Begin" | End -> "End")
    (Option.value t.context ~default:"-")
    t.line

let () =
  Runtime_events.start ();
  let cursor = Runtime_events.create_cursor None in
  let seen = ref [] in
  let callbacks =
    Runtime_events.Callbacks.create ()
    |> Runtime_events.Callbacks.add_user_event Lwt_runtime_events.Trace.t
         (fun _domain _ts _event (t : Lwt_runtime_events.Trace.t) ->
           if t.filename = file then seen := span t :: !seen)
  in
  let pending () = Lwt.wait () in
  (* A bind that waits, inside a context. *)
  let (_ : unit Lwt.t) =
    Lwt.with_tracing_context "ctx" (fun () ->
      let p, w = pending () in
      let r = Lwt.backtrace_bind file 10 add_loc p Lwt.return in
      Lwt.wakeup w ();
      r)
  in
  (* A bind on a resolved promise: no span. *)
  let (_ : unit Lwt.t) =
    Lwt.backtrace_bind file 20 add_loc Lwt.return_unit Lwt.return
  in
  (* A catch that waits, outside any context. *)
  let (_ : unit Lwt.t) =
    let p, w = pending () in
    let r = Lwt.backtrace_catch file 30 add_loc (fun () -> p) Lwt.reraise in
    Lwt.wakeup w ();
    r
  in
  (* The ppx's loop spans read the context through [Private.tracing_context]:
     it must be the key [with_tracing_context] sets. *)
  let shared =
    Lwt.with_tracing_context "loop" (fun () ->
      Lwt.return (Lwt.get Lwt.Private.tracing_context))
  in
  ignore (Runtime_events.read_poll cursor callbacks None : int);
  let got = List.rev !seen in
  let expected =
    [ "Begin ctx 10"; "End ctx 10"; "Begin - 30"; "End - 30" ]
  in
  if got <> expected then begin
    Printf.printf "spans: expected [%s], got [%s]\n"
      (String.concat "; " expected) (String.concat "; " got);
    exit 1
  end;
  if Lwt.state shared <> Lwt.Return (Some "loop") then begin
    print_endline "Private.tracing_context is not the key of with_tracing_context";
    exit 1
  end;
  print_endline "ppx spans through runtime events: ok"
