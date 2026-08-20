(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* [Lwt_sequence] is deprecated – we don't want users outside Lwt using it.
   However, it is still used internally by Lwt. So, briefly disable warning 3
   ("deprecated"), and create a local, non-deprecated alias for
   [Lwt_sequence] that can be referred to by the rest of the code in this
   module without triggering any more warnings. *)
module Lwt_sequence = Lwt_sequence

(* This module is the legitimate user of the core's scheduler hooks
   ([Lwt.Private]): it installs the engine-blocking idle hook and drives the
   run queue. *)
[@@@alert "-trespassing"]

open Lwt.Infix

(* PER DOMAIN. Iteration hooks belong to a loop, not to a process: an enter hook
   is called around one engine iteration, and with N loops there are N engines.
   Exit hooks belong to a loop too, since running one means running an Lwt loop,
   which must happen on the domain that owns the promises involved.

   One slot holding the three, per the rule of the S2 log: a module takes one
   slot, not one per variable.

   The slot's initialiser has a deliberate side effect: on a domain that is NOT
   the main one it registers, through [Lwt_dls.at_domain_exit], the runner that
   drains that domain's exit hooks when the domain terminates. Doing it here
   rather than in [run] means a domain that registers a hook and never calls
   [run] still gets it drained. The main domain keeps the historical
   [Stdlib.at_exit] registered below, and this is not a detail: [Domain.at_exit]
   fires BEFORE every [Stdlib.at_exit] callback, so moving the main domain over
   would run Lwt's hooks, [Lwt_io]'s [flush_all] among them, before a user's own
   [Stdlib.at_exit] handler rather than after it, silently dropping whatever that
   handler wrote to an Lwt channel. *)
type hooks = {
  enter_iter : (unit -> unit) Lwt_sequence.t;
  leave_iter : (unit -> unit) Lwt_sequence.t;
  exits : (unit -> unit Lwt.t) Lwt_sequence.t;

  (* PER DOMAIN, and this is what lets N loops run at once. The flag detects a
     NESTED [run], which is a per-domain notion: a second domain entering [run]
     is not nesting, it is the whole point. It was process-wide, so it made the
     second domain fail with "nested calls are not allowed"; and a domain
     draining its exit hooks cleared the flag of a domain still inside [run].

     Its mutex stays, and is per domain too: it guards against two SYSTEM THREADS
     of this domain racing between the read and the write, which is what the
     process-wide mutex did. It is taken once per [run], so its cost is nothing.
     Both live in the hooks record rather than in slots of their own, per the
     one-slot rule. *)
  mutable running : [ `No | `From_somewhere | `From of string ];
  running_mutex : Mutex.t;
}

let drain_exit_hooks = ref (fun () -> ())

[@@@alert "-lwt_internal"]

let hooks : hooks Lwt_dls.t =
  Lwt_dls.new_key (fun () ->
    if not (Lwt_dls.is_main_domain ()) then
      Lwt_dls.at_domain_exit (fun () -> !drain_exit_hooks ());
    {
      enter_iter = Lwt_sequence.create ();
      leave_iter = Lwt_sequence.create ();
      exits = Lwt_sequence.create ();
      running = `No;
      running_mutex = Mutex.create ();
    })

let enter_iter_hooks () = (Lwt_dls.get hooks).enter_iter
let leave_iter_hooks () = (Lwt_dls.get hooks).leave_iter
let exit_hooks () = (Lwt_dls.get hooks).exits

let yield = Lwt.pause

let abandon_yielded_and_paused () =
  Lwt.abandon_paused ()

(* The effect-based core runs its own scheduler (the run queue, serving fibers
   and paused promises); [run] drives it through the [Lwt.Private] hooks and
   installs the engine-blocking idle hook. The hook is called when the run
   queue is empty and no pause is pending: it performs one historical Lwt_main
   loop lap (enter hooks, one engine iteration, fulfil paused promises, leave
   hooks) and reports whether to keep going — [false] exactly when [p] is
   resolved, which makes the scheduler return [p]'s outcome. *)
let run (type a) (p : a Lwt.t) : a =
  let idle () =
    Lwt_rte.emit_sch_lap ();
    Lwt_unix.write_job_count_runtimte_event ();
    Lwt_rte.emit_paused_count (Lwt.paused_count ());
    if not (Lwt.is_sleeping p) then false
    else begin
      (* Call enter hooks. *)
      Lwt_sequence.iter_l (fun f -> f ()) (enter_iter_hooks ());

      (* Do the main loop call. Block only if nothing became ready meanwhile:
         the enter hooks may have resolved promises (e.g. Lwt_direct pumps its
         task queue from them) — possibly [p] itself — and the core scheduler
         must run that work now, not after an unbounded engine wait. *)
      let should_block_waiting_for_io =
        Lwt.is_sleeping p
        && Lwt.paused_count () = 0
        && Lwt.Private.scheduler_queue_is_empty ()
      in
      Lwt_engine.iter should_block_waiting_for_io;

      (* Fulfill paused promises. *)
      Lwt.wakeup_paused ();

      (* Call leave hooks. *)
      Lwt_sequence.iter_l (fun f -> f ()) (leave_iter_hooks ());

      true
    end
  in

  Lwt_rte.emit_sch_call_begin ();
  Fun.protect
    ~finally:(fun () -> Lwt_rte.emit_sch_call_end ())
    (fun () ->
      Lwt.Private.scheduler_set_idle idle;
      Lwt.Private.scheduler_run (fun () -> p))

let finished () =
  let h = Lwt_dls.get hooks in
  Mutex.lock h.running_mutex;
  h.running <- `No;
  Mutex.unlock h.running_mutex

let run p =
  (* Fail in case a call to Lwt_main.run is nested under another invocation of
     Lwt_main.run ON THIS DOMAIN. Another domain running its own loop is not a
     nested call. *)
  let h = Lwt_dls.get hooks in
  Mutex.lock h.running_mutex;

  let error_message_if_call_is_nested =
    match h.running with
    (* `From is effectively disabled for the time being, because there is a bug,
       present in all versions of OCaml supported by Lwt, where, with the
       bytecode runtime, if one changes the working directory and then attempts
       to retrieve the backtrace, the runtime calls [abort] at the C level and
       exits the program ungracefully. It is especially likely that a daemon
       would change directory before calling [Lwt_main.run], so we can't have it
       retrieving the backtrace, even though a daemon is not likely to be
       compiled to bytecode.

       This can be addressed with detection. Starting with 4.04, there is a
       type [Sys.backend_type] that could be used. *)
    | `From backtrace_string ->
      Some (Printf.sprintf "%s\n%s\n%s"
        "Nested calls to Lwt_main.run are not allowed"
        "Lwt_main.run already called from:"
        backtrace_string)
    | `From_somewhere ->
      Some ("Nested calls to Lwt_main.run are not allowed")
    | `No ->
      let called_from =
        (* See comment above.
        if Printexc.backtrace_status () then
          let backtrace =
            try raise Exit
            with Exit -> Printexc.get_backtrace ()
          in
          `From backtrace
        else *)
          `From_somewhere
      in
      h.running <- called_from;
      None
  in

  Mutex.unlock h.running_mutex;

  begin match error_message_if_call_is_nested with
  | Some message -> failwith message
  | None -> ()
  end;

  (* Inside the [match], not before it: if it raises, [finished ()] must still
     run, or the flag stays set and every later [Lwt_main.run] in the process
     reports a nested call. *)
  match Lwt_unix.install_sigchld_handler (); run p with
  | result ->
    finished ();
    result
  | exception exn when Lwt.Exception_filter.run exn ->
    finished ();
    raise exn

let rec call_hooks () =
  match Lwt_sequence.take_opt_l (exit_hooks ()) with
  | None ->
    Lwt.return_unit
  | Some f ->
    Lwt.catch
      (fun () -> f ())
      (fun _  -> Lwt.return_unit) >>= fun () ->
    call_hooks ()

(* Drains THIS domain's exit hooks. Used by the main domain's [Stdlib.at_exit]
   below and, for every other domain, by the [Lwt_dls.at_domain_exit] the hooks
   slot registers. The [finished ()] it calls now clears only this domain's flag,
   so a domain leaving while another is inside [run] no longer disturbs it. *)
let drain () =
  if not (Lwt_sequence.is_empty (exit_hooks ())) then begin
    Lwt.abandon_wakeups ();
    finished ();
    run (call_hooks ())
  end

let () = drain_exit_hooks := drain
let () = at_exit drain
let at_exit f = ignore (Lwt_sequence.add_l f (exit_hooks ()))

module type Hooks =
sig
  type 'return_value kind
  type hook

  val add_first : (unit -> unit kind) -> hook
  val add_last : (unit -> unit kind) -> hook
  val remove : hook -> unit
  val remove_all : unit -> unit
end

module type Hook_sequence =
sig
  type 'return_value kind

  (* A FUNCTION, not a value: the sequence is per domain, so it must be looked up
     when the hook is added rather than captured when the functor is applied. *)
  val sequence : unit -> (unit -> unit kind) Lwt_sequence.t
end

module Wrap_hooks (Sequence : Hook_sequence) =
struct
  type 'a kind = 'a Sequence.kind
  type hook = (unit -> unit Sequence.kind) Lwt_sequence.node

  let add_first hook_fn =
    let hook_node = Lwt_sequence.add_l hook_fn (Sequence.sequence ()) in
    hook_node

  let add_last hook_fn =
    let hook_node = Lwt_sequence.add_r hook_fn (Sequence.sequence ()) in
    hook_node

  let remove hook_node =
    Lwt_sequence.remove hook_node

  let remove_all () =
    Lwt_sequence.iter_node_l Lwt_sequence.remove (Sequence.sequence ())
end

module Enter_iter_hooks =
  Wrap_hooks (struct
    type 'return_value kind = 'return_value
    let sequence = enter_iter_hooks
  end)

module Leave_iter_hooks =
  Wrap_hooks (struct
    type 'return_value kind = 'return_value
    let sequence = leave_iter_hooks
  end)

module Exit_hooks =
  Wrap_hooks (struct
    type 'return_value kind = 'return_value Lwt.t
    let sequence = exit_hooks
  end)
