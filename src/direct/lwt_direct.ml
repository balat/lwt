(* Direct-style wrapper for Lwt code

   The implementation of the direct-style wrapper relies on ocaml5's effect
   system capturing continuations and adding them as a callback to some lwt
   promises. *)

(* part 1: tasks, getting the scheduler to call them.

   The effect-based core runs its own scheduler (a run queue drained by
   [Lwt_main.run]): direct-style continuations are pushed straight onto it
   through [Lwt.Private.scheduler_enqueue], with no intermediate task queue,
   no [Lwt_main] iteration hooks and no engine round-trip per batch — a yield
   costs one queue push/pop. The queued thunk runs under the fiber-local
   storage current at the push. Exceptions escaping a task go to
   [Lwt.async_exception_hook], as before. *)

[@@@alert "-trespassing"]

let[@inline] push_task f : unit =
  Lwt.Private.scheduler_enqueue (fun () ->
    try f ()
    with exn ->
      (* TODO 6.0: change async_exception handler to accept a backtrace, pass it
         here and at the other use site. *)
      (* TODO 6.0: this and other try-with: respect exception-filter *)
      !Lwt.async_exception_hook exn)

(* The resolution state of the scheduler (callback depth, storage, cascades
   in progress) is stack-shaped and travels with a suspended fiber: [suspend]
   right after capturing a continuation, [resume] around its resumption. *)
let suspend = Lwt.Private.scheduler_suspend
let resume = Lwt.Private.scheduler_resume

(* Resumptions go through the queue unprotected: an exception escaping a
   resumed callback propagates out of the pass, hence out of [Lwt_main.run],
   as an exception escaping any callback does in Lwt. A spawned task catches
   its own, in its wrapper, before it can get here. *)
let push_resume f : unit = Lwt.Private.scheduler_enqueue f

(* A yield parks its resumption for the next lap of the loop, after one engine
   iteration, instead of re-queuing it at once: a task yielding in a loop must
   not keep the engine, the timers and the pauses from running. *)
let push_next_lap f : unit = Lwt.Private.scheduler_enqueue_next_lap f

(* The body of a task runs as a callback runs, at depth 1 of the resolution
   loop, so that a [wakeup_later] it performs never runs the awakened
   continuation on its stack: deferred to the end of the body, or handed to
   the run queue at its next suspension. *)
let in_resolution_loop = Lwt.Private.in_resolution_loop

[@@@alert "+trespassing"]

(* part 2: effects, performing them *)

type _ Effect.t +=
  | Await : 'a Lwt.t -> 'a Effect.t
  | Yield : unit Effect.t

(* A perform with no handler on the stack raises [Effect.Unhandled] at the
   point of the perform. Two situations lead there, and the message names
   both: no event loop is running on this domain, or a C frame stands between
   the caller and the loop's handler, since an effect cannot be performed
   across a C call (the libev engine invokes through [caml_callback] the
   callbacks handed directly to [Lwt_engine]). *)
let no_handler fname =
  failwith
    (fname
    ^ ": no scheduler handler on the current stack. Either no event loop is \
       running on this domain (start one with Lwt_direct.main or \
       Lwt_main.run), or this callback was invoked from C code, across which \
       an effect cannot be performed.")

(* A region where suspension is forbidden ([no_await]) is a counter kept by the
   scheduler, [Lwt.Private.no_suspend], so that the core's other clients
   ([Lwt_react]'s setters) can open one without depending on this library or
   on effects. [await] and [yield] consult it right before performing, on the
   pending path only, and raise at the point of the call. *)
exception Suspension_forbidden

[@@@alert "-trespassing"]

let check_suspension_allowed () =
  if Lwt.Private.suspension_forbidden () then raise Suspension_forbidden

(* Before suspending on a promise of another domain: the owner check that
   attaching the continuation would make, made here so that Foreign_promise
   is raised at the call site rather than out of the handler, where it would
   escape Lwt_main.run with the continuation lost. *)
let check_owner = Lwt.Private.check_owner

let no_await (f : unit -> 'a) : 'a = Lwt.Private.no_suspend f

[@@@alert "+trespassing"]

let await (fut : 'a Lwt.t) : 'a =
  match Lwt.state fut with
  | Lwt.Return x -> x
  | Lwt.Fail exn -> raise exn
  | Lwt.Sleep -> (
    check_suspension_allowed ();
    check_owner fut;
    match Effect.perform (Await fut) with
    | v -> v
    | exception Effect.Unhandled (Await _) -> no_handler "Lwt_direct.await")

let yield () : unit =
  check_suspension_allowed ();
  match Effect.perform Yield with
  | () -> ()
  | exception Effect.Unhandled Yield -> no_handler "Lwt_direct.yield"

(* interlude: task-local storage helpers *)

module Storage = struct
  [@@@alert "-trespassing"]
  module Lwt_storage = Lwt.Private.Sequence_associated_storage
  [@@@alert "+trespassing"]
  type 'a key = 'a Lwt.key
  let new_key = Lwt.new_key
  let get = Lwt.get
  let set k v =
    let open Lwt_storage in
    set_current_storage (modify_storage k (Some v) (get_current_storage ()))
  let remove k =
    let open Lwt_storage in
    set_current_storage (modify_storage k None (get_current_storage ()))
  let reset_to_empty () =
    let open Lwt_storage in
    set_current_storage empty_storage
end

(* part 3: handling effects *)

(* Run [f ()] under the effect handler, using the OCaml 5.3+
   [match … with effect] syntax. [Yield] re-schedules the continuation as a
   task; [Await] resumes it (or discontinues it) once the awaited Lwt promise
   settles. *)
let with_effect_handler (f : unit -> unit) : unit =
  match f () with
  | () -> ()
  | effect Yield, k ->
    let st = suspend () in
    push_next_lap (fun () -> resume st (fun () -> Effect.Deep.continue k ()))
  | effect Await fut, k ->
    let st = suspend () in
    Lwt.on_any fut
      (fun res -> push_resume (fun () ->
        resume st (fun () -> Effect.Deep.continue k res)))
      (fun exn -> push_resume (fun () ->
        resume st (fun () -> Effect.Deep.discontinue k exn)))

(* part 4: putting it all together: running tasks *)

let run_inside_effect_handler_and_resolve_ (type a) (promise : a Lwt.u) f () : unit =
  with_effect_handler (fun () ->
    in_resolution_loop (fun () ->
      Storage.reset_to_empty();
      match f () with
      | res -> Lwt.wakeup promise res
      | exception exc -> Lwt.wakeup_exn promise exc))

let spawn f : _ Lwt.t =
  let lwt, resolve = Lwt.wait () in
  push_task (run_inside_effect_handler_and_resolve_ resolve f);
  lwt

(* part 4 (encore): running a task in the background *)

let run_inside_effect_handler_in_the_background_ f () : unit =
  with_effect_handler (fun () ->
    in_resolution_loop (fun () ->
      Storage.reset_to_empty();
      try
        f ()
      with exn ->
        !Lwt.async_exception_hook exn))

let spawn_in_the_background f : unit =
  push_task (run_inside_effect_handler_in_the_background_ f)

(* part 4 (coda): the entry point of a direct-style program *)

let main f = Lwt_main.run (spawn f)

[@@@alert "-trespassing"]
(* part 5: await anywhere, by running the scheduler loop under the handler *)

(* The scheduler loop itself runs under the effect handler, so an [await] (or a
   [yield]) performed by ANY code the loop runs (a bind continuation, an
   [on_success] callback, a completion handler of the engine) is handled, not
   only inside [spawn]. The continuation captured then holds the rest of the
   loop pass too, which is why that pass must be retired: [drive] tells the core
   the fiber is no longer the drainer ([scheduler_retire_drainer]) and starts a
   new pass on a fresh fiber. When the suspended fiber is resumed (as a task of
   the new drainer) it finishes its callback, and the loop pass inside it, seeing
   it was retired, returns, back into the resumer's task. So exactly one pass
   drains the queue at any time, and a resumed one sits on top of it only for
   the duration of its own callback. The recursion in [drive] is outside the
   handler and in tail position: the stack does not grow with the number of
   suspensions.

   [spawn] keeps its own, nearer handler: an [await] inside a spawned task
   suspends that task alone, as before, and never reaches this one.

   What the continuation freezes along with the callback is whatever else was
   on the stack between the loop and the [await]: the remaining waiters of the
   same resolution cascade, and the resolver's own code after its [wakeup]. See
   the .mli for the rule of thumb this gives. *)
let rec drive (loop : unit -> unit) : unit =
  let gen = Lwt.Private.scheduler_drainer_gen () in
  let retire_if_drainer () =
    if Lwt.Private.scheduler_drainer_gen () = gen then
      Lwt.Private.scheduler_retire_drainer ()
  in
  let outcome =
    match loop () with
    | () -> `Done
    | effect Yield, k ->
      retire_if_drainer ();
      let st = suspend () in
      push_next_lap (fun () ->
        resume st (fun () -> ignore (Effect.Deep.continue k ())));
      `Suspended
    | effect Await fut, k ->
      retire_if_drainer ();
      let st = suspend () in
      Lwt.on_any fut
        (fun res -> push_resume (fun () ->
          resume st (fun () -> ignore (Effect.Deep.continue k res))))
        (fun exn -> push_resume (fun () ->
          resume st (fun () -> ignore (Effect.Deep.discontinue k exn))));
      `Suspended
  in
  match outcome with
  | `Done -> ()
  | `Suspended -> drive loop

let () = Lwt.Private.scheduler_set_runner drive
[@@@alert "+trespassing"]
