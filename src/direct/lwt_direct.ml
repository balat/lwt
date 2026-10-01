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

[@@@alert "+trespassing"]

(* part 2: effects, performing them *)

type _ Effect.t +=
  | Await : 'a Lwt.t -> 'a Effect.t
  | Yield : unit Effect.t

let await (fut : 'a Lwt.t) : 'a =
  match Lwt.state fut with
  | Lwt.Return x -> x
  | Lwt.Fail exn -> raise exn
  | Lwt.Sleep -> (
    match Effect.perform (Await fut) with
    | v -> v
    | exception Effect.Unhandled (Await _) ->
      (* No handler on this stack. Either no Lwt loop is running on this
         domain (a direct-style program awaiting at top level, where running
         the loop until [fut] settles is exactly what is meant), or a C frame
         stands between us and the loop's handler: an effect cannot be
         performed across a C call, and the libev engine invokes its watcher
         callbacks through [caml_callback]. In the latter case [Lwt_main.run]
         refuses to nest, and we turn its message into ours. *)
      (try Lwt_main.run fut with
       | Failure msg when String.length msg >= 6 && String.sub msg 0 6 = "Nested" ->
         failwith
           "Lwt_direct.await: no effect handler on the current stack while \
            Lwt_main.run is running on this domain; an await cannot be \
            performed from a callback invoked by C code (libev engine?)"))

let yield () : unit = Effect.perform Yield

(* A region where suspension is forbidden: the nearest handler wins, so an
   [await] on a pending promise (or a [yield]) anywhere below [f], at any depth,
   reaches this handler first and is turned into an exception raised at the
   point of the call. A [spawn] started inside the region only pushes a task; its
   body runs later, outside the region. *)
exception Suspension_forbidden

let no_await (f : unit -> 'a) : 'a =
  match f () with
  | v -> v
  | effect Await _, k -> Effect.Deep.discontinue k Suspension_forbidden
  | effect Yield, k -> Effect.Deep.discontinue k Suspension_forbidden

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
  let save_current () = Lwt_storage.get_current_storage ()
  let restore_current saved = Lwt_storage.set_current_storage saved
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
    (* [push_task] runs the thunk under the storage current at the push,
       i.e. this fiber's storage — no explicit capture needed. *)
    push_task (fun () -> Effect.Deep.continue k ())
  | effect Await fut, k ->
    (* The [on_any] callback fires at resolution time, under the RESOLVER's
       storage: capture this fiber's storage explicitly. *)
    let storage = Storage.save_current () in
    Lwt.on_any fut
      (fun res -> push_task (fun () ->
        Storage.restore_current storage; Effect.Deep.continue k res))
      (fun exn -> push_task (fun () ->
        Storage.restore_current storage; Effect.Deep.discontinue k exn))

(* part 4: putting it all together: running tasks *)

let run_inside_effect_handler_and_resolve_ (type a) (promise : a Lwt.u) f () : unit =
  with_effect_handler (fun () ->
    Storage.reset_to_empty();
    match f () with
    | res -> Lwt.wakeup promise res
    | exception exc -> Lwt.wakeup_exn promise exc)

let spawn f : _ Lwt.t =
  let lwt, resolve = Lwt.wait () in
  push_task (run_inside_effect_handler_and_resolve_ resolve f);
  lwt

(* part 4 (encore): running a task in the background *)

let run_inside_effect_handler_in_the_background_ f () : unit =
  with_effect_handler (fun () ->
    Storage.reset_to_empty();
    try
      f ()
    with exn ->
      !Lwt.async_exception_hook exn)

let spawn_in_the_background f : unit =
  push_task (run_inside_effect_handler_in_the_background_ f)

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
      push_task (fun () -> ignore (Effect.Deep.continue k ()));
      `Suspended
    | effect Await fut, k ->
      retire_if_drainer ();
      let storage = Storage.save_current () in
      Lwt.on_any fut
        (fun res -> push_task (fun () ->
          Storage.restore_current storage; ignore (Effect.Deep.continue k res)))
        (fun exn -> push_task (fun () ->
          Storage.restore_current storage; ignore (Effect.Deep.discontinue k exn)));
      `Suspended
  in
  match outcome with
  | `Done -> ()
  | `Suspended -> drive loop

let () = Lwt.Private.scheduler_set_runner drive
[@@@alert "+trespassing"]
