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
  | Lwt.Sleep -> Effect.perform (Await fut)

let yield () : unit = Effect.perform Yield

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
    current_storage := (modify_storage k (Some v) !current_storage)
  let remove k =
    let open Lwt_storage in
    current_storage := (modify_storage k None !current_storage)
  let reset_to_empty () =
    let open Lwt_storage in
    current_storage := empty_storage
  let save_current () = !Lwt_storage.current_storage
  let restore_current saved = Lwt_storage.current_storage := saved
end

(* part 3: handling effects *)

let handler : _ Effect.Deep.effect_handler =
  let effc : type b. b Effect.t -> ((b, unit) Effect.Deep.continuation -> 'a) option =
    function
    | Yield ->
      (* [push_task] runs the thunk under the storage current at the push,
         i.e. this fiber's storage — no explicit capture needed. *)
      Some (fun k -> push_task (fun () -> Effect.Deep.continue k ()))
    | Await fut ->
      Some
        (fun k ->
          (* The [on_any] callback fires at resolution time, under the
             RESOLVER's storage: capture this fiber's storage explicitly. *)
          let storage = Storage.save_current () in
          Lwt.on_any fut
            (fun res -> push_task (fun () ->
              Storage.restore_current storage; Effect.Deep.continue k res))
            (fun exn -> push_task (fun () ->
              Storage.restore_current storage; Effect.Deep.discontinue k exn)))
    | _ -> None
  in
  { effc }

(* part 4: putting it all together: running tasks *)

let run_inside_effect_handler_and_resolve_ (type a) (promise : a Lwt.u) f () : unit =
  let run_f_and_set_res () =
    Storage.reset_to_empty();
    match f () with
    | res -> Lwt.wakeup promise res
    | exception exc -> Lwt.wakeup_exn promise exc
  in
  Effect.Deep.try_with run_f_and_set_res () handler

let spawn f : _ Lwt.t =
  let lwt, resolve = Lwt.wait () in
  push_task (run_inside_effect_handler_and_resolve_ resolve f);
  lwt

(* part 4 (encore): running a task in the background *)

let run_inside_effect_handler_in_the_background_ f () : unit =
  let run_f () : unit =
    Storage.reset_to_empty();
    try
      f ()
    with exn ->
      !Lwt.async_exception_hook exn
  in
  Effect.Deep.try_with run_f () handler

let spawn_in_the_background f : unit =
  push_task (run_inside_effect_handler_in_the_background_ f)
