(** Direct style control flow for Lwt.

    Using this module you can write code in direct style (using loops,
    exceptions handlers, etc.) in an Lwt codebase. Direct-style sections are
    typically enclosed in a call to {!spawn}, and they may {!await} on
    promises; {!await} also works in any callback the event loop runs, see its
    documentation, and a direct-style program starts with {!main}. For
    example:

    {[
    open Lwt_direct
    spawn (fun () ->
      let continue = ref true in
      while !continue do
        match await @@ Lwt_io.read_line in_channel with
        | exception End_of_file -> continue := false
        | line ->
          let uppercase_line = String.uppercase_ascii line in
          await @@ Lwt_io.write_line out_channel uppercase_line
      done)
    ]}

    In this code snippet, the [while]-loop repeats a simple task of reading from
    an {!Lwt_io.channel}, modifying it, and writing it to a different channel.
    The code is in direct-style: the control structures are standard OCaml
    without any Lwt primitives.

    The code-snippet as a whole is a [unit Lwt.t] promise. It becomes resovled
    when the function returns. Conversely, the promises inside the snippet are
    wrapped in {!await}, turning them into regular plain (non-Lwt) values
    (although values that are not available immediately).

    The [Lwt_direct] module is implemented using OCaml 5's
    {{:https://ocaml.org/manual/5.3/effects.html} effects and effect handlers}.
    This allows the kind of scheduling where a promise is turned into a regular
    value and vice-versa. *)

val spawn : (unit -> 'a) -> 'a Lwt.t
(** [spawn f] runs the function [f ()], it also returns a promise [p] which is
    resolved when the call to [f ()] returns a value. If [f ()] throws an
    exception, the promise [p] is rejected.

    The function [f] can create Lwt promises (e.g., by calling functions from
    [Lwt_io], [Lwt_unix], or third-party libraries) and use {!await} to wait for
    them. These promises are evaluated in the Lwt event loop.

    Like any promise in Lwt, [f ()] can starve the event loop if it runs long
    computations without yielding to the event loop.

    Cancelling the promise returned by [spawn] has no effect: the execution of
    [f ()] continues and the promise is not cancelled.

    The task runs as a callback does, at depth 1 of Lwt's resolution loop: a
    {!Lwt.wakeup_later} it performs defers the callbacks of the promise to the
    end of the task, or to its next suspension, so they never run on the
    task's stack. {!Lwt.wakeup} runs them at once, as documented, and if one
    of them awaits, the task waits with it.

    When [f ()] terminates (successfully or not), the promise
    [spawn f] is resolved with [f ()]'s result, or the exception
    raised by [f ()]. *)

val spawn_in_the_background :
  (unit -> unit) ->
  unit
(** [spawn_in_the_background f] is similar to [ignore (spawn f)].
    The computation [f ()] runs in the background in the event loop
    and returns no result.

    If [f()] raises an exception, {!Lwt.async_exception_hook} is called. *)

val yield : unit -> unit
(** Yield to the event loop: suspends the current task until the next lap of
    the loop, after the engine has run once, so that I/O, timers, pauses and
    the other tasks get their turn. It is {!await}[ (Lwt.pause ())] with less
    machinery and fewer characters.

    A task that yields in a loop therefore costs one engine iteration per
    yield when it is alone, as a loop of [Lwt.pause] does; that is the point:
    nothing it shares the loop with is starved. Outside of {!spawn} and
    {!spawn_in_the_background}, [yield] suspends the current task, as
    {!await} does; see there for what the current task is. *)

val await : 'a Lwt.t -> 'a
(** [await p] returns the result of [p] (or raises the exception with which [p]
    was rejected.

    If [p] is not resolved yet, [await p] will suspend the current task (i.e.,
    the computation started by the surrounding {!spawn}) and resume it when [p]
    is resolved.

    [await] also works outside of {!spawn} and {!spawn_in_the_background},
    under {!Lwt_main.run}: in a {!Lwt.bind} continuation, an {!Lwt.on_success}
    callback, an {!Lwt_switch} hook, an exit hook, any callback the event loop
    runs. What it suspends is the {e current task}: the callback, together
    with whatever called it synchronously and is still on the stack. Every
    pause, I/O completion and timer has its callbacks run as a task of their
    own, and the other callbacks attached to the same promise run while one
    of them waits, so an awaiting callback delays nothing but the code that
    ran it synchronously: the code that resolved the promise with
    {!Lwt.wakeup} (not {!Lwt.wakeup_later}, which inside the loop always
    defers them), and the other actions of an {!Lwt_timeout} due in the same
    second. When that matters, start a task with {!spawn} inside the
    callback.

    Where suspension is refused, [await] on a pending promise raises
    {!Suspension_forbidden} at the call: inside {!no_await}, inside a
    propagation started by a setter of [Lwt_react], and inside the event
    loop's own lap, that is the iteration hooks of {!Lwt_main} and the
    callbacks the engine invokes directly (a callback handed to
    {!Lwt_engine}, an [Lwt_unix.on_signal] handler, an
    [Lwt_unix.make_notification] callback, the synchronous part of
    [Lwt_preemptive.run_in_main], an [Lwt_gc.finalise] function), on every
    engine alike. With no event loop running on the current domain (at top
    level, or in a system thread such as the body of
    [Lwt_preemptive.detach]), it raises [Failure] naming itself. A promise
    already resolved or rejected returns its value, or raises, anywhere.

    A task suspended on a promise that is never resolved keeps its stack for
    the life of the program, some hundreds of bytes at least: the OCaml
    runtime does not reclaim the stack of a dropped continuation. Cancelling
    the promise releases it, since the task is then resumed with
    {!Lwt.Canceled}. *)

val main : (unit -> 'a) -> 'a
(** [main f] runs the event loop until [f ()], started as a task with
    {!spawn}, returns, and returns its result, or raises the exception it
    raised. It is {!Lwt_main.run}[ (spawn f)]: the entry point of a program
    written in direct style, as [Lwt_main.run] is the entry point of a program
    written with promises. *)

exception Suspension_forbidden
(** Raised by {!await} and {!yield} inside a {!no_await} region, at the point
    of the call, when the awaited promise is not resolved yet. *)

val no_await : (unit -> 'a) -> 'a
(** [no_await f] runs [f ()] and forbids suspension during it: an {!await} on a
    promise that is not resolved yet, or a {!yield}, performed by [f] or by
    anything [f] calls, at any depth, raises {!Suspension_forbidden} at the
    point of the call instead of suspending the task. An {!await} on a promise
    that is already resolved returns its value as usual.

    This is for code that must run without interleaving and cannot know what
    its callbacks do: a reactive update cycle, the critical section of a data
    structure. Once {!await} is in use, the types no longer say whether a call
    may suspend; [no_await] says it dynamically, where it matters, and turns a
    silent interleaving into an exception.

    Tasks started inside the region with {!spawn} or
    {!spawn_in_the_background} run later, as tasks of their own, and are not
    affected. The senders and setters returned by [Lwt_react.E.create] and
    [Lwt_react.S.create] open such a region around the propagation they
    start. *)

(** Local storage.

    This storage is the same as the one described with {!Lwt.key},
    except that it is usable from the inside of {!spawn} or
    {!spawn_in_the_background}.

    Each task has its own storage, independent from other tasks or promises.

    NOTE: it is recommended to use [Lwt_direct.Storage] functions rather than
    [Lwt.key] functions from {!Lwt}. The latter is deprecated. *)
module Storage : sig
  type 'a key = 'a Lwt.key
  val new_key : unit -> 'a key
  (** Alias to {!Lwt.new_key} *)

  val get : 'a key -> 'a option
  (** get the value associated with this key in local storage, or [None] *)

  val set : 'a key -> 'a -> unit
  (** [set k v] sets the key to the value for the rest of the task. *)

  val remove : 'a key -> unit
  (** Remove the value associated with this key, if any *)
end
