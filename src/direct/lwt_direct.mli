(** Direct style control flow for Lwt.

    Using this module you can write code in direct style (using loops,
    exceptions handlers, etc.) in an Lwt codebase. Direct-style sections are
    typically enclosed in a call to {!spawn}, and they may {!await} on
    promises; {!await} also works in any callback the event loop runs, and at
    top level, see its documentation. For example:

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
(** Yield to the event loop.

    This is similar to [await (Lwt.pause ())], using less indirection internally
    and fewer characters to write.

    Outside of {!spawn} and {!spawn_in_the_background}, [yield] suspends the
    current task, as {!await} does; see there for what the current task is. *)

val await : 'a Lwt.t -> 'a
(** [await p] returns the result of [p] (or raises the exception with which [p]
    was rejected.

    If [p] is not resolved yet, [await p] will suspend the current task (i.e.,
    the computation started by the surrounding {!spawn}) and resume it when [p]
    is resolved.

    [await] also works outside of {!spawn} and {!spawn_in_the_background},
    under {!Lwt_main.run}: in a {!Lwt.bind} continuation, in an
    {!Lwt.on_success} callback, in any callback the event loop runs. What it
    suspends is then the {e current task}: the callback, together with whatever
    called it synchronously and is still on the stack. The event loop runs the
    callbacks of each pause, I/O completion and timer as a task of their own,
    so an awaiting callback never delays the callbacks of other events. A
    callback triggered synchronously by {!Lwt.wakeup}, or by {!Lwt.wakeup_later}
    called from outside any callback, shares its task with the code that
    resolved the promise: that code, and the other callbacks attached to the
    same promise, resume only once the await is over. ({!Lwt.wakeup_later}
    called from inside a callback defers the callbacks instead, so they run as
    their own task.) When that matters, start a task with {!spawn} inside the
    callback.

    With no event loop running on the current domain, [await p] runs
    {!Lwt_main.run}[ p]: a direct-style program needs no [Lwt_main.run] of its
    own. An await from a callback that the engine invokes from C (as the libev
    engine does) raises [Failure], because an effect cannot cross a C frame; the
    callbacks of promises are never in that position, only a callback handed
    directly to {!Lwt_engine} is. *)

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
    affected. *)

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
