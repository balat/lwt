(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(** Communication between Lwt loops running on different domains.

    Lwt promises are domain-local: a promise belongs to the loop that created it,
    and touching one from another domain raises {!Lwt.Foreign_promise} rather than
    corrupting it quietly. That is what makes N independent loops safe, and it is
    also why they need something to talk through. This module is that something.

    {2 The shape of this API}

    Every operation here returns an ORDINARY LOCAL PROMISE, resolved by the
    calling loop. Nothing crosses a domain except plain data and the wake-up that
    carries it. What tells you a value is shared is its TYPE, not the operations
    on it: a {!Mutex.t} at module level is visible on re-reading, while the code
    that uses it looks like ordinary Lwt.

    {2 What this module is not}

    It is not a domain-safe mirror of Lwt's containers. {!Lwt_stream},
    {!Lwt_pool}, {!Lwt_mvar} and the rest stay domain-local, deliberately: a
    lazy clonable stream is not what one wants across a domain boundary, and a
    shared pool of Lwt resources is not merely hard but wrong, since a connection
    belongs to the loop that opened it. Give each loop its own pool and bound the
    total with a {!Semaphore}. *)

(** {2 Loops} *)

type loop
(** A handle on a running Lwt loop, which other domains can post work to.

    Obtained with {!self} on the loop itself and then shared: it is plain data,
    safe to pass to another domain. *)

val self : unit -> loop
(** [self ()] is a handle on the calling domain's loop. Creating it registers the
    inbox and the notification that drains it, so a domain that never calls this
    pays nothing. *)

exception Loop_terminated
(** Raised by {!run_on} when the target loop's domain has terminated. *)

val run_on : loop -> (unit -> unit) -> unit
(** [run_on loop f] posts [f] to [loop] and wakes it. [f] runs ON THAT LOOP's
    domain, on its next lap, with nothing held: it may do anything an ordinary
    Lwt callback may do, including resolving that loop's promises.

    Callable from any domain and from any thread, including a thread that is not
    running Lwt at all.

    This is the one unsafe primitive of this module, in the sense that it is the
    only one that hands code to another domain; everything else here is built on
    it. What it does not do is carry a result back, which is what
    {!Lwt_multicore.t} is for.

    @raise Loop_terminated if [loop]'s domain has already terminated. A domain
    that terminates BETWEEN the post and the execution is a race nothing can
    close from here: the work is then simply not done, and a caller who needs to
    know that should use the higher-level primitives, which reject their promise
    in that case. *)
