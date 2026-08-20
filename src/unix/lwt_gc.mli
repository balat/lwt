(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(** Interaction with the garbage collector *)

(** This module offers a convenient way to add a finaliser launching a
    thread to a value, without having to use [Lwt_unix.run] in the
    finaliser. *)

(** {2 Which loop runs the deferred function}

    The function is run by the loop of the domain that REGISTERED it, on its next
    lap, since that is where the notification the finaliser sends is delivered.

    If that domain has gone by the time the value is collected, the function is
    not run at all: the finaliser is then executed by whichever domain adopted it,
    and the notification it sends names a channel that no longer exists, so it is
    dropped. Nothing is misrouted and nothing fails; the work is simply not
    done. *)

val finalise : ('a -> unit Lwt.t) -> 'a -> unit
  (** [finalise f x] ensures [f x] is evaluated after [x] has been
      garbage collected. If [f x] yields, then Lwt will wait for its
      termination at the end of the program.

      Note that [f x] is not called at garbage collection time, but
      later in the main loop. *)

val finalise_or_exit : ('a -> unit Lwt.t) -> 'a -> unit
  (** [finalise_or_exit f x] call [f x] when [x] is garbage collected
      or (exclusively) when the program exits. *)
