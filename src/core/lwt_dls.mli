(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(** The per-domain layer: a slot, and the little else the Lwt packages need to
    know that domains exist. Deliberately the ONLY place with that knowledge, so
    that the OCaml 4.14 floor and js_of_ocaml are handled once.

    Internal to the Lwt packages. The [lwt] library is [wrapped false], so this
    module is importable from anywhere and no wrapping hides it; the alert on its
    values is what says to keep away, the same device {!Lwt.Private} uses.

    On OCaml 5 this is {!Domain.DLS}. On 4.14, where there is a single domain by
    construction, it degrades to a plain cell: same interface, no test, no
    indirection, so keeping the 4.14 floor costs nothing at run time. That is
    also what makes the layer free under js_of_ocaml.

    Deliberately minimal: the core holds ONE slot, containing its whole
    scheduler state, so a hot path pays at most one lookup. Do not add slots
    without measuring. *)

type 'a t

val new_key : (unit -> 'a) -> 'a t
  [@@alert lwt_internal "Lwt_dls is internal to the Lwt packages, keep away."]
(** [new_key init] creates a slot. On OCaml 5, [init ()] runs on first access
    from each domain; on 4.14 it runs once, at creation. Callers must therefore
    not depend on when it runs, only on the value it produces. *)

val get : 'a t -> 'a
  [@@alert lwt_internal "Lwt_dls is internal to the Lwt packages, keep away."]

val set : 'a t -> 'a -> unit
  [@@alert lwt_internal "Lwt_dls is internal to the Lwt packages, keep away."]

val is_main_domain : unit -> bool
  [@@alert lwt_internal "Lwt_dls is internal to the Lwt packages, keep away."]
(** Always [true] on 4.14, where there is one domain and it is the main one. *)

val at_domain_exit : (unit -> unit) -> unit
  [@@alert lwt_internal "Lwt_dls is internal to the Lwt packages, keep away."]
(** [at_domain_exit f] runs [f] when the CURRENT domain exits. On the main domain
    that is at process exit, and it fires BEFORE every [Stdlib.at_exit] callback,
    which is why a caller wanting the historical interleaving must keep using
    [Stdlib.at_exit] for the main domain. On 4.14 it is [Stdlib.at_exit]. *)
