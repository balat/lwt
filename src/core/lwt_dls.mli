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

type token = private int
(** The identity of a domain, as something to compare. It is the domain's
    identifier, which is cheap: reading a slot instead costs 64 instructions
    against 9, measured, and the domain-affine containers compare one per
    operation, down to one per buffered character.

    The price is that identifiers are RECYCLED when a domain terminates, so a
    container created by a domain that has since died can be taken for its own by
    a later domain that inherited the identifier. That is a missed violation,
    never a false one, and it needs the owner to be dead.

    This is what the domain-affine CONTAINERS are stamped with. Promises carry the
    core's own scheduler record instead, which is exact and costs nothing extra
    there, the record being in hand already. *)

val self_token : unit -> token
  [@@alert lwt_internal "Lwt_dls is internal to the Lwt packages, keep away."]
(** The calling domain's identity. *)

val check_owner : string -> token -> unit
  [@@alert lwt_internal "Lwt_dls is internal to the Lwt packages, keep away."]
(** [check_owner name owner] raises [Invalid_argument], mentioning [name], unless
    [owner] is the calling domain's token. One implementation for every
    domain-affine container, since all that varies is the name of the operation
    being refused.

    On the few paths where the CALL costs more than the check -- the buffered
    character of [Lwt_io], the descriptor of [Lwt_unix] -- write the comparison
    out with {!self_token} and {!foreign} instead. Measured: the call adds some
    thirty instructions to a nine-instruction check. *)

val foreign : string -> 'a
  [@@alert lwt_internal "Lwt_dls is internal to the Lwt packages, keep away."]
(** Raises the [Invalid_argument] {!check_owner} raises. For the hot paths that
    inline their own comparison. *)

val at_domain_exit : (unit -> unit) -> unit
  [@@alert lwt_internal "Lwt_dls is internal to the Lwt packages, keep away."]
(** [at_domain_exit f] runs [f] when the CURRENT domain exits. On the main domain
    that is at process exit, and it fires BEFORE every [Stdlib.at_exit] callback,
    which is why a caller wanting the historical interleaving must keep using
    [Stdlib.at_exit] for the main domain. On 4.14 it is [Stdlib.at_exit]. *)
