(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(** A per-domain slot, and the only place where the core knows that domains
    exist.

    On OCaml 5 this is {!Domain.DLS}. On 4.14, where there is a single domain by
    construction, it degrades to a plain cell: same interface, no test, no
    indirection, so keeping the 4.14 floor costs nothing at run time. That is
    also what makes the layer free under js_of_ocaml.

    Deliberately minimal: the core holds ONE slot, containing its whole
    scheduler state, so a hot path pays at most one lookup. Do not add slots
    without measuring. *)

type 'a t

val new_key : (unit -> 'a) -> 'a t
(** [new_key init] creates a slot. On OCaml 5, [init ()] runs on first access
    from each domain; on 4.14 it runs once, at creation. Callers must therefore
    not depend on when it runs, only on the value it produces. *)

val get : 'a t -> 'a
val set : 'a t -> 'a -> unit
