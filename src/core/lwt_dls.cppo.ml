(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

#if OCAML_VERSION >= (5, 0, 0)

type 'a t = 'a Domain.DLS.key

let new_key init = Domain.DLS.new_key init

(* [Domain.DLS.get] is not inlinable (it goes through the recursive
   [maybe_grow]), so at least spare the wrapper's own frame. *)
let[@inline] get k = Domain.DLS.get k
let[@inline] set k v = Domain.DLS.set k v
let is_main_domain () = Domain.is_main_domain ()
let at_domain_exit f = Domain.at_exit f

(* The domain's own identifier, and NOT a token in a slot of its own, because the
   measurement says so: reading a [Domain.DLS] slot costs 64 instructions, while
   [Domain.self ()] plus an integer comparison costs 9. On the buffered-character
   path of [Lwt_io] the slot version more than doubled the cost of a character.

   And it gives up nothing, which is worth stating because it is easy to assume
   otherwise: [Domain.self ()] is documented as "an identifier unique among all
   domains ever created by the program" (it is [Domain.self_index] that is reused
   after a domain terminates, and its own documentation points here for identity).
   Measured too: twenty sequential spawn-and-join give 1 to 20, never a repeat. So
   the comparison is exact, not approximate. *)
type token = int

let[@inline] self_token () = (Domain.self () :> int)

#else

(* One domain, so a slot is a cell. The initialiser runs eagerly: the core's
   only use is its scheduler record, whose initialiser is pure. *)
type 'a t = 'a ref

let new_key init = ref (init ())
let[@inline] get k = !k
let[@inline] set k v = k := v

(* One domain, and it is the main one. [at_domain_exit] is therefore never the
   right hook here, but it is defined rather than omitted so callers need no
   version test of their own; [Stdlib.at_exit] is its faithful equivalent. *)
let is_main_domain () = true
let at_domain_exit f = Stdlib.at_exit f

(* One domain, so one token, and every comparison against it succeeds. *)
type token = int

let[@inline] self_token () = 0

#endif

(* Shared by both branches: the affinity check for the domain-affine containers.
   One implementation and one message shape for all of them, since the only thing
   that varies is the name of the operation being refused. *)

let[@inline never] foreign name =
  invalid_arg
    (name
    ^ ": belongs to another domain. An Lwt value that holds waiters, "
    ^ "or that does I/O, lives on the domain that created it, the one whose "
    ^ "loop runs its callbacks")

let[@inline] check_owner name owner =
  if owner <> self_token () then foreign name
