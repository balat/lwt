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

type token = unit ref

let token_key : token t = new_key (fun () -> ref ())
let[@inline] self_token () = get token_key

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
type token = unit ref

let the_token : token = ref ()
let[@inline] self_token () = the_token

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
  if owner != self_token () then foreign name
