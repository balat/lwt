(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

#if OCAML_VERSION >= (5, 0, 0)

(* SPIKE, REJECTED, KEPT FOR THE RECORD. One array per key, indexed by the
   domain's slot index, so the fast path is a load, a C call and a compare rather
   than [Domain.DLS.get]'s 57 instructions. Measured at 26.

   DELIBERATELY INCOMPLETE, which is half of why it was rejected: a domain's slot
   index is REUSED once it terminates, and this prototype does not clear the
   entry, so a fresh domain inheriting an index would inherit a dead domain's
   scheduler. Making it correct needs an at_exit clear per key and domain, whose
   ordering against Lwt_main's own exit drain then has to be reasoned about, plus
   a global registry of keys. About 120 lines of delicate machinery for the eight
   instructions between this and the 9-instruction floor.

   Growth is under a mutex and published through an Atomic; an entry is only ever
   written by its own domain. *)
type 'a t = { slots : Obj.t array Atomic.t; init : unit -> 'a }

let none : Obj.t = Obj.repr (ref 0)

let new_key init = { slots = Atomic.make (Array.make 8 none); init }

let grow_mutex = Mutex.create ()

let ensure k i =
  let st = Atomic.get k.slots in
  if i < Array.length st then st
  else begin
    Mutex.lock grow_mutex;
    let st = Atomic.get k.slots in
    let st =
      if i < Array.length st then st
      else begin
        let n = ref (Array.length st) in
        while i >= !n do n := 2 * !n done;
        let st' = Array.make !n none in
        Array.blit st 0 st' 0 (Array.length st);
        Atomic.set k.slots st';
        st'
      end
    in
    Mutex.unlock grow_mutex;
    st
  end

let[@inline never] slow k i =
  let st = ensure k i in
  let v = Obj.repr (k.init ()) in
  Array.unsafe_set st i v;
  (Obj.obj v : 'a)

let[@inline] get (k : 'a t) : 'a =
  let st = Atomic.get k.slots in
  let i = Domain.self_index () in
  if i < Array.length st then begin
    let v = Array.unsafe_get st i in
    if v != none then (Obj.obj v : 'a) else slow k i
  end
  else slow k i

let[@inline] set k v =
  let i = Domain.self_index () in
  let st = ensure k i in
  Array.unsafe_set st i (Obj.repr v)
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
