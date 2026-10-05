(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(** Values with one instance per loop.

    An [Lwt_mutex.t], an [Lwt_pool.t] or any other value holding Lwt state
    belongs to the loop that created it, and using it from another loop raises
    [Invalid_argument]. A value created when a module is initialised therefore
    belongs to the main domain's loop: a library that keeps a mutex or a pool at
    top level works with one loop and fails with several. This module is the way
    to keep such a value at top level and still have one per loop:

{[
  let lock = Lwt_per_loop.make Lwt_mutex.create

  let with_resource f = Lwt_mutex.with_lock (Lwt_per_loop.get lock) f
]}

    A loop is a domain running Lwt: each domain has at most one loop at a time,
    and a domain that runs [Lwt_main.run] again later finds the same instances.
    With a single domain, which is always the case on OCaml 4.14 and under
    js_of_ocaml, there is a single instance, as with a plain lazy value. *)

type 'a t
(** A value with one instance per loop. *)

val make : (unit -> 'a) -> 'a t
(** [make init] is a value whose instance in each loop is made by [init ()],
    on that loop's domain, the first time that domain calls {!get}. [init] may
    run at most once per domain; when exactly is not specified. *)

val get : 'a t -> 'a
(** [get t] is the calling loop's instance of [t]. *)
