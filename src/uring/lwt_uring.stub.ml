(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* The implementation built where the uring library is missing: on every system
   but Linux, since that library only installs there, or on a Linux switch
   without it. It has the interface of the real engine, so that a program links
   the same way everywhere, and it reports io_uring as unavailable. *)

let not_available () = raise (Lwt_sys.Not_available "io_uring")

let available () = false

let set ?queue_depth:_ ?deferred:_ () = not_available ()

let set_if_available ?queue_depth:_ ?deferred:_ () = false

type Lwt_engine.engine_id += Engine_id__uring

(* The class exists so that the interface is the same as on Linux. Building an
   instance raises before the object exists; it inherits from [Lwt_engine.select]
   only because a class needs an implementation. *)
class uring ?queue_depth:_ ?deferred:_ () =
  let () = not_available () in
  object
    inherit Lwt_engine.select
  end

module Io = struct
  type bigarray =
    (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

  let read _ _ _ _ = not_available ()
  let write _ _ _ _ = not_available ()
  let read_bigarray _ _ _ _ = not_available ()
  let write_bigarray _ _ _ _ = not_available ()
end
