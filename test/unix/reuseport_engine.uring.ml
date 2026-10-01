(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* lwt_uring is available: install the io_uring engine on the calling domain's
   loop. *)

let install () = Lwt_uring.set ()
