(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The public face of the per-domain slot: a key per value, which is a slot of
   the user's, never on one of Lwt's own hot paths. *)

[@@@alert "-lwt_internal"]

type 'a t = 'a Lwt_dls.t

let make init = Lwt_dls.new_key init

let get = Lwt_dls.get
