(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The promises a model test keeps, numbered in the order the commands made
   them, so that a later command can cancel or observe one. A command names a
   promise by an arbitrary small integer, taken modulo how many exist: the model
   and the real thing count the same way, so they agree on which one it is. *)

type 'a t = (int, 'a Lwt.t) Hashtbl.t

let create () : 'a t = Hashtbl.create 16

let add t p = Hashtbl.replace t (Hashtbl.length t) p

let count = Hashtbl.length

(* The promise [j] designates, if any exists. *)
let pick t j =
  let n = count t in
  if n = 0 then None else Some (Hashtbl.find t (j mod n))

(* The same choice, on the model's side. *)
let pick_index ~count j = if count = 0 then None else Some (j mod count)

let describe show p =
  match Lwt.state p with
  | Lwt.Return v -> show v
  | Lwt.Fail Lwt.Canceled -> "cancelled"
  | Lwt.Fail e -> "failed " ^ Printexc.to_string e
  | Lwt.Sleep -> "pending"

exception Boom

let () =
  Printexc.register_printer (function Boom -> Some "Boom" | _ -> None)

let answer s = STM.Res (STM.string, s)

let agrees (expected : string) (res : STM.res) =
  match res with
  | STM.Res ((STM.String, _), s) -> String.equal s expected
  | _ -> false
