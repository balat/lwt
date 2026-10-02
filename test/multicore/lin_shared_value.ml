(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Linearisability of the shared value, checked by qcheck-lin on real domains.

   A [Lwt_multicore.t] is the one structure here that any domain may settle,
   with or without a loop, so it is the one that must behave as if every
   operation happened at a single instant: of two domains that race to settle
   it, exactly one wins and the other gets [Invalid_argument]; [cancel] never
   fails; and whoever looks afterwards sees the winner's value, never a mix.
   qcheck-lin runs random programs on two domains and checks that what they
   observed is explained by SOME sequential interleaving of the same calls.

   [peek] is [await] observed at once: settled values come back synchronously,
   and a pending one leaves a waiter, which is cancelled before returning. That
   is what keeps it linearisable (the decision is taken under the value's lock)
   and it exercises the withdrawal of a waiter that a racing [resolve] may have
   taken already, the delicate path of this structure. *)

exception Boom of int

let () =
  Printexc.register_printer (function
    | Boom i -> Some (Printf.sprintf "Boom %d" i)
    | _ -> None)

let peek t =
  let p = Lwt_multicore.await t in
  match Lwt.state p with
  | Lwt.Return i -> Printf.sprintf "fulfilled %d" i
  | Lwt.Fail e -> "rejected " ^ Printexc.to_string e
  | Lwt.Sleep ->
    Lwt.cancel p;
    "pending"

module Spec = struct
  type t = int Lwt_multicore.t

  let init () = Lwt_multicore.create ()
  let cleanup _ = ()

  open Lin

  let api =
    [ val_ "resolve" Lwt_multicore.resolve (t @-> nat_small @-> returning_or_exc unit);
      val_ "reject"
        (fun t i -> Lwt_multicore.reject t (Boom i))
        (t @-> nat_small @-> returning_or_exc unit);
      val_ "cancel" Lwt_multicore.cancel (t @-> returning unit);
      val_ "is_pending" Lwt_multicore.is_pending (t @-> returning bool);
      val_freq 2 "peek" peek (t @-> returning string) ]
end

module Test = Lin_domain.Make (Spec)

let () =
  QCheck_base_runner.run_tests_main
    [ Test.lin_test ~count:500 ~name:"Lwt_multicore.t is linearisable" ]
