(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The order in which [Lwt.cancel] runs what it cancels, as in Lwt since its
   historical core: every cancelable promise the cancellation reaches is marked
   [Canceled] first, and only then do their callbacks run, last found first. So
   a callback never sees a sibling whose cancellation is still under way, and a
   promise reached twice is cancelled once. *)

open Test

let state p =
  match Lwt.state p with
  | Lwt.Sleep -> "Sleep"
  | Lwt.Return _ -> "Return"
  | Lwt.Fail Lwt.Canceled -> "Canceled"
  | Lwt.Fail _ -> "Fail"

(* Two cancelable tasks combined by [combine], each with an [on_cancel] that
   records which ran and what it saw of the other. *)
let siblings combine =
  let log = ref [] in
  let a, _ = Lwt.task () and b, _ = Lwt.task () in
  Lwt.on_cancel a (fun () -> log := ("a sees b " ^ state b) :: !log);
  Lwt.on_cancel b (fun () -> log := ("b sees a " ^ state a) :: !log);
  let r = combine [a; b] in
  Lwt.cancel r;
  (List.rev !log, state r)

let expected = (["b sees a Canceled"; "a sees b Canceled"], "Canceled")

let suite = suite "lwt cancel order" [
  test_direct "join: both are cancelled before either callback runs"
    (fun () -> siblings Lwt.join = expected);

  test_direct "choose: both are cancelled before either callback runs"
    (fun () -> siblings Lwt.choose = expected);

  test_direct "pick: both are cancelled before either callback runs"
    (fun () -> siblings Lwt.pick = expected);

  test_direct "both: both are cancelled before either callback runs"
    (fun () ->
      siblings (function
        | [a; b] -> Lwt.map ignore (Lwt.both a b)
        | _ -> Lwt.return_unit)
      = expected);

  test_direct "a cancel callback cannot resolve a sibling being cancelled"
    (fun () ->
      let a, _ = Lwt.task () and b, wb = Lwt.task () in
      Lwt.on_cancel a (fun () ->
        try Lwt.wakeup wb () with Invalid_argument _ -> ());
      Lwt.cancel (Lwt.join [a; b]);
      state a = "Canceled" && state b = "Canceled");

  test_direct "a promise reached twice is cancelled once"
    (fun () ->
      let p, _ = Lwt.task () in
      let runs = ref 0 in
      Lwt.on_cancel p (fun () -> incr runs);
      let r =
        Lwt.join
          [ Lwt.bind p (fun () -> Lwt.return_unit);
            Lwt.bind p (fun () -> Lwt.return_unit) ]
      in
      Lwt.cancel r;
      !runs = 1 && state r = "Canceled");

  test_direct "the caller of cancel sees every cancellation done"
    (fun () ->
      let p, _ = Lwt.task () in
      let seen = ref "" in
      let r = Lwt.bind p (fun () -> Lwt.return_unit) in
      Lwt.on_failure r (fun _ -> seen := "rejected");
      Lwt.cancel r;
      !seen = "rejected" && state p = "Canceled");
]
