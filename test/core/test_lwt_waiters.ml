(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The waiter list of a pending promise: unlinking a removable waiter and
   splicing one list into another must both be O(1), and the order in which
   waiters run after a splice is Lwt's historical order.

   The complexity tests bound ALLOCATION rather than time, so that they are
   deterministic: a linear-time removal or splice rebuilds whole lists and
   allocates quadratically, which no machine can hide. *)

open Test

let allocated_words f =
  let before = Gc.minor_words () in
  f ();
  Gc.minor_words () -. before

let all_fulfilled_with v rs = Array.for_all (fun r -> Lwt.state r = Lwt.Return v) rs

let suite = suite "lwt_waiters" [
  test_direct "removable: concurrent picks against one long-lived promise"
    (fun () ->
      let n = 20_000 in
      let stop, _ = Lwt.wait () in
      let requests =
        Array.init n (fun _ ->
          let p, w = Lwt.task () in
          (Lwt.pick [stop; p], w))
      in
      let words =
        allocated_words (fun () ->
          Array.iter (fun (_, w) -> Lwt.wakeup w ()) requests)
      in
      all_fulfilled_with () (Array.map fst requests)
      && words < 100. *. float_of_int n);

  test_direct "removable: concurrent chooses against one long-lived promise"
    (fun () ->
      let n = 20_000 in
      let stop, _ = Lwt.wait () in
      let requests =
        Array.init n (fun _ ->
          let p, w = Lwt.task () in
          (Lwt.choose [stop; p], w))
      in
      let words =
        allocated_words (fun () ->
          Array.iter (fun (_, w) -> Lwt.wakeup w ()) requests)
      in
      all_fulfilled_with () (Array.map fst requests)
      && words < 100. *. float_of_int n);

  test_direct "forward: splicing into a promise carrying many waiters"
    (fun () ->
      let n = 5_000 in
      let shared, resolve_shared = Lwt.wait () in
      for _ = 1 to n do
        Lwt.on_success shared ignore
      done;
      let binds =
        Array.init n (fun _ ->
          let p, w = Lwt.task () in
          (Lwt.bind p (fun () -> shared), w))
      in
      let words =
        allocated_words (fun () ->
          Array.iter (fun (_, w) -> Lwt.wakeup w ()) binds)
      in
      Lwt.wakeup resolve_shared ();
      all_fulfilled_with () (Array.map fst binds)
      && words < 100. *. float_of_int n);

  test_direct "forward: the bind result's waiters run before the returned promise's"
    (fun () ->
      let log = ref [] in
      let note s () = log := s :: !log in
      let p, w = Lwt.task () in
      let q, wq = Lwt.task () in
      Lwt.on_success q (note "q1");
      Lwt.on_success q (note "q2");
      let r = Lwt.bind p (fun () -> q) in
      Lwt.on_success r (note "r1");
      Lwt.on_success r (note "r2");
      Lwt.wakeup w ();
      Lwt.wakeup wq ();
      List.rev !log = ["r2"; "r1"; "q2"; "q1"]);

  test_direct "removable: the loser does not retain the winner"
    (fun () ->
      let stop, _ = Lwt.wait () in
      let weak = Weak.create 1 in
      (* In its own function, so that no stack slot keeps the winner alive. *)
      let race () =
        let p, w = Lwt.task () in
        let _ : unit Lwt.t = Lwt.pick [stop; p] in
        Weak.set weak 0 (Some p);
        Lwt.wakeup w ()
      in
      race ();
      Gc.full_major ();
      not (Weak.check weak 0));

  test_direct "removable: unlinking the oldest, a middle and the newest node"
    (fun () ->
      let stop, resolve_stop = Lwt.wait () in
      let race () =
        let p, w = Lwt.task () in
        (Lwt.pick [stop; p], w)
      in
      let r1, w1 = race () in
      let r2, w2 = race () in
      let r3, w3 = race () in
      Lwt.wakeup w2 "b";
      Lwt.wakeup w1 "a";
      Lwt.wakeup w3 "c";
      let r4, _ = race () in
      let seen = ref [] in
      Lwt.on_success stop (fun s -> seen := s :: !seen);
      Lwt.wakeup resolve_stop "stop";
      Lwt.state r1 = Lwt.Return "a"
      && Lwt.state r2 = Lwt.Return "b"
      && Lwt.state r3 = Lwt.Return "c"
      && Lwt.state r4 = Lwt.Return "stop"
      && !seen = ["stop"]);

  test_direct "removable: unlinking after the promise was forwarded"
    (fun () ->
      let stop, resolve_stop = Lwt.wait () in
      let p, w = Lwt.task () in
      let picked = Lwt.pick [stop; p] in
      let src, resolve_src = Lwt.task () in
      (* [stop]'s waiters move into [r]'s list here. *)
      let r = Lwt.bind src (fun () -> stop) in
      Lwt.wakeup resolve_src ();
      (* The pick's node is now in [r]'s list, and is unlinked from there. *)
      Lwt.wakeup w 1;
      let seen = ref 0 in
      Lwt.on_success r (fun v -> seen := v);
      Lwt.wakeup resolve_stop 7;
      Lwt.state picked = Lwt.Return 1
      && Lwt.state r = Lwt.Return 7
      && Lwt.state stop = Lwt.Return 7
      && !seen = 7);

  test_direct "pick: the same promise listed twice"
    (fun () ->
      let p, w = Lwt.task () in
      let r = Lwt.pick [p; p] in
      Lwt.wakeup w 1;
      Lwt.state r = Lwt.Return 1);

  test_direct "protected: cancelled, then the source resolves"
    (fun () ->
      let p, w = Lwt.task () in
      let r = Lwt.protected p in
      Lwt.cancel r;
      Lwt.wakeup w 2;
      Lwt.state r = Lwt.Fail Lwt.Canceled && Lwt.state p = Lwt.Return 2);
]
