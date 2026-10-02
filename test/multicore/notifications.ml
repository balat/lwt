(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Lwt_unix notifications under concurrency: the substrate of every wake-up
   that crosses a domain, [Lwt_multicore.run_on] included. It lives here rather
   than in test/unix for its dependencies: qcheck-lin and OCaml 5.

   Two checks, one per half of the machinery.

   THE TABLE OF NOTIFIERS, the OCaml half, is process-wide and any domain may
   create, stop, replace or call an entry. qcheck-lin runs random programs of
   those calls on two domains and checks that every outcome is explained by some
   sequential order. The case that matters most is a [~once] notification called
   from both sides at once: its lookup and its removal are one step, so exactly
   one caller runs the handler.

   DELIVERY, the C half: each loop's channel buffers the ids sent to it, from
   any thread, and the loop runs the handlers when it drains. The property: every
   send to a live notification runs its handler exactly once, on the domain that
   made it, whoever sent it; a [~once] notification runs at most once; a stopped
   one never runs. Sends come from two other domains, a plain system thread and
   the owner itself, while the owner's loop is NOT running, so the buffer fills
   up; some programs send more than its initial 4096 cells, which exercises its
   growth under contention. *)

let notifications = 4

(* Notifications 0 and 1 are [~once], 2 and 3 are not. *)
let is_once k = k < 2

(* +-----------------------------------------------------------------+
   | The table, linearisable                                         |
   +-----------------------------------------------------------------+ *)

(* Whether the handler ran IN THIS CALL: per domain, since the two domains call
   concurrently and each must learn about its own call only. *)
let ran : bool Domain.DLS.key = Domain.DLS.new_key (fun () -> false)
let handler () = Domain.DLS.set ran true

module Table_spec = struct
  type t = {
    ids : Lwt_unix.notification array;
    (* Notifications made during the run, stopped at cleanup so that the table
       does not grow from one program to the next. *)
    extra : Lwt_unix.notification list Atomic.t;
  }

  let init () =
    { ids =
        Array.init notifications (fun k ->
          Lwt_unix.make_notification ~once:(is_once k) handler);
      extra = Atomic.make [] }

  let cleanup t =
    Array.iter Lwt_unix.stop_notification t.ids;
    List.iter Lwt_unix.stop_notification (Atomic.get t.extra)

  let call t k =
    Domain.DLS.set ran false;
    Lwt_unix.call_notification t.ids.(k);
    Domain.DLS.get ran

  let stop t k = Lwt_unix.stop_notification t.ids.(k)
  let set t k = Lwt_unix.set_notification t.ids.(k) handler

  let make t =
    let id = Lwt_unix.make_notification handler in
    let rec record () =
      let l = Atomic.get t.extra in
      if not (Atomic.compare_and_set t.extra l (id :: l)) then record ()
    in
    record ()

  open Lin

  let index = int_bound (notifications - 1)

  let api =
    [ val_freq 3 "call" call (t @-> index @-> returning bool);
      val_ "stop" stop (t @-> index @-> returning unit);
      val_ "set" set (t @-> index @-> returning_or_exc unit);
      val_ "make" make (t @-> returning unit) ]
end

module Table = Lin_domain.Make (Table_spec)

(* +-----------------------------------------------------------------+
   | Delivery, exactly once                                          |
   +-----------------------------------------------------------------+ *)

(* A program: which notifications are stopped before anything is sent, and
   what each of the four senders sends, as a list of (notification, count). *)
type program = {
  stopped : bool array;
  sends : (int * int) list array;
}

let senders = 4

let print_program p =
  let sends l =
    String.concat " " (List.map (fun (k, n) -> Printf.sprintf "%dx%d" n k) l)
  in
  Printf.sprintf "stopped [%s]; %s"
    (String.concat ""
       (Array.to_list (Array.map (fun b -> if b then "x" else ".") p.stopped)))
    (String.concat " | " (Array.to_list (Array.map sends p.sends)))

let gen_program =
  let open QCheck.Gen in
  let send =
    pair (int_bound (notifications - 1))
      (oneof_weighted [ (6, int_range 1 20); (1, int_range 500 3000) ])
  in
  map2
    (fun stopped sends ->
      { stopped = Array.of_list stopped; sends = Array.of_list sends })
    (list_size (return notifications)
       (oneof_weighted [ (4, return false); (1, return true) ]))
    (list_size (return senders) (list_size (int_bound 6) send))

let arb_program = QCheck.make ~print:print_program gen_program

let deliver p =
  let owner = Domain.self () in
  let counts = Array.make notifications 0 in
  let elsewhere = Atomic.make false in
  let ids =
    Array.init notifications (fun k ->
      Lwt_unix.make_notification ~once:(is_once k) (fun () ->
        if Domain.self () <> owner then Atomic.set elsewhere true;
        counts.(k) <- counts.(k) + 1))
  in
  Array.iteri (fun k s -> if s then Lwt_unix.stop_notification ids.(k)) p.stopped;
  let send l =
    List.iter
      (fun (k, n) ->
        for _ = 1 to n do
          Lwt_unix.send_notification ids.(k)
        done)
      l
  in
  let go = Atomic.make false in
  let start () = while not (Atomic.get go) do Domain.cpu_relax () done in
  let d1 = Domain.spawn (fun () -> start (); send p.sends.(0)) in
  let d2 = Domain.spawn (fun () -> start (); send p.sends.(1)) in
  let th = Thread.create (fun () -> start (); send p.sends.(2)) () in
  Atomic.set go true;
  send p.sends.(3);
  Domain.join d1;
  Domain.join d2;
  Thread.join th;
  let sent = Array.make notifications 0 in
  Array.iter (List.iter (fun (k, n) -> sent.(k) <- sent.(k) + n)) p.sends;
  let expected k =
    if p.stopped.(k) then 0
    else if is_once k then min 1 sent.(k)
    else sent.(k)
  in
  let complete () =
    let rec all k = k = notifications || (counts.(k) >= expected k && all (k + 1)) in
    all 0
  in
  (* Every send has returned, so everything is in the buffer: draining it is
     a matter of the loop's next laps. Wait until the counts are reached, then
     a little more, so that a handler run too often is seen too. *)
  let deadline = Unix.gettimeofday () +. 5. in
  let rec wait () =
    if complete () || Unix.gettimeofday () > deadline then Lwt.return_unit
    else Lwt.bind (Lwt_unix.sleep 0.001) wait
  in
  Lwt_main.run (Lwt.bind (wait ()) (fun () -> Lwt_unix.sleep 0.01));
  Array.iter Lwt_unix.stop_notification ids;
  let ok = ref (not (Atomic.get elsewhere)) in
  for k = 0 to notifications - 1 do
    if counts.(k) <> expected k then begin
      ok := false;
      QCheck.Test.fail_reportf "notification %d: ran %d times, expected %d" k
        counts.(k) (expected k)
    end
  done;
  !ok || QCheck.Test.fail_report "a handler ran on another domain"

let delivery =
  QCheck.Test.make ~count:100
    ~name:"every send to a live notification is delivered exactly once"
    arb_program deliver

let () =
  QCheck_base_runner.run_tests_main
    [ Table.lin_test ~count:300 ~name:"the table of notifiers is linearisable";
      delivery ]
