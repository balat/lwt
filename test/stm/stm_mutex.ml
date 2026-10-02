(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A model of Lwt_mutex, checked by qcheck-stm. Lockers that have to wait are
   kept, and later commands unlock, cancel or look at them, so that the queue,
   the hand-over and the withdrawal of a cancelled waiter are all exercised in
   orders nobody wrote by hand. Sequential: an Lwt_mutex belongs to one domain.

   No loop runs. Everything a command does to these promises happens before it
   returns, since resolution in Lwt is synchronous. *)

module Spec = struct
  type cmd = Lock | Unlock | Cancel of int | Status of int | Is_locked | Is_empty

  let show_cmd = function
    | Lock -> "lock"
    | Unlock -> "unlock"
    | Cancel j -> Printf.sprintf "cancel %d" j
    | Status j -> Printf.sprintf "status %d" j
    | Is_locked -> "is_locked"
    | Is_empty -> "is_empty"

  (* Whether the mutex is held, the lockers waiting for it in order, and what
     each locker's promise should say, by number. *)
  type state = { held : bool; queue : int list; status : string list }

  type sut = { mutex : Lwt_mutex.t; lockers : unit Handles.t }

  let init_state = { held = false; queue = []; status = [] }
  let init_sut () = { mutex = Lwt_mutex.create (); lockers = Handles.create () }
  let cleanup _ = ()

  let arb_cmd _ =
    let open QCheck.Gen in
    QCheck.make ~print:show_cmd
      (oneof_weighted
         [ (4, return Lock);
           (4, return Unlock);
           (2, map (fun j -> Cancel j) (int_bound 7));
           (2, map (fun j -> Status j) (int_bound 7));
           (1, return Is_locked);
           (1, return Is_empty) ])

  let set status k v = List.mapi (fun i s -> if i = k then v else s) status
  let count s = List.length s.status

  let next_state cmd s =
    match cmd with
    | Lock ->
      let k = count s in
      if s.held then { s with queue = s.queue @ [ k ]; status = s.status @ [ "pending" ] }
      else { s with held = true; status = s.status @ [ "done" ] }
    | Unlock -> (
      if not s.held then s
      else
        match s.queue with
        (* Handed to the first waiter: the mutex stays held. *)
        | k :: rest -> { s with queue = rest; status = set s.status k "done" }
        | [] -> { s with held = false })
    | Cancel j -> (
      match Handles.pick_index ~count:(count s) j with
      | Some k when List.nth s.status k = "pending" ->
        { s with
          queue = List.filter (( <> ) k) s.queue;
          status = set s.status k "cancelled" }
      | _ -> s)
    | Status _ | Is_locked | Is_empty -> s

  let precond _ _ = true

  let run cmd sut =
    let describe = Handles.describe (fun () -> "done") in
    Handles.answer
      (match cmd with
       | Lock ->
         let p = Lwt_mutex.lock sut.mutex in
         Handles.add sut.lockers p;
         describe p
       | Unlock -> Lwt_mutex.unlock sut.mutex; "unlocked"
       | Cancel j -> (
         match Handles.pick sut.lockers j with
         | None -> "none"
         | Some p -> Lwt.cancel p; describe p)
       | Status j -> (
         match Handles.pick sut.lockers j with
         | None -> "none"
         | Some p -> describe p)
       | Is_locked -> Printf.sprintf "locked %b" (Lwt_mutex.is_locked sut.mutex)
       | Is_empty -> Printf.sprintf "empty %b" (Lwt_mutex.is_empty sut.mutex))

  let postcond cmd s res =
    let picked j =
      match Handles.pick_index ~count:(count s) j with
      | None -> "none"
      | Some k -> List.nth s.status k
    in
    Handles.agrees
      (match cmd with
       | Lock -> if s.held then "pending" else "done"
       | Unlock -> "unlocked"
       | Cancel j -> (
         match picked j with "pending" -> "cancelled" | other -> other)
       | Status j -> picked j
       | Is_locked -> Printf.sprintf "locked %b" s.held
       | Is_empty -> Printf.sprintf "empty %b" (s.queue = []))
      res
end

module Test = STM_sequential.Make (Spec)

let () =
  QCheck_base_runner.run_tests_main
    [ Test.agree_test ~count:1000 ~name:"Lwt_mutex against a model" ]
