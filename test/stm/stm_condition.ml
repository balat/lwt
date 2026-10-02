(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A model of Lwt_condition, with and without its mutex, checked by qcheck-stm.

   The interesting part is the mutex. [wait ~mutex] releases it, waits, and
   takes it again before returning, whatever ended the wait: a signal, a
   broadcast of an exception, or a cancellation. So a waiter that has been
   signalled may still be pending, queued for the mutex behind other lockers,
   and cancelling it then withdraws it from the MUTEX's queue. The model keeps
   both queues, and each entry of the mutex's queue says what its promise will
   say once it gets the lock. Sequential: both belong to one domain. *)

module Spec = struct
  type cmd =
    | Lock
    | Unlock
    | Wait
    | Wait_mutex
    | Signal of int
    | Broadcast of int
    | Broadcast_exn
    | Cancel of int
    | Status of int

  let show_cmd = function
    | Lock -> "lock"
    | Unlock -> "unlock"
    | Wait -> "wait"
    | Wait_mutex -> "wait ~mutex"
    | Signal v -> Printf.sprintf "signal %d" v
    | Broadcast v -> Printf.sprintf "broadcast %d" v
    | Broadcast_exn -> "broadcast_exn"
    | Cancel j -> Printf.sprintf "cancel %d" j
    | Status j -> Printf.sprintf "status %d" j

  (* [held]: the mutex. [lockers]: who waits for it, in order, each with what
     its promise says once it has the lock ("done" for a plain [lock], the
     outcome of the wait for a waiter taking it back). [waiters]: who waits on
     the condition, and whether with the mutex. *)
  type state = {
    held : bool;
    lockers : (int * string) list;
    waiters : (int * bool) list;
    status : string list;
  }

  type sut = {
    mutex : Lwt_mutex.t;
    cond : int Lwt_condition.t;
    kept : string Handles.t;
  }

  let init_state = { held = false; lockers = []; waiters = []; status = [] }

  let init_sut () =
    { mutex = Lwt_mutex.create ();
      cond = Lwt_condition.create ();
      kept = Handles.create () }

  let cleanup _ = ()

  let arb_cmd _ =
    let open QCheck.Gen in
    QCheck.make ~print:show_cmd
      (oneof_weighted
         [ (3, return Lock);
           (3, return Unlock);
           (2, return Wait);
           (3, return Wait_mutex);
           (3, map (fun v -> Signal v) (int_bound 99));
           (1, map (fun v -> Broadcast v) (int_bound 99));
           (1, return Broadcast_exn);
           (2, map (fun j -> Cancel j) (int_bound 7));
           (2, map (fun j -> Status j) (int_bound 7)) ])

  let set status k v = List.mapi (fun i s -> if i = k then v else s) status
  let count s = List.length s.status
  let value v = Printf.sprintf "value %d" v
  let boom = "failed Boom"

  (* [k] wants the mutex, and will say [outcome] once it has it. *)
  let want_lock s k outcome =
    if s.held then { s with lockers = s.lockers @ [ (k, outcome) ] }
    else { s with held = true; status = set s.status k outcome }

  let unlock s =
    if not s.held then s
    else
      match s.lockers with
      | (k, outcome) :: rest -> { s with lockers = rest; status = set s.status k outcome }
      | [] -> { s with held = false }

  (* The wait of [k] has ended with [outcome]. *)
  let ended s (k, with_mutex) outcome =
    if with_mutex then want_lock s k outcome else { s with status = set s.status k outcome }

  let next_state cmd s =
    let k = count s in
    match cmd with
    | Lock -> want_lock { s with status = s.status @ [ "pending" ] } k "done"
    | Unlock -> unlock s
    | Wait -> { s with waiters = s.waiters @ [ (k, false) ]; status = s.status @ [ "pending" ] }
    | Wait_mutex ->
      (* Queued on the condition first, then the mutex is released. *)
      let s = { s with waiters = s.waiters @ [ (k, true) ]; status = s.status @ [ "pending" ] } in
      unlock s
    | Signal v -> (
      match s.waiters with
      | w :: rest -> ended { s with waiters = rest } w (value v)
      | [] -> s)
    | Broadcast v ->
      List.fold_left (fun s w -> ended s w (value v)) { s with waiters = [] } s.waiters
    | Broadcast_exn ->
      List.fold_left (fun s w -> ended s w boom) { s with waiters = [] } s.waiters
    | Cancel j -> (
      match Handles.pick_index ~count:k j with
      | Some i when List.nth s.status i = "pending" -> (
        match List.assoc_opt i s.waiters, List.mem_assoc i s.lockers with
        | Some with_mutex, _ ->
          (* Withdrawn from the condition; a waiter with the mutex still takes
             it back before its promise is rejected. *)
          let s = { s with waiters = List.remove_assoc i s.waiters } in
          ended s (i, with_mutex) "cancelled"
        | None, true ->
          { s with lockers = List.remove_assoc i s.lockers; status = set s.status i "cancelled" }
        | None, false -> s)
      | _ -> s)
    | Status _ -> s

  let precond _ _ = true

  let run cmd sut =
    let describe = Handles.describe Fun.id in
    let keep p = Handles.add sut.kept p; describe p in
    Handles.answer
      (match cmd with
       | Lock -> keep (Lwt.map (fun () -> "done") (Lwt_mutex.lock sut.mutex))
       | Unlock -> Lwt_mutex.unlock sut.mutex; "unlocked"
       | Wait -> keep (Lwt.map value (Lwt_condition.wait sut.cond))
       | Wait_mutex ->
         keep (Lwt.map value (Lwt_condition.wait ~mutex:sut.mutex sut.cond))
       | Signal v -> Lwt_condition.signal sut.cond v; "signalled"
       | Broadcast v -> Lwt_condition.broadcast sut.cond v; "broadcast"
       | Broadcast_exn ->
         Lwt_condition.broadcast_exn sut.cond Handles.Boom; "broadcast"
       | Cancel j -> (
         match Handles.pick sut.kept j with
         | None -> "none"
         | Some p -> Lwt.cancel p; describe p)
       | Status j -> (
         match Handles.pick sut.kept j with
         | None -> "none"
         | Some p -> describe p))

  let postcond cmd s res =
    let after = next_state cmd s in
    let picked j =
      match Handles.pick_index ~count:(count after) j with
      | None -> "none"
      | Some i -> List.nth after.status i
    in
    Handles.agrees
      (match cmd with
       | Lock | Wait | Wait_mutex -> List.nth after.status (count s)
       | Unlock -> "unlocked"
       | Signal _ -> "signalled"
       | Broadcast _ | Broadcast_exn -> "broadcast"
       | Cancel j | Status j -> picked j)
      res
end

module Test = STM_sequential.Make (Spec)

let () =
  QCheck_base_runner.run_tests_main
    [ Test.agree_test ~count:2000 ~name:"Lwt_condition and its mutex against a model" ]
