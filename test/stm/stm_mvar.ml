(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A model of Lwt_mvar, checked by qcheck-stm. Writers that find it full and
   readers that find it empty are kept, and later commands serve, cancel or
   look at them: the two queues, the hand-over in both directions and the
   withdrawal of a cancelled waiter. Sequential: an Lwt_mvar belongs to one
   domain. No loop runs; resolution is synchronous. *)

module Spec = struct
  type cmd =
    | Put of int
    | Take
    | Take_available
    | Cancel of int
    | Status of int
    | Is_empty

  let show_cmd = function
    | Put v -> Printf.sprintf "put %d" v
    | Take -> "take"
    | Take_available -> "take_available"
    | Cancel j -> Printf.sprintf "cancel %d" j
    | Status j -> Printf.sprintf "status %d" j
    | Is_empty -> "is_empty"

  (* The contents, the writers waiting (by number, with their value), the
     readers waiting, and what each kept promise should say. Puts and takes are
     numbered together, in the order the commands made them. *)
  type state = {
    contents : int option;
    writers : (int * int) list;
    readers : int list;
    status : string list;
  }

  (* A put's promise says "done"; a take's says the value. Both are kept as
     strings so that one table holds them. *)
  type sut = { mvar : int Lwt_mvar.t; kept : string Handles.t }

  let init_state = { contents = None; writers = []; readers = []; status = [] }

  let init_sut () =
    { mvar = Lwt_mvar.create_empty (); kept = Handles.create () }

  let cleanup _ = ()

  let arb_cmd _ =
    let open QCheck.Gen in
    QCheck.make ~print:show_cmd
      (oneof_weighted
         [ (4, map (fun v -> Put v) (int_bound 99));
           (4, return Take);
           (1, return Take_available);
           (2, map (fun j -> Cancel j) (int_bound 7));
           (2, map (fun j -> Status j) (int_bound 7));
           (1, return Is_empty) ])

  let set status k v = List.mapi (fun i s -> if i = k then v else s) status
  let count s = List.length s.status
  let value v = Printf.sprintf "value %d" v

  (* The mvar was emptied: the first waiting writer fills it. *)
  let next_writer s =
    match s.writers with
    | (k, v) :: rest ->
      { s with contents = Some v; writers = rest; status = set s.status k "done" }
    | [] -> { s with contents = None }

  let next_state cmd s =
    let k = count s in
    match cmd with
    | Put v -> (
      match s.contents with
      | Some _ ->
        { s with writers = s.writers @ [ (k, v) ]; status = s.status @ [ "pending" ] }
      | None -> (
        match s.readers with
        (* Straight to the first waiting reader. *)
        | r :: rest ->
          { s with readers = rest; status = set s.status r (value v) @ [ "done" ] }
        | [] -> { s with contents = Some v; status = s.status @ [ "done" ] }))
    | Take -> (
      match s.contents with
      | Some v ->
        let s = next_writer s in
        { s with status = s.status @ [ value v ] }
      | None ->
        { s with readers = s.readers @ [ k ]; status = s.status @ [ "pending" ] })
    | Take_available -> (
      match s.contents with Some _ -> next_writer s | None -> s)
    | Cancel j -> (
      match Handles.pick_index ~count:k j with
      | Some i when List.nth s.status i = "pending" ->
        { s with
          writers = List.filter (fun (w, _) -> w <> i) s.writers;
          readers = List.filter (( <> ) i) s.readers;
          status = set s.status i "cancelled" }
      | _ -> s)
    | Status _ | Is_empty -> s

  let precond _ _ = true

  let run cmd sut =
    let describe = Handles.describe Fun.id in
    let keep p = Handles.add sut.kept p; describe p in
    Handles.answer
      (match cmd with
       | Put v -> keep (Lwt.map (fun () -> "done") (Lwt_mvar.put sut.mvar v))
       | Take -> keep (Lwt.map value (Lwt_mvar.take sut.mvar))
       | Take_available -> (
         match Lwt_mvar.take_available sut.mvar with
         | Some v -> Printf.sprintf "some %d" v
         | None -> "none")
       | Cancel j -> (
         match Handles.pick sut.kept j with
         | None -> "none"
         | Some p -> Lwt.cancel p; describe p)
       | Status j -> (
         match Handles.pick sut.kept j with
         | None -> "none"
         | Some p -> describe p)
       | Is_empty -> Printf.sprintf "empty %b" (Lwt_mvar.is_empty sut.mvar))

  let postcond cmd s res =
    let k = count s in
    let picked j =
      match Handles.pick_index ~count:k j with
      | None -> "none"
      | Some i -> List.nth s.status i
    in
    Handles.agrees
      (match cmd with
       | Put _ -> if s.contents = None then "done" else "pending"
       | Take -> (match s.contents with Some v -> value v | None -> "pending")
       | Take_available -> (
         match s.contents with Some v -> Printf.sprintf "some %d" v | None -> "none")
       | Cancel j -> (
         match picked j with "pending" -> "cancelled" | other -> other)
       | Status j -> picked j
       | Is_empty -> Printf.sprintf "empty %b" (s.contents = None))
      res
end

module Test = STM_sequential.Make (Spec)

let () =
  QCheck_base_runner.run_tests_main
    [ Test.agree_test ~count:1000 ~name:"Lwt_mvar against a model" ]
