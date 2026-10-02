(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A model of Lwt_pool, checked by qcheck-stm. The pool's subtle part is its
   accounting: how many members exist, which one each user gets, and what
   happens to a member when its user finishes, fails or is cancelled, before or
   after a [clear]. Each [use] here starts a job that later commands finish,
   fail or cancel, so those paths interleave with waiting users and clears in
   orders nobody wrote by hand. Sequential: a pool belongs to one domain.

   Members are numbered in the order the pool creates them, and their creation,
   validation and disposal resolve at once, so that no loop has to run. *)

let size = 2

module Spec = struct
  type cmd =
    | Use
    | Finish of int
    | Fail of int
    | Cancel of int
    | Clear
    | Status of int
    | Stats

  let show_cmd = function
    | Use -> "use"
    | Finish j -> Printf.sprintf "finish %d" j
    | Fail j -> Printf.sprintf "fail %d" j
    | Cancel j -> Printf.sprintf "cancel %d" j
    | Clear -> "clear"
    | Status j -> Printf.sprintf "status %d" j
    | Stats -> "stats"

  (* [created] and [disposed] count members; the free ones, in the order the pool
     hands them out; the users waiting for one; the users running, with their
     member and the generation of the pool when they got it (a [clear] starts a
     new one, and a member from an older one is disposed when its user is
     done); and what each user's promise should say. *)
  type state = {
    created : int;
    disposed : int;
    free : int list;
    waiting : int list;
    running : (int * (int * int)) list;
    generation : int;
    status : string list;
  }

  type sut = {
    pool : int Lwt_pool.t;
    users : string Handles.t;
    (* The member and the job of each user that is running. *)
    jobs : (int, int * unit Lwt.u) Hashtbl.t;
    made : int ref;
    gone : int ref;
  }

  let init_state =
    { created = 0; disposed = 0; free = []; waiting = []; running = [];
      generation = 0; status = [] }

  let init_sut () =
    let made = ref 0 and gone = ref 0 in
    let create () =
      let id = !made in
      incr made;
      Lwt.return id
    in
    let dispose _ = incr gone; Lwt.return_unit in
    { pool = Lwt_pool.create size ~dispose create;
      users = Handles.create ();
      jobs = Hashtbl.create 16;
      made;
      gone }

  let cleanup _ = ()

  let arb_cmd _ =
    let open QCheck.Gen in
    let user = int_bound 7 in
    QCheck.make ~print:show_cmd
      (oneof_weighted
         [ (4, return Use);
           (3, map (fun j -> Finish j) user);
           (2, map (fun j -> Fail j) user);
           (2, map (fun j -> Cancel j) user);
           (1, return Clear);
           (2, map (fun j -> Status j) user);
           (2, return Stats) ])

  let set status k v = List.mapi (fun i s -> if i = k then v else s) status
  let count s = List.length s.status
  let running c = Printf.sprintf "running on %d" c

  let serve s k c =
    { s with
      running = s.running @ [ (k, (c, s.generation)) ];
      status = set s.status k (running c) }

  (* A member came back: to the first waiting user, or to the pool. *)
  let release s c =
    match s.waiting with
    | w :: rest -> serve { s with waiting = rest } w c
    | [] -> { s with free = s.free @ [ c ] }

  (* A member was disposed: the first waiting user, if any, gets a new one. *)
  let replace_disposed s =
    match s.waiting with
    | w :: rest -> serve { s with waiting = rest; created = s.created + 1 } w s.created
    | [] -> s

  (* User [k] is done with member [c], acquired in generation [g], and its
     promise says [outcome]. *)
  let finished s k (c, g) outcome =
    let s =
      { s with running = List.remove_assoc k s.running; status = set s.status k outcome }
    in
    if g < s.generation then replace_disposed { s with disposed = s.disposed + 1 }
    else release s c

  let picked s j = Handles.pick_index ~count:(count s) j

  let next_state cmd s =
    match cmd with
    | Use -> (
      let k = count s in
      let s = { s with status = s.status @ [ "pending" ] } in
      match s.free with
      | c :: rest -> serve { s with free = rest } k c
      | [] ->
        if s.created - s.disposed < size then
          serve { s with created = s.created + 1 } k s.created
        else { s with waiting = s.waiting @ [ k ] })
    | Finish j | Fail j | Cancel j -> (
      match picked s j with
      | None -> s
      | Some k -> (
        match List.assoc_opt k s.running, cmd with
        | Some member, Finish _ -> finished s k member "done"
        | Some member, Fail _ -> finished s k member "failed Boom"
        | Some member, _ -> finished s k member "cancelled"
        | None, Cancel _ when List.mem k s.waiting ->
          { s with
            waiting = List.filter (( <> ) k) s.waiting;
            status = set s.status k "cancelled" }
        | None, _ -> s))
    | Clear ->
      { s with
        free = [];
        disposed = s.disposed + List.length s.free;
        generation = s.generation + 1 }
    | Status _ | Stats -> s

  let precond _ _ = true

  let describe sut k p =
    match Lwt.state p, Hashtbl.find_opt sut.jobs k with
    | Lwt.Sleep, Some (c, _) -> running c
    | _ -> Handles.describe Fun.id p

  let user sut j f =
    let n = Handles.count sut.users in
    match Handles.pick_index ~count:n j with
    | None -> "none"
    | Some k ->
      let p = Hashtbl.find sut.users k in
      f k p;
      describe sut k p

  let job_of sut k = Hashtbl.find_opt sut.jobs k

  let run cmd sut =
    Handles.answer
      (match cmd with
       | Use ->
         let k = Handles.count sut.users in
         let work c =
           let job, resolver = Lwt.task () in
           Hashtbl.replace sut.jobs k (c, resolver);
           job
         in
         let p =
           Lwt.map (fun () -> "done")
             (Lwt_pool.use sut.pool work)
         in
         (* The job, once it has ended, no longer says which member it has. *)
         Lwt.on_termination p (fun () -> Hashtbl.remove sut.jobs k);
         Handles.add sut.users p;
         describe sut k p
       | Finish j ->
         user sut j (fun k _ ->
           match job_of sut k with
           | Some (_, r) -> Lwt.wakeup r ()
           | None -> ())
       | Fail j ->
         user sut j (fun k _ ->
           match job_of sut k with
           | Some (_, r) -> Lwt.wakeup_exn r Handles.Boom
           | None -> ())
       | Cancel j -> user sut j (fun _ p -> Lwt.cancel p)
       | Clear ->
         (match Lwt.state (Lwt_pool.clear sut.pool) with
          | Lwt.Return () -> "cleared"
          | _ -> "clear did not finish")
       | Status j -> user sut j (fun _ _ -> ())
       | Stats ->
         Printf.sprintf "created %d disposed %d waiting %d" !(sut.made) !(sut.gone)
           (Lwt_pool.wait_queue_length sut.pool))

  let postcond cmd s res =
    let after = next_state cmd s in
    Handles.agrees
      (match cmd with
       | Use -> List.nth after.status (count s)
       | Finish j | Fail j | Cancel j | Status j -> (
         match picked after j with None -> "none" | Some k -> List.nth after.status k)
       | Clear -> "cleared"
       | Stats ->
         Printf.sprintf "created %d disposed %d waiting %d" s.created s.disposed
           (List.length s.waiting))
      res
end

module Test = STM_sequential.Make (Spec)

let () =
  QCheck_base_runner.run_tests_main
    [ Test.agree_test ~count:2000 ~name:"Lwt_pool against a model" ]
