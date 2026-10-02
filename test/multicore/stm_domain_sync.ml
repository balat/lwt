(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Models of the shared Semaphore, Mutex and Stream, checked by qcheck-stm on
   two real domains at once: whatever the two did, the answers they got must be
   those of SOME interleaving of their calls against the model. This is what
   checks the counting, the hand-over and the closing rules under contention,
   which the example tests can only sample.

   What makes a blocking primitive testable this way: an operation that has to
   wait leaves a waiter and returns a pending promise, and that is all it does in
   the command. Nobody cancels it and no loop runs, so a waiter that is later
   served stays served: the unit, the lock or the room it was handed is IN THE
   MODEL from the instant it was handed over, which is when the server took it
   under the lock. The model therefore keeps, besides the visible state, how many
   waiters are queued.

   Cancelling a blocked operation is deliberately not a command here. A waiter
   served by the other domain and cancelled by its own before its loop has run
   has its resource in transit, handed back only when that loop drains its
   inbox, and no interleaving explains what a third call observes meanwhile.
   That is a property of the design, not of these models; sync.ml tests that the
   resource does come back.

   The two domains are kept alive until both have run all their commands
   ([wrap_cmd_seq]): a domain that exits retires its loop, which hands back what
   was handed to its waiters, and that would change the state under the other
   domain's feet. *)

(* Both parallel command sequences end at this barrier. Reset by [init_sut],
   which runs on the main domain before the two are spawned; the sequential
   prefix also runs there, and does not wait. *)
let finished = Atomic.make 0

let keep_alive thunk =
  let result = thunk () in
  if not (Domain.is_main_domain ()) then begin
    Atomic.incr finished;
    while Atomic.get finished < 2 do
      Domain.cpu_relax ()
    done
  end;
  result

let observe p ~ok =
  match Lwt.state p with
  | Lwt.Return v -> ok v
  | Lwt.Sleep -> "pending"
  | Lwt.Fail Lwt_multicore.Stream.Closed -> "closed"
  | Lwt.Fail e -> "raised " ^ Printexc.to_string e

let answer s = STM.Res (STM.string, s)

let agrees (expected : string) (res : STM.res) =
  match res with
  | STM.Res ((STM.String, _), s) -> String.equal s expected
  | _ -> false

let weighted l = QCheck.Gen.oneof_weighted l

(* +-----------------------------------------------------------------+
   | Semaphore        | _ :: waiting -> { s with pushers = waiting })                                               |
   +-----------------------------------------------------------------+ *)

module Semaphore_spec = struct
  include STM.SpecDefaults

  let initial = 1

  type cmd = Acquire | Release | Available

  let show_cmd = function
    | Acquire -> "acquire"
    | Release -> "release"
    | Available -> "available"

  (* Units available, and acquirers queued for one. Never both non-zero. *)
  type state = { count : int; queued : int }

  type sut = Lwt_multicore.Semaphore.t

  let init_state = { count = initial; queued = 0 }

  let init_sut () =
    Atomic.set finished 0;
    Lwt_multicore.Semaphore.create initial

  let cleanup _ = ()

  let arb_cmd _ =
    QCheck.make ~print:show_cmd
      (weighted
         [ (3, QCheck.Gen.return Acquire);
           (3, QCheck.Gen.return Release);
           (2, QCheck.Gen.return Available) ])

  let next_state cmd s =
    match cmd with
    | Acquire ->
      if s.count > 0 then { s with count = s.count - 1 }
      else { s with queued = s.queued + 1 }
    | Release ->
      (* The unit goes to the first queued acquirer, without ever being
         available to a third party. *)
      if s.queued > 0 then { s with queued = s.queued - 1 }
      else { s with count = s.count + 1 }
    | Available -> s

  let wrap_cmd_seq = keep_alive

  let run cmd sut =
    answer
      (match cmd with
       | Acquire ->
         observe (Lwt_multicore.Semaphore.acquire sut) ~ok:(fun () -> "acquired")
       | Release -> Lwt_multicore.Semaphore.release sut; "released"
       | Available ->
         Printf.sprintf "available %d" (Lwt_multicore.Semaphore.available sut))

  let postcond cmd s res =
    agrees
      (match cmd with
       | Acquire -> if s.count > 0 then "acquired" else "pending"
       | Release -> "released"
       | Available -> Printf.sprintf "available %d" s.count)
      res
end

(* +-----------------------------------------------------------------+
   | Mutex                                                           |
   +-----------------------------------------------------------------+ *)

module Mutex_spec = struct
  include STM.SpecDefaults

  type cmd = Lock | Unlock | Is_locked

  let show_cmd = function
    | Lock -> "lock"
    | Unlock -> "unlock"
    | Is_locked -> "is_locked"

  type state = { held : bool; queued : int }

  type sut = Lwt_multicore.Mutex.t

  let init_state = { held = false; queued = 0 }

  let init_sut () =
    Atomic.set finished 0;
    Lwt_multicore.Mutex.create ()

  let cleanup _ = ()

  let arb_cmd _ =
    QCheck.make ~print:show_cmd
      (weighted
         [ (3, QCheck.Gen.return Lock);
           (3, QCheck.Gen.return Unlock);
           (2, QCheck.Gen.return Is_locked) ])

  let next_state cmd s =
    match cmd with
    | Lock ->
      if s.held then { s with queued = s.queued + 1 } else { s with held = true }
    | Unlock ->
      (* Handed to the first waiter, the mutex stays held: nobody can jump the
         queue. Unlocking a free mutex does nothing. *)
      if not s.held then s
      else if s.queued > 0 then { s with queued = s.queued - 1 }
      else { s with held = false }
    | Is_locked -> s

  let wrap_cmd_seq = keep_alive

  let run cmd sut =
    answer
      (match cmd with
       | Lock -> observe (Lwt_multicore.Mutex.lock sut) ~ok:(fun () -> "locked")
       | Unlock -> Lwt_multicore.Mutex.unlock sut; "unlocked"
       | Is_locked -> Printf.sprintf "held %b" (Lwt_multicore.Mutex.is_locked sut))

  let postcond cmd s res =
    agrees
      (match cmd with
       | Lock -> if s.held then "pending" else "locked"
       | Unlock -> "unlocked"
       | Is_locked -> Printf.sprintf "held %b" s.held)
      res
end

(* +-----------------------------------------------------------------+
   | Stream                                                          |
   +-----------------------------------------------------------------+ *)

module Stream_spec = struct
  include STM.SpecDefaults

  let capacity = 2

  (* Which domain runs a command: the main one for the sequential prefix, then
     one of the two spawned ones. The model needs it for one rule, below. *)
  type origin = Main | Left | Right

  type op = Push of int | Take | Close | Length | Is_closed

  type cmd = { origin : origin; op : op }

  let show_origin = function Main -> "" | Left -> "L " | Right -> "R "

  let show_cmd { origin; op } =
    show_origin origin
    ^
    match op with
    | Push i -> Printf.sprintf "push %d" i
    | Take -> "take"
    | Close -> "close"
    | Length -> "length"
    | Is_closed -> "is_closed"

  (* What is buffered, how many takers wait for an item, which pushers wait for
     room (their domain and their value, in order), and whether the stream is
     closed.

     A pusher that waits holds NOTHING: when room appears it is woken, and it
     RETRIES its push (see [push]). Woken by another domain, the wake-up is
     posted to the pusher's loop, which does not run here: the room stays free
     for anyone, and the pusher simply leaves the queue.

     Woken by its OWN domain, its promise is resolved at once and the retry runs
     inside the [take] that made the room, but not atomically with it: the other
     domain can push into the room first, and the retry then queues again, at
     the back. No deterministic model says which happens, so [precond] keeps that
     case out. It is also what the case means: a waiting producer can be
     overtaken by a newcomer, and across domains for as long as its loop takes to
     run, which the shared Mutex and Semaphore do not allow (they hand over). *)
  type state = {
    items : int list;
    takers : int;
    pushers : (origin * int) list;
    closed : bool;
  }

  type sut = int Lwt_multicore.Stream.t

  let init_state = { items = []; takers = 0; pushers = []; closed = false }

  let init_sut () =
    Atomic.set finished 0;
    Lwt_multicore.Stream.create ~capacity

  let cleanup _ = ()

  let arb_op origin =
    QCheck.make ~print:show_cmd
      (QCheck.Gen.map
         (fun op -> { origin; op })
         (weighted
            [ (4, QCheck.Gen.map (fun i -> Push i) (QCheck.Gen.int_bound 99));
              (4, QCheck.Gen.return Take);
              (1, QCheck.Gen.return Close);
              (2, QCheck.Gen.return Length);
              (1, QCheck.Gen.return Is_closed) ]))

  let arb_cmd _ = arb_op Main

  let push origin i s =
    if s.closed then s
    else if s.takers > 0 then
      (* Straight to the first waiting taker. *)
      { s with takers = s.takers - 1 }
    else if List.length s.items < capacity then { s with items = s.items @ [ i ] }
    else { s with pushers = s.pushers @ [ (origin, i) ] }

  let next_state { origin; op } s =
    match op with
    | Push i -> push origin i s
    | Take -> (
      match s.items with
      | _ :: rest ->
        (* A slot was freed: the first waiting pusher is woken, and leaves. *)
        let pushers = match s.pushers with [] -> [] | _ :: w -> w in
        { s with items = rest; pushers }
      | [] -> if s.closed then s else { s with takers = s.takers + 1 })
    | Close ->
      (* Takers learn the end, pushers are rejected: both queues empty. *)
      { s with closed = true; takers = 0; pushers = [] }
    | Length | Is_closed -> s

  (* A take that would wake a pusher of its own domain; see the state. *)
  let precond { origin; op } s =
    match (op, s.items, s.pushers) with
    | Take, _ :: _, (o, _) :: _ -> o <> origin
    | _ -> true

  let wrap_cmd_seq = keep_alive

  let run { op; _ } sut =
    let module S = Lwt_multicore.Stream in
    answer
      (match op with
       | Push i -> observe (S.push sut i) ~ok:(fun () -> "pushed")
       | Take ->
         observe (S.take sut) ~ok:(function
           | None -> "end"
           | Some i -> Printf.sprintf "took %d" i)
       | Close -> S.close sut; "closed"
       | Length -> Printf.sprintf "length %d" (S.length sut)
       | Is_closed -> Printf.sprintf "is_closed %b" (S.is_closed sut))

  let postcond { op; _ } s res =
    agrees
      (match op with
       | Push _ ->
         if s.closed then "closed"
         else if s.takers > 0 || List.length s.items < capacity then "pushed"
         else "pending"
       | Take -> (
         match s.items with
         | i :: _ -> Printf.sprintf "took %d" i
         | [] -> if s.closed then "end" else "pending")
       | Close -> "closed"
       | Length -> Printf.sprintf "length %d" (List.length s.items)
       | Is_closed -> Printf.sprintf "is_closed %b" s.closed)
      res
end

module Semaphore_test = STM_domain.MakeExt (Semaphore_spec)
module Mutex_test = STM_domain.MakeExt (Mutex_spec)
module Stream_test = STM_domain.MakeExt (Stream_spec)

(* [agree_test_par] draws the three command lists from one generator; the
   stream's model needs to know which domain runs each command, so its lists are
   drawn from three, with STM_domain's own lengths and repetitions. And shrunk
   in place: STM's shrinker also moves commands from the parallel lists into the
   prefix, where a command tagged for a spawned domain would run on the main one
   and the model would be told the wrong origin. *)
let stream_test ~count ~name =
  let arb =
    let from origin _state = Stream_spec.arb_op origin in
    let triple = Stream_test.arb_triple 20 12 (from Main) (from Left) (from Right) in
    let list l = QCheck.Shrink.list l in
    QCheck.make
      ~print:(Util.print_triple_vertical Stream_spec.show_cmd)
      ~shrink:(QCheck.Shrink.triple list list list)
      (QCheck.gen triple)
  in
  QCheck.Test.make ~retries:10 ~count ~name arb (fun triple ->
    QCheck.assume (Stream_test.all_interleavings_ok triple);
    Util.repeat 25 Stream_test.agree_prop_par triple)

let () =
  QCheck_base_runner.run_tests_main
    [ Semaphore_test.agree_test_par ~count:200
        ~name:"Lwt_multicore.Semaphore on two domains, against a model";
      Mutex_test.agree_test_par ~count:200
        ~name:"Lwt_multicore.Mutex on two domains, against a model";
      stream_test ~count:200
        ~name:"Lwt_multicore.Stream on two domains, against a model" ]
