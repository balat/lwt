(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* A MODEL of the bounded channel, checked against the real thing by qcheck-stm.
   An example test says a known scenario works; a model goes looking for the
   scenarios nobody thought of, which is what the bound, the ordering and the
   closing rules deserve.

   What makes this possible without any timing games: the channel's operations
   resolve SYNCHRONOUSLY when they do not have to wait. So "did it block?" is
   simply [Lwt.state] on the returned promise, with no loop to run and nothing to
   race. A blocked operation is cancelled at once, which keeps the real channel in
   step with the model AND exercises the withdrawal path on every blocked command.

   Sequential, deliberately. STM's parallel mode wants a linearisable model, and
   operations that BLOCK are not linearisable against a deterministic model: two
   domains contending would make "did it block" depend on timing. Concurrency is
   tested by example instead, in sync.ml and stream.ml, where the interleaving is
   forced rather than sampled. *)

let capacity = 3

module Spec = struct
  type cmd = Push of int | Take | Close | Length

  let show_cmd = function
    | Push i -> Printf.sprintf "push %d" i
    | Take -> "take"
    | Close -> "close"
    | Length -> "length"

  (* The model: what is in the channel, and whether it is closed. *)
  type state = { items : int list; closed : bool }

  type sut = int Lwt_multicore.Stream.t

  let init_state = { items = []; closed = false }
  let init_sut () = Lwt_multicore.Stream.create ~capacity
  let cleanup s = Lwt_multicore.Stream.close s

  let arb_cmd _state =
    QCheck.make ~print:show_cmd
      (QCheck.Gen.oneof_weighted
         [ (4, QCheck.Gen.map (fun i -> Push i) (QCheck.Gen.int_bound 99));
           (4, QCheck.Gen.return Take);
           (1, QCheck.Gen.return Close);
           (2, QCheck.Gen.return Length) ])

  let next_state cmd state =
    match cmd with
    | Push i ->
      if state.closed then state
      else if List.length state.items >= capacity then state
      else { state with items = state.items @ [ i ] }
    | Take -> (
      match state.items with [] -> state | _ :: rest -> { state with items = rest })
    | Close -> { state with closed = true }
    | Length -> state

  let precond _cmd _state = true

  (* Every command reports a string, which keeps the model a plain observation
     function and makes a counterexample readable at a glance. "blocked" is what a
     promise that had to wait looks like; it is cancelled before returning, so
     neither the channel nor the model is left with a waiter, and the withdrawal
     path gets exercised on every blocked command. *)
  let observe p ~ok =
    match Lwt.state p with
    | Lwt.Return v -> ok v
    | Lwt.Sleep ->
      Lwt.cancel p;
      "blocked"
    | Lwt.Fail Lwt_multicore.Stream.Closed -> "closed"
    | Lwt.Fail e -> "raised " ^ Printexc.to_string e

  let run cmd sut =
    let answer =
      match cmd with
      | Push i -> observe (Lwt_multicore.Stream.push sut i) ~ok:(fun () -> "pushed")
      | Take ->
        observe (Lwt_multicore.Stream.take sut) ~ok:(function
          | None -> "end"
          | Some i -> Printf.sprintf "took %d" i)
      | Close ->
        Lwt_multicore.Stream.close sut;
        "closed"
      | Length -> Printf.sprintf "length %d" (Lwt_multicore.Stream.length sut)
    in
    STM.Res (STM.string, answer)

  (* What the model says the channel should have answered. *)
  let expected cmd (state : state) =
    match cmd with
    | Push _ ->
      if state.closed then "closed"
      else if List.length state.items >= capacity then "blocked"
      else "pushed"
    | Take -> (
      match state.items with
      (* Order matters: a channel is FIFO, and the give-back of a cancelled
         consumer must not disturb it. *)
      | i :: _ -> Printf.sprintf "took %d" i
      | [] -> if state.closed then "end" else "blocked")
    | Close -> "closed"
    | Length -> Printf.sprintf "length %d" (List.length state.items)

  let postcond cmd state res =
    match res with
    | STM.Res ((STM.String, _), answer) -> answer = expected cmd state
    | _ -> false
end

module Sequential = STM_sequential.Make (Spec)

let () =
  QCheck_base_runner.run_tests_main
    [ Sequential.agree_test ~count:2000
        ~name:"Lwt_multicore.Stream against a model" ]
