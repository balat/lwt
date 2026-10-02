(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* Differential test of the promise core.

   The core (src/core/lwt.ml) was rewritten behind an unchanged interface. The
   historical test suite passes on both, but it only checks what someone
   thought to check. This test generates random programs out of the core's
   public operations, runs each on the current core and on a vendored copy of
   the historical core (lwt_historical.ml), and compares everything
   observable: the order in which callbacks run, what they receive, what the
   resolution functions raise, and the final state of every promise.

   Programs use the pure core only, with no event loop: suspensions are
   pauses, served by wakeup_paused, the way js_of_ocaml drives Lwt. A callback
   can itself perform steps (wake another resolver, cancel, attach callbacks),
   which is where two cores are most likely to differ: code running inside a
   resolution cascade.

   A divergence is a counterexample that QCheck shrinks to a minimal program.
   It is either a bug in the current core, or a change of observable behaviour
   that must be decided and documented; this test makes sure every such change
   is known. *)

exception Boom of int

(* The subset of the core's interface the programs use. Both cores satisfy it
   as they are. *)
module type CORE = sig
  type 'a t
  type 'a u
  type 'a state = Return of 'a | Fail of exn | Sleep

  exception Canceled

  val return : 'a -> 'a t
  val fail : exn -> 'a t
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val map : ('a -> 'b) -> 'a t -> 'b t
  val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t
  val try_bind : (unit -> 'a t) -> ('a -> 'b t) -> (exn -> 'b t) -> 'b t
  val finalize : (unit -> 'a t) -> (unit -> unit t) -> 'a t
  val wait : unit -> 'a t * 'a u
  val task : unit -> 'a t * 'a u
  val wakeup : 'a u -> 'a -> unit
  val wakeup_exn : 'a u -> exn -> unit
  val wakeup_later : 'a u -> 'a -> unit
  val wakeup_later_exn : 'a u -> exn -> unit
  val cancel : 'a t -> unit
  val on_cancel : 'a t -> (unit -> unit) -> unit
  val protected : 'a t -> 'a t
  val no_cancel : 'a t -> 'a t
  val choose : 'a t list -> 'a t
  val pick : 'a t list -> 'a t
  val nchoose : 'a t list -> 'a list t
  val npick : 'a t list -> 'a list t
  val join : unit t list -> unit t
  val both : 'a t -> 'b t -> ('a * 'b) t
  val on_success : 'a t -> ('a -> unit) -> unit
  val on_failure : 'a t -> (exn -> unit) -> unit
  val on_termination : 'a t -> (unit -> unit) -> unit
  val on_any : 'a t -> ('a -> unit) -> (exn -> unit) -> unit
  val pause : unit -> unit t
  val wakeup_paused : unit -> unit
  val abandon_paused : unit -> unit
  val paused_count : unit -> int
  val state : 'a t -> 'a state
  val async_exception_hook : (exn -> unit) ref
end

(* {1 Programs} *)

(* A step refers to promises and resolvers by index into the list of those
   created so far. The interpreter reduces an index modulo the current count,
   so every generated program is valid; a step that needs a promise when none
   exists yet does nothing. *)
type step =
  | Wait
  | Task
  | Return of int
  | Fail of int
  | Bind of int * callback
  | Map of int
  | Catch of int * callback
  | Try_bind of int * callback * callback
  | Finalize of int * callback
  | Choose of int list
  | Pick of int list
  | Nchoose of int list
  | Npick of int list
  | Join of int list
  | Both of int * int
  | Protected of int
  | No_cancel of int
  | On_success of int * callback
  | On_failure of int * callback
  | On_termination of int * callback
  | On_any of int * callback * callback
  | On_cancel of int * callback
  | Wakeup of int * outcome
  | Wakeup_later of int * outcome
  | Cancel of int
  | Pause
  | Wakeup_paused

and outcome = Value of int | Exception of int

(* What a callback does when it runs, then what it produces. The production
   matters for bind, catch and try_bind, whose callback returns a promise. *)
and callback = { body : step list; produce : produce }

and produce =
  | Produce_value
  | Produce_promise of int
  | Produce_raise of int

(* {1 Interpretation, with a trace} *)

module Run (C : CORE) = struct
  let trace = Buffer.create 4096
  let log fmt = Printf.kbprintf (fun b -> Buffer.add_char b '\n') trace fmt

  let promises : int C.t list ref = ref []
  let resolvers : int C.u list ref = ref []
  let next_callback = ref 0

  let string_of_exn = function
    | Boom n -> Printf.sprintf "Boom %d" n
    | C.Canceled -> "Canceled"
    | Invalid_argument s -> Printf.sprintf "Invalid_argument %S" s
    | e -> Printexc.to_string e

  (* Lists grow at the head; index 0 is the oldest promise, so that a shrunk
     program keeps referring to the same ones. *)
  let nth l i =
    let n = List.length l in
    if n = 0 then None else Some (List.nth l (n - 1 - (i mod n)))

  let add_promise p = promises := p :: !promises
  let add_resolver r = resolvers := r :: !resolvers

  let promise i = nth !promises i
  let resolver i = nth !resolvers i

  let promise_list is =
    List.filter_map promise is

  let rec run_step = function
    | Wait ->
      let p, r = C.wait () in
      add_promise p;
      add_resolver r
    | Task ->
      let p, r = C.task () in
      add_promise p;
      add_resolver r
    | Return v -> add_promise (C.return v)
    | Fail e -> add_promise (C.fail (Boom e))
    | Bind (i, cb) ->
      with_promise i (fun p ->
        let k = fresh cb in
        add_promise (C.bind p (fun v -> value_callback k cb v)))
    | Map i ->
      with_promise i (fun p ->
        let k = !next_callback in
        incr next_callback;
        add_promise (C.map (fun v -> log "cb%d(%d)" k v; v + 1) p))
    | Catch (i, cb) ->
      with_promise i (fun p ->
        let k = fresh cb in
        add_promise (C.catch (fun () -> p) (fun e -> exn_callback k cb e)))
    | Try_bind (i, cb_ok, cb_exn) ->
      with_promise i (fun p ->
        let k1 = fresh cb_ok in
        let k2 = fresh cb_exn in
        add_promise
          (C.try_bind
             (fun () -> p)
             (fun v -> value_callback k1 cb_ok v)
             (fun e -> exn_callback k2 cb_exn e)))
    | Finalize (i, cb) ->
      with_promise i (fun p ->
        let k = fresh cb in
        add_promise
          (C.finalize
             (fun () -> p)
             (fun () -> C.map ignore (unit_callback k cb))))
    | Choose is -> combine_one_resolved is C.choose
    | Pick is -> combine_pick is
    | Nchoose is -> combine is (fun ps -> C.map List.length (C.nchoose ps))
    | Npick is -> combine is (fun ps -> C.map List.length (C.npick ps))
    | Join is ->
      combine is (fun ps ->
        C.map (fun () -> 0) (C.join (List.map (C.map ignore) ps)))
    | Both (i, j) ->
      (match promise i, promise j with
       | Some p, Some q -> add_promise (C.map (fun (a, b) -> a + b) (C.both p q))
       | _ -> ())
    | Protected i -> with_promise i (fun p -> add_promise (C.protected p))
    | No_cancel i -> with_promise i (fun p -> add_promise (C.no_cancel p))
    | On_success (i, cb) ->
      with_promise i (fun p ->
        let k = fresh cb in
        C.on_success p (fun v -> ignore (value_callback k cb v)))
    | On_failure (i, cb) ->
      with_promise i (fun p ->
        let k = fresh cb in
        C.on_failure p (fun e -> ignore (exn_callback k cb e)))
    | On_termination (i, cb) ->
      with_promise i (fun p ->
        let k = fresh cb in
        C.on_termination p (fun () -> ignore (unit_callback k cb)))
    | On_any (i, cb_ok, cb_exn) ->
      with_promise i (fun p ->
        let k1 = fresh cb_ok in
        let k2 = fresh cb_exn in
        C.on_any p
          (fun v -> ignore (value_callback k1 cb_ok v))
          (fun e -> ignore (exn_callback k2 cb_exn e)))
    | On_cancel (i, cb) ->
      with_promise i (fun p ->
        let k = fresh cb in
        C.on_cancel p (fun () -> ignore (unit_callback k cb)))
    | Wakeup (i, o) ->
      with_resolver i (fun r ->
        resolve "wakeup" (fun () ->
          match o with
          | Value v -> C.wakeup r v
          | Exception e -> C.wakeup_exn r (Boom e)))
    | Wakeup_later (i, o) ->
      with_resolver i (fun r ->
        resolve "wakeup_later" (fun () ->
          match o with
          | Value v -> C.wakeup_later r v
          | Exception e -> C.wakeup_later_exn r (Boom e)))
    | Cancel i -> with_promise i C.cancel
    | Pause -> add_promise (C.map (fun () -> 0) (C.pause ()))
    | Wakeup_paused ->
      log "wakeup_paused (%d)" (C.paused_count ());
      C.wakeup_paused ()

  (* A step that raises is an observable event, not the end of the program. *)
  and exec s =
    match run_step s with
    | () -> ()
    | exception e -> log "step raised %s" (string_of_exn e)

  and with_promise i f = match promise i with Some p -> f p | None -> ()
  and with_resolver i f = match resolver i with Some r -> f r | None -> ()

  and combine is f =
    match promise_list is with
    | [] -> ()
    | ps -> add_promise (f ps)

  (* choose and pick select AT RANDOM among the promises already resolved when
     they are called, by contract, so the two cores may legitimately differ
     there. Keep at most one resolved promise in their list: the choice is
     then forced, and everything else about them is still compared. *)
  (* pick cancels the losers; when one promise is already resolved, the
     historical core cancels first and then selects at random among the
     resolved, cancelled losers included, while the current core returns the
     one that was resolved at the call. Both are within the contract's "at
     random", so a pick with a resolved promise is reduced to that promise. *)
  and combine_pick is =
    let ps = promise_list is in
    match List.filter (fun p -> C.state p <> Sleep) ps with
    | [] -> combine is C.pick
    | p :: _ -> add_promise (C.pick [ p ])

  and combine_one_resolved is f =
    let resolved = ref false in
    let ps =
      List.filter
        (fun p ->
           match C.state p with
           | Sleep -> true
           | Return _ | Fail _ ->
             if !resolved then false else (resolved := true; true))
        (promise_list is)
    in
    match ps with
    | [] -> ()
    | ps -> add_promise (f ps)

  (* The resolution functions raise on a promise that is already resolved;
     what they raise, and when, is part of the contract. *)
  and resolve name f =
    match f () with
    | () -> log "%s ok" name
    | exception e -> log "%s raised %s" name (string_of_exn e)

  (* A callback may produce only a promise that existed when it was
     registered. The promise its registration creates comes after, so no
     callback can produce the promise that depends on it, nor anything newer
     that may depend on that: no cycle of forwards, which no real program
     writes without a reference and which makes the historical core recurse
     for ever. *)
  and fresh _cb =
    let k = !next_callback in
    incr next_callback;
    (k, List.length !promises)

  (* Every callback logs its entry with what it received, runs its body, and
     produces what the program says. *)
  and value_callback (k, limit) cb v =
    log "cb%d(%d)" k v;
    List.iter exec cb.body;
    produce limit cb.produce (v + 1)

  and exn_callback (k, limit) cb e =
    log "cb%d(%s)" k (string_of_exn e);
    List.iter exec cb.body;
    produce limit cb.produce 0

  and unit_callback (k, limit) cb =
    log "cb%d()" k;
    List.iter exec cb.body;
    produce limit cb.produce 0

  and produce limit p v =
    match p with
    | Produce_value -> C.return v
    | Produce_promise i ->
      let older =
        let all = !promises in
        let n = List.length all in
        (* The list grows at the head: drop what came after [limit]. *)
        let rec drop l k = if k = 0 then l else match l with [] -> [] | _ :: l -> drop l (k - 1) in
        drop all (n - limit)
      in
      (match nth older i with Some p -> p | None -> C.return v)
    | Produce_raise e -> raise (Boom e)

  let run program =
    (* The core is global: a program that paused and never served its pauses
       would leave them for the next one, which must start from nothing. *)
    C.abandon_paused ();
    Buffer.clear trace;
    promises := [];
    resolvers := [];
    next_callback := 0;
    C.async_exception_hook := (fun e -> log "async_exception_hook %s" (string_of_exn e));
    List.iter exec program;
    List.iteri
      (fun i p ->
         match C.state p with
         | Return v -> log "p%d = Return %d" i v
         | Fail e -> log "p%d = Fail %s" i (string_of_exn e)
         | Sleep -> log "p%d = Sleep" i)
      (List.rev !promises);
    log "paused: %d" (C.paused_count ());
    Buffer.contents trace
end

module Current = Run (Lwt)
module Historical = Run (Lwt_historical)

(* {1 Generation} *)

open QCheck2

let gen_outcome =
  Gen.oneof
    [ Gen.map (fun v -> Value v) (Gen.int_bound 9);
      Gen.map (fun e -> Exception e) (Gen.int_bound 3) ]

let gen_index = Gen.int_bound 7
let gen_indexes = Gen.list_size (Gen.int_range 1 3) gen_index

(* The two observable differences between the cores that are known, and
   pending a decision rather than a bug (see the module comment of
   test/differential and the session that found them):

   - cancellation: the historical core marks every promise a cancel reaches
     as cancelled first, then runs their callbacks in reverse order of
     discovery; the current core cancels and runs each in list order, so a
     callback can see a sibling still pending;
   - merging: when a bind's callback returns a pending promise, the
     historical core runs the bind result's callbacks before that promise's,
     the current core after.

   The default run leaves out what produces them (cancel, pick and npick,
   which cancel their losers, and callbacks that return an existing promise),
   so that it checks everything else and stays green. DIFFERENTIAL_ALL=1
   puts them back, to see the differences or to check a change that aligns
   the cores. DIFFERENTIAL_COUNT sets the number of programs. *)
let all_classes = Sys.getenv_opt "DIFFERENTIAL_ALL" <> None
let known w = if all_classes then w else 0

let count =
  match Option.bind (Sys.getenv_opt "DIFFERENTIAL_COUNT") int_of_string_opt with
  | Some n -> n
  | None -> 5_000

let gen_produce =
  Gen.oneof_weighted
    [ 3, Gen.return Produce_value;
      known 2, Gen.map (fun i -> Produce_promise i) gen_index;
      1, Gen.map (fun e -> Produce_raise e) (Gen.int_bound 3) ]

(* Callbacks nest up to [depth]; the leaves have an empty body. *)
let rec gen_step depth =
  let callback = gen_callback depth in
  let one f = Gen.map f gen_index in
  let with_cb f = Gen.map2 f gen_index callback in
  Gen.oneof_weighted
    [ 4, Gen.return Wait;
      3, Gen.return Task;
      2, one (fun v -> Return v);
      1, one (fun e -> Fail e);
      5, with_cb (fun i cb -> Bind (i, cb));
      1, one (fun i -> Map i);
      2, with_cb (fun i cb -> Catch (i, cb));
      1, Gen.map2 (fun (i, cb1) cb2 -> Try_bind (i, cb1, cb2))
           (Gen.pair gen_index callback) callback;
      2, with_cb (fun i cb -> Finalize (i, cb));
      1, Gen.map (fun is -> Choose is) gen_indexes;
      known 2, Gen.map (fun is -> Pick is) gen_indexes;
      1, Gen.map (fun is -> Nchoose is) gen_indexes;
      known 1, Gen.map (fun is -> Npick is) gen_indexes;
      2, Gen.map (fun is -> Join is) gen_indexes;
      1, Gen.map2 (fun i j -> Both (i, j)) gen_index gen_index;
      1, one (fun i -> Protected i);
      1, one (fun i -> No_cancel i);
      2, with_cb (fun i cb -> On_success (i, cb));
      1, with_cb (fun i cb -> On_failure (i, cb));
      1, with_cb (fun i cb -> On_termination (i, cb));
      1, Gen.map2 (fun (i, cb1) cb2 -> On_any (i, cb1, cb2))
           (Gen.pair gen_index callback) callback;
      2, with_cb (fun i cb -> On_cancel (i, cb));
      5, Gen.map2 (fun i o -> Wakeup (i, o)) gen_index gen_outcome;
      3, Gen.map2 (fun i o -> Wakeup_later (i, o)) gen_index gen_outcome;
      known 3, one (fun i -> Cancel i);
      2, Gen.return Pause;
      2, Gen.return Wakeup_paused ]

and gen_callback depth =
  if depth = 0 then
    Gen.map (fun produce -> { body = []; produce }) gen_produce
  else
    Gen.map2
      (fun body produce -> { body; produce })
      (Gen.list_size (Gen.int_bound 3) (gen_step (depth - 1)))
      gen_produce

let gen_program = Gen.list_size (Gen.int_range 1 25) (gen_step 2)

(* {1 Printing counterexamples} *)

let rec string_of_step = function
  | Wait -> "Wait"
  | Task -> "Task"
  | Return v -> Printf.sprintf "Return %d" v
  | Fail e -> Printf.sprintf "Fail %d" e
  | Bind (i, cb) -> Printf.sprintf "Bind (%d, %s)" i (string_of_callback cb)
  | Map i -> Printf.sprintf "Map %d" i
  | Catch (i, cb) -> Printf.sprintf "Catch (%d, %s)" i (string_of_callback cb)
  | Try_bind (i, a, b) ->
    Printf.sprintf "Try_bind (%d, %s, %s)" i (string_of_callback a)
      (string_of_callback b)
  | Finalize (i, cb) ->
    Printf.sprintf "Finalize (%d, %s)" i (string_of_callback cb)
  | Choose is -> Printf.sprintf "Choose %s" (string_of_indexes is)
  | Pick is -> Printf.sprintf "Pick %s" (string_of_indexes is)
  | Nchoose is -> Printf.sprintf "Nchoose %s" (string_of_indexes is)
  | Npick is -> Printf.sprintf "Npick %s" (string_of_indexes is)
  | Join is -> Printf.sprintf "Join %s" (string_of_indexes is)
  | Both (i, j) -> Printf.sprintf "Both (%d, %d)" i j
  | Protected i -> Printf.sprintf "Protected %d" i
  | No_cancel i -> Printf.sprintf "No_cancel %d" i
  | On_success (i, cb) ->
    Printf.sprintf "On_success (%d, %s)" i (string_of_callback cb)
  | On_failure (i, cb) ->
    Printf.sprintf "On_failure (%d, %s)" i (string_of_callback cb)
  | On_termination (i, cb) ->
    Printf.sprintf "On_termination (%d, %s)" i (string_of_callback cb)
  | On_any (i, a, b) ->
    Printf.sprintf "On_any (%d, %s, %s)" i (string_of_callback a)
      (string_of_callback b)
  | On_cancel (i, cb) ->
    Printf.sprintf "On_cancel (%d, %s)" i (string_of_callback cb)
  | Wakeup (i, o) -> Printf.sprintf "Wakeup (%d, %s)" i (string_of_outcome o)
  | Wakeup_later (i, o) ->
    Printf.sprintf "Wakeup_later (%d, %s)" i (string_of_outcome o)
  | Cancel i -> Printf.sprintf "Cancel %d" i
  | Pause -> "Pause"
  | Wakeup_paused -> "Wakeup_paused"

and string_of_outcome = function
  | Value v -> Printf.sprintf "Value %d" v
  | Exception e -> Printf.sprintf "Exception %d" e

and string_of_indexes is =
  "[" ^ String.concat "; " (List.map string_of_int is) ^ "]"

and string_of_callback { body; produce } =
  let produce =
    match produce with
    | Produce_value -> "Produce_value"
    | Produce_promise i -> Printf.sprintf "Produce_promise %d" i
    | Produce_raise e -> Printf.sprintf "Produce_raise %d" e
  in
  match body with
  | [] -> Printf.sprintf "{%s}" produce
  | _ ->
    Printf.sprintf "{[%s] %s}"
      (String.concat "; " (List.map string_of_step body))
      produce

let print_program program =
  "\n  " ^ String.concat ";\n  " (List.map string_of_step program)

(* {1 The test} *)

exception Timeout of string

(* A program that makes a core loop is a finding too, and QCheck can only
   shrink it if the property returns. *)
let with_watchdog core f =
  let previous =
    Sys.signal Sys.sigalrm
      (Sys.Signal_handle (fun _ -> raise (Timeout core)))
  in
  ignore (Unix.alarm 2);
  Fun.protect
    ~finally:(fun () ->
      ignore (Unix.alarm 0);
      Sys.set_signal Sys.sigalrm previous)
    f

(* With DIFFERENTIAL_LOG set to a path, every program is written there before
   it runs, each core named before its turn: a crash that no exception can
   report (a stack overflow in C, a segmentation fault) then leaves the
   culprit as the last entry. *)
let log_channel =
  match Sys.getenv_opt "DIFFERENTIAL_LOG" with
  | Some path -> Some (open_out path)
  | None -> None

let log_step s =
  match log_channel with
  | Some oc -> output_string oc s; flush oc
  | None -> ()

exception Crashed of string
exception Historical_crashed

(* Runs [f] in a forked child first, and reports a child killed by a signal,
   a segmentation fault for instance, which no exception could report. The
   program is then run again here for its trace. Not on Windows. *)
let survives f =
  if Sys.win32 then true
  else
    match Unix.fork () with
    | 0 ->
      (match f () with
       | (_ : string) -> Unix._exit 0
       | exception _ -> Unix._exit 1)
    | pid ->
      (match Unix.waitpid [] pid with
       | _, Unix.WSIGNALED _ -> false
       | _, (Unix.WEXITED _ | Unix.WSTOPPED _) -> true)

(* The historical core has a memory-safety bug: cancelling a choose over
   [protected p; p] where p is a pending promise with callbacks crashes it
   (found by this test, reproduced on upstream in nine lines, see the session
   report). So it always runs in a child first, and a program that crashes it
   is counted and skipped rather than compared: the test is about the current
   core. With DIFFERENTIAL_ISOLATE set, the current core gets the same
   treatment, and a crash of it is a failure QCheck shrinks. *)
let isolate = Sys.getenv_opt "DIFFERENTIAL_ISOLATE" <> None
let historical_crashes = ref 0

let run_both p =
  log_step ("program:" ^ print_program p ^ "\nhistorical\n");
  if not (survives (fun () -> Historical.run p)) then begin
    incr historical_crashes;
    raise Historical_crashed
  end;
  let historical = with_watchdog "historical" (fun () -> Historical.run p) in
  log_step "current\n";
  if isolate && not (survives (fun () -> Current.run p)) then
    raise (Crashed "current core: killed by a signal");
  let current = with_watchdog "current" (fun () -> Current.run p) in
  log_step "done\n";
  historical, current

(* Side by side, so that a counterexample shows where the traces part. *)
let show_divergence program =
  let a = String.split_on_char '\n' (Historical.run program) in
  let b = String.split_on_char '\n' (Current.run program) in
  let rec go i a b =
    match a, b with
    | [], [] -> ()
    | x :: a, y :: b when x = y -> go (i + 1) a b
    | _ ->
      Printf.printf "  first divergence at event %d:\n" i;
      Printf.printf "    historical: %s\n"
        (match a with x :: _ -> x | [] -> "<end>");
      Printf.printf "    current:    %s\n"
        (match b with y :: _ -> y | [] -> "<end>")
  in
  go 0 a b

let () =
  let program = ref None in
  let same_trace =
    Test.make ~name:"random programs run the same on both cores" ~count
      ~print:(fun p ->
        program := Some p;
        print_program p)
      gen_program
      (fun p ->
        match run_both p with
        | historical, current -> historical = current
        | exception Historical_crashed -> true)
  in
  let status = QCheck_base_runner.run_tests ~verbose:true [ same_trace ] in
  if !historical_crashes > 0 then
    Printf.printf "  (%d programs crashed the historical core and were skipped)\n"
      !historical_crashes;
  (match !program with
   | Some p ->
     print_endline "  traces:";
     show_divergence p
   | None -> ());
  exit status
