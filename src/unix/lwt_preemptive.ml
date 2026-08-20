(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* [Lwt_sequence] is deprecated – we don't want users outside Lwt using it.
   However, it is still used internally by Lwt. So, briefly disable warning 3
   ("deprecated"), and create a local, non-deprecated alias for
   [Lwt_sequence] that can be referred to by the rest of the code in this
   module without triggering any more warnings. *)
module Lwt_sequence = Lwt_sequence

open Lwt.Infix

(* ONE POOL PER LOOP, and that is not a preference: a shared pool of system
   threads cannot survive domains that come and go.

   The fact that decides it, measured rather than assumed: A DOMAIN DOES NOT
   TERMINATE WHILE ANY OF ITS THREADS IS STILL RUNNING. Spawn a domain, let it
   create a thread that loops, return from its body, and [Domain.join] blocks for
   ever. Worker threads are created by whichever domain first needs one, so a
   shared pool would hand a spawned domain a thread it created and thereby pin
   that domain alive after its work is done. Nothing about notifications or
   promises enters into it; it is the threads themselves.

   So each loop keeps its own free list, its own count and its own queue of
   clients waiting for a worker. Everything here is touched by one domain only,
   which is also why none of it needs a lock: the shared-pool version had to
   protect the free list with a mutex and hand waiting clients their worker
   through a notification, because their promises belonged to different domains.
   Per loop, the promises are local again and the code is the one Lwt always had.

   The bounds stay PROCESS-WIDE settings, applied per loop: [set_bounds (0, 4)]
   means four workers per loop, not four in the process. Documented, because a
   program with several loops does get more threads than it used to.

   A loop's workers are terminated when its domain exits, which is what makes the
   domain able to exit at all.

   [run_in_main] and [run_in_main_dont_wait] keep their meaning: the function runs
   on the domain that initialised this module, not on the domain that detached the
   work. With N loops "the main thread" is ambiguous, and changing it silently
   would be worse than leaving it documented. *)
[@@@alert "-lwt_internal"]

(* +-----------------------------------------------------------------+
   | Parameters                                                      |
   +-----------------------------------------------------------------+ *)

(* Settings, shared by every loop and applied per loop, so read and written from
   any domain: atomics rather than refs. *)

(* Minimum number of preemptive threads, per loop: *)
let min_threads = Atomic.make 0

(* Maximum number of preemptive threads, per loop: *)
let max_threads = Atomic.make 0

(* Size of the waiting queue: *)
let max_thread_queued = Atomic.make 1000

let get_max_number_of_threads_queued _ = Atomic.get max_thread_queued

let set_max_number_of_threads_queued n =
  if n < 0 then invalid_arg "Lwt_preemptive.set_max_number_of_threads_queued";
  Atomic.set max_thread_queued n

(* +-----------------------------------------------------------------+
   | Preemptive threads management                                   |
   +-----------------------------------------------------------------+ *)

module CELL :
sig
  type 'a t

  val make : unit -> 'a t
  val get : 'a t -> 'a
  val set : 'a t -> 'a -> unit
end =
struct
  type 'a t = {
    m  : Mutex.t;
    cv : Condition.t;
    mutable cell : 'a option;
  }

  let make () = { m = Mutex.create (); cv = Condition.create (); cell = None }

  let get t =
    let rec await_value t =
      match t.cell with
      | None ->
        Condition.wait t.cv t.m;
        await_value t
      | Some v ->
        t.cell <- None;
        Mutex.unlock t.m;
        v
    in
    Mutex.lock t.m;
    await_value t

  let set t v =
    Mutex.lock t.m;
    t.cell <- Some v;
    Mutex.unlock t.m;
    Condition.signal t.cv
end

type thread = {
  task_cell: (Lwt_unix.notification * (unit -> unit)) CELL.t;
  (* Channel used to communicate notification id and tasks to the
     worker thread. *)

  mutable thread : Thread.t;
  (* The worker thread. *)

  reuse : bool Atomic.t;
  (* Whether the thread must be re-added to the pool when the work is done.
     Atomic because the worker writes it and its loop reads it. *)
}

(* PER LOOP. Only its own domain touches any of this, so none of it is locked;
   see the note at the top for why a shared pool is not an option. *)
type pool = {
  (* Free workers. *)
  free : thread Queue.t;
  (* Clients waiting for one, as promises of this domain. *)
  waiters : thread Lwt.u Lwt_sequence.t;
  (* How many workers this loop has created. *)
  mutable count : int;
  (* Every worker created, so that they can be joined at domain exit. *)
  mutable all : thread list;
  (* Set at domain exit: tells a worker to stop looping. Read by the workers,
     hence atomic. *)
  stopping : bool Atomic.t;
  (* A notification a dying worker can send harmlessly: this domain will not be
     draining any more, so it is dropped. *)
  quit : Lwt_unix.notification;
}

let shutdown_pool : (pool -> unit) ref = ref (fun _ -> ())

let pool_slot : pool Lwt_dls.t =
  Lwt_dls.new_key (fun () ->
    let pool =
      { free = Queue.create ();
        waiters = Lwt_sequence.create ();
        count = 0;
        all = [];
        stopping = Atomic.make false;
        quit = Lwt_unix.make_notification (fun () -> ()) }
    in
    Lwt_dls.at_domain_exit (fun () -> !shutdown_pool pool);
    pool)

let[@inline] self_pool () = Lwt_dls.get pool_slot

(* Code executed by a worker: *)
let rec worker_loop pool worker =
  let id, task = CELL.get worker.task_cell in
  task ();
  (* Tell the loop that submitted this task that the work is done. The id names
     that loop's notification channel. *)
  Lwt_unix.send_notification id;
  if Atomic.get worker.reuse && not (Atomic.get pool.stopping) then
    worker_loop pool worker

(* create a new worker: *)
let make_worker pool =
  pool.count <- pool.count + 1;
  let worker = {
    task_cell = CELL.make ();
    thread = Thread.self ();
    reuse = Atomic.make true;
  } in
  worker.thread <- Thread.create (worker_loop pool) worker;
  pool.all <- worker :: pool.all;
  worker

(* Add a worker to the pool: *)
let add_worker pool worker =
  match Lwt_sequence.take_opt_l pool.waiters with
  | None ->
    Queue.add worker pool.free
  | Some w ->
    Lwt.wakeup w worker

(* Wait for worker to be available, then return it: *)
let get_worker pool =
  if not (Queue.is_empty pool.free) then
    Lwt.return (Queue.take pool.free)
  else if pool.count < Atomic.get max_threads then
    Lwt.return (make_worker pool)
  else
    (Lwt.add_task_r [@ocaml.warning "-3"]) pool.waiters

(* Ends this loop's workers, and waits for them. Without this the domain could
   not terminate at all: a running thread pins its domain, so a loop that ever
   detached anything would hang [Domain.join] for ever.

   A free worker is woken with a task that does nothing, so that it notices
   [stopping] and leaves its loop. A BUSY worker is left to finish what it is
   doing and notices on its own; joining it is what makes the wait correct rather
   than a race. *)
(* Ordering, and why this needs none. A worker created AFTER this has run, for
   instance by an exit hook that detaches, is not a leak and does not hang the
   domain: [stopping] is already set, so the worker leaves its loop after its one
   task, and [detach]'s own finaliser joins it. So this may run before or after
   [Lwt_main]'s exit drain, and does not have to be sequenced against it. *)
let shutdown pool =
  Atomic.set pool.stopping true;
  Queue.iter
    (fun worker -> CELL.set worker.task_cell (pool.quit, fun () -> ()))
    pool.free;
  Queue.clear pool.free;
  List.iter (fun worker -> Thread.join worker.thread) pool.all;
  pool.all <- [];
  pool.count <- 0

let () = shutdown_pool := shutdown

(* +-----------------------------------------------------------------+
   | Initialisation, and dynamic parameters reset                    |
   +-----------------------------------------------------------------+ *)

let get_bounds () = (Atomic.get min_threads, Atomic.get max_threads)

(* The bounds are process-wide; the workers they launch are this loop's. *)
let set_bounds (min, max) =
  if min < 0 || max < min then invalid_arg "Lwt_preemptive.set_bounds";
  let pool = self_pool () in
  let diff = min - pool.count in
  Atomic.set min_threads min;
  Atomic.set max_threads max;
  (* Launch new workers: *)
  for _i = 1 to diff do
    add_worker pool (make_worker pool)
  done

(* Whether the bounds have been set. Per loop, since what [simple_init] has to
   arrange is this loop's minimum: a second domain arriving later must still get
   its own workers. The bounds themselves stay shared. *)
let initialized : bool ref Lwt_dls.t = Lwt_dls.new_key (fun () -> ref false)

let init min max _errlog =
  (Lwt_dls.get initialized) := true;
  set_bounds (min, max)

let simple_init () =
  let flag = Lwt_dls.get initialized in
  if not !flag then begin
    flag := true;
    (* Only take the default bounds if nobody has set any. *)
    if Atomic.get max_threads = 0 then set_bounds (0, 4)
    else set_bounds (Atomic.get min_threads, Atomic.get max_threads)
  end

(* All three report on the CALLING loop's pool. *)
let nbthreads () = (self_pool ()).count
let nbthreadsqueued () =
  Lwt_sequence.fold_l (fun _ x -> x + 1) (self_pool ()).waiters 0
let nbthreadsbusy () =
  let pool = self_pool () in
  pool.count - Queue.length pool.free

(* +-----------------------------------------------------------------+
   | Detaching                                                       |
   +-----------------------------------------------------------------+ *)

let init_result = Result.Error (Failure "Lwt_preemptive.detach")

let detach f args =
  simple_init ();
  let pool = self_pool () in
  let result = ref init_result in
  (* The task for the worker thread: *)
  let task () =
    try
      result := Result.Ok (f args)
    with exn when Lwt.Exception_filter.run exn ->
      result := Result.Error exn
  in
  get_worker pool >>= fun worker ->
  let waiter, wakener = Lwt.wait () in
  let id =
    Lwt_unix.make_notification ~once:true
      (fun () -> Lwt.wakeup_result wakener !result)
  in
  Lwt.finalize
    (fun () ->
       (* Send the id and the task to the worker: *)
       CELL.set worker.task_cell (id, task);
       waiter)
    (fun () ->
       if Atomic.get worker.reuse && not (Atomic.get pool.stopping) then
         (* Put back the worker to the pool: *)
         add_worker pool worker
       else begin
         pool.count <- pool.count - 1;
         (* Or wait for the thread to terminates, to free its associated
            resources: *)
         Thread.join worker.thread
       end;
       Lwt.return_unit)

(* +-----------------------------------------------------------------+
   | Running Lwt threads in the main thread                          |
   +-----------------------------------------------------------------+ *)

(* Queue of [unit -> unit Lwt.t] functions. *)
let jobs = Queue.create ()

(* Mutex to protect access to [jobs]. *)
let jobs_mutex = Mutex.create ()

let job_notification =
  Lwt_unix.make_notification
    (fun () ->
       (* Take the first job. The queue is never empty at this
          point. *)
       Mutex.lock jobs_mutex;
       let thunk = Queue.take jobs in
       Mutex.unlock jobs_mutex;
       ignore (thunk ()))

let run_in_main_dont_wait f =
  (* Add the job to the queue. *)
  Mutex.lock jobs_mutex;
  Queue.add f jobs;
  Mutex.unlock jobs_mutex;
  (* Notify the main thread. *)
  Lwt_unix.send_notification job_notification

(* There is a potential performance issue from creating a cell every time this
   function is called. See:
   https://github.com/ocsigen/lwt/issues/218
   https://github.com/ocsigen/lwt/pull/219
   https://github.com/ocaml/ocaml/issues/7158 *)
let run_in_main f =
  let cell = CELL.make () in
  (* Create the job. *)
  let job () =
    (* Execute [f] and wait for its result. *)
    Lwt.try_bind f
      (fun ret -> Lwt.return (Result.Ok ret))
      (fun exn -> Lwt.return (Result.Error exn)) >>= fun result ->
    (* Send the result. *)
    CELL.set cell result;
    Lwt.return_unit
  in
  run_in_main_dont_wait job;
  (* Wait for the result. *)
  match CELL.get cell with
  | Result.Ok ret -> ret
  | Result.Error exn -> raise exn

(* This version shadows the one above, adding an exception handler *)
let run_in_main_dont_wait f handler =
  let f () = Lwt.catch f (fun exc -> handler exc; Lwt.return_unit) in
  run_in_main_dont_wait f
