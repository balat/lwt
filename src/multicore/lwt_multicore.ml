(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* This module is part of the Lwt packages, so it may use the per-domain layer.
   It is also a legitimate user of [Lwt.Private]: adopting a foreign promise means
   asking its owner to attach the callback, which requires knowing who the owner
   is, and the core is the only thing that knows. *)
[@@@alert "-lwt_internal"]
[@@@alert "-trespassing"]

(* MULTI-PRODUCER, SINGLE-CONSUMER, which is exactly the shape: any domain posts,
   the owning loop drains. Saturn's queue is lock-free and verified upstream, and
   it brings [close], which is what makes "posting to a terminated loop is
   refused" a property of the data structure rather than a flag of ours. *)
module Inbox = Saturn.Single_consumer_queue

type loop = {
  (* Which domain this loop belongs to, so that a promise owned by that domain
     can be routed to it. *)
  dom : int;
  inbox : (unit -> unit) Inbox.t;
  (* Wakes this loop. A notification id names the channel of the domain that
     created it, so sending it from anywhere wakes the right loop; that machinery
     is [Lwt_unix]'s and predates this module. *)
  notification : Lwt_unix.notification;
}

exception Loop_terminated

(* Runs on the owning loop, from its notification handler, so the thunks run on
   the right domain with nothing held. [pop_opt] raises [Closed] once the queue is
   both closed and empty, which happens only after the domain has gone; there is
   no handler to run then, but the guard keeps the shape obvious. *)
let drain inbox =
  let rec go () =
    match Inbox.pop_opt inbox with
    | Some f -> f (); go ()
    | None -> ()
    | exception Inbox.Closed -> ()
  in
  go ()

(* EVERY LOOP THAT HAS A HANDLE, by domain. Shared, hence the mutex, and consulted
   only by [adopt], which is the one thing that has to find a loop it was not
   given. A loop registers itself when its handle is created and deregisters when
   its domain exits. *)
let registry : (int, loop) Hashtbl.t = Hashtbl.create 8
let registry_mutex = Mutex.create ()

let register l =
  Mutex.lock registry_mutex;
  Hashtbl.replace registry l.dom l;
  Mutex.unlock registry_mutex

let deregister dom =
  Mutex.lock registry_mutex;
  Hashtbl.remove registry dom;
  Mutex.unlock registry_mutex

let registered dom =
  Mutex.lock registry_mutex;
  let l = Hashtbl.find_opt registry dom in
  Mutex.unlock registry_mutex;
  l

let self_slot : loop Lwt_dls.t =
  Lwt_dls.new_key (fun () ->
    let dom = (Domain.self () :> int) in
    let inbox = Inbox.create () in
    let notification = Lwt_unix.make_notification (fun () -> drain inbox) in
    let l = { dom; inbox; notification } in
    register l;
    (* Closing the inbox is what makes a later [run_on] fail instead of dropping
       work silently. The notification goes too, so the id stops naming a live
       handler, and the registry entry with it. *)
    Lwt_dls.at_domain_exit (fun () ->
      Inbox.close inbox;
      Lwt_unix.stop_notification notification;
      deregister dom);
    l)

let self () = Lwt_dls.get self_slot

let run_on loop f =
  (match Inbox.push loop.inbox f with
   | () -> ()
   | exception Inbox.Closed -> raise Loop_terminated);
  (* Outside the push, and after it: the notification is a system call, and the
     work must be visible before the wake-up that announces it. *)
  Lwt_unix.send_notification loop.notification

(* +-----------------------------------------------------------------+
   | A value several loops can wait for                              |
   +-----------------------------------------------------------------+ *)

(* THE INVARIANT OF THIS MODULE, and the reason it is safe: a waiter's promise is
   never touched from anywhere but its own domain. It is STORED here, which is
   plain data, and it is RESOLVED inside a thunk that [run_on] runs on its own
   loop. The ownership check of the core is what enforces that rather than this
   comment; it would raise if we got it wrong.

   The mutex protects two fields and nothing else. Every critical section below is
   a test and an assignment: no Lwt operation, no system call, no allocation, per
   the rule the plan sets for these locks. Waking the waiters happens outside. *)

type 'a state = Pending | Fulfilled of 'a | Rejected of exn

type 'a waiter = {
  w_loop : loop;
  w_promise : 'a Lwt.t;
  w_resolver : 'a Lwt.u;
}

(* Built OUTSIDE any critical section, always: a promise is an allocation and an
   Lwt operation, and the rule for the locks in this module is that a critical
   section holds neither. *)
let new_waiter () =
  let promise, resolver = Lwt.task () in
  { w_loop = self (); w_promise = promise; w_resolver = resolver }

(* [w]'s promise belongs to [w]'s domain, so this has to happen there. Returns
   whether it actually resolved it: a waiter whose promise was cancelled
   meanwhile is a case the callers below must handle, since a resource may have
   been handed to it. *)
let wake_now w v =
  if Lwt.is_sleeping w.w_promise then (Lwt.wakeup w.w_resolver v; true)
  else false

(* Resolves [w] on its own loop, wherever that is. [on_dead] is for the caller to
   undo whatever it handed over, and runs on the waiter's loop too. *)
let wake w v ~on_dead =
  let here = self () in
  if w.w_loop == here then (if not (wake_now w v) then on_dead ())
  else
    match
      run_on w.w_loop (fun () -> if not (wake_now w v) then on_dead ())
    with
    | () -> ()
    | exception Loop_terminated -> on_dead ()

type 'a t = {
  mutex : Mutex.t;
  mutable state : 'a state;
  mutable waiters : 'a waiter list;
}

let create () = { mutex = Mutex.create (); state = Pending; waiters = [] }

let is_pending t =
  Mutex.lock t.mutex;
  let pending = match t.state with Pending -> true | _ -> false in
  Mutex.unlock t.mutex;
  pending

(* Runs on the waiter's own domain, so it may resolve the waiter's promise. The
   promise may have been cancelled meanwhile, hence the test, which is Lwt's own
   idiom for a resolver that may have been raced. *)
let deliver w result =
  if Lwt.is_sleeping w.w_promise then
    match result with
    | Ok v -> Lwt.wakeup w.w_resolver v
    | Error e -> Lwt.wakeup_exn w.w_resolver e

let wake_all waiters result =
  (* Read once: [self ()] is a slot lookup, and this loop is the same for every
     waiter in the list. *)
  let here = self () in
  List.iter
    (fun w ->
      if w.w_loop == here then
        (* Our own waiter, so resolve it now rather than posting to ourselves.
           That is not only cheaper, it is the right semantics: [Lwt.wakeup]
           resolves synchronously, and a [resolve] that quietly became
           asynchronous for the local case would be a trap. *)
        deliver w result
      else
        (* A waiter whose loop has gone is skipped: it is not the resolver's
           business that someone has terminated, and that loop's promise died
           with its domain. *)
        match run_on w.w_loop (fun () -> deliver w result) with
        | () -> ()
        | exception Loop_terminated -> ())
    waiters

(* Settles [t], or reports that it was settled already. Returns the waiters to
   wake, so that the waking is done by the caller, outside the lock. *)
let settle t state =
  Mutex.lock t.mutex;
  match t.state with
  | Pending ->
    t.state <- state;
    let waiters = t.waiters in
    t.waiters <- [];
    Mutex.unlock t.mutex;
    Some waiters
  | Fulfilled _ | Rejected _ ->
    Mutex.unlock t.mutex;
    None

let resolve t v =
  match settle t (Fulfilled v) with
  | Some waiters -> wake_all waiters (Ok v)
  | None -> invalid_arg "Lwt_multicore.resolve"

let reject t e =
  match settle t (Rejected e) with
  | Some waiters -> wake_all waiters (Error e)
  | None -> invalid_arg "Lwt_multicore.reject"

let cancel t =
  match settle t (Rejected Lwt.Canceled) with
  | Some waiters -> wake_all waiters (Error Lwt.Canceled)
  | None -> ()

let withdraw t w =
  Mutex.lock t.mutex;
  t.waiters <- List.filter (fun w' -> w' != w) t.waiters;
  Mutex.unlock t.mutex

let await t =
  (* Built BEFORE the lock is taken: a promise is an allocation and an Lwt
     operation, and neither belongs in a critical section shared with other
     domains. If [t] turns out to be settled, this promise is simply resolved at
     once. *)
  let promise, resolver = Lwt.task () in
  let w = { w_loop = self (); w_promise = promise; w_resolver = resolver } in
  Mutex.lock t.mutex;
  let state = t.state in
  (match state with Pending -> t.waiters <- w :: t.waiters | _ -> ());
  Mutex.unlock t.mutex;
  match state with
  | Fulfilled v -> Lwt.wakeup resolver v; promise
  | Rejected e -> Lwt.wakeup_exn resolver e; promise
  | Pending ->
    (* Cancelling the local promise withdraws this loop's interest and leaves [t]
       and the other waiters alone. *)
    Lwt.on_cancel promise (fun () -> withdraw t w);
    promise

(* +-----------------------------------------------------------------+
   | Adopting a foreign promise                                      |
   +-----------------------------------------------------------------+ *)

exception Cannot_adopt

let adopt (p : 'a Lwt.t) : 'a Lwt.t =
  match Lwt.Private.promise_owner_domain p with
  (* Already resolved, so owned by nobody and readable from anywhere. *)
  | None -> p
  | Some owner ->
    if owner = (Domain.self () :> int) then
      (* Ours already. Adopting it would be a needless round trip, and returning
         it unchanged is what the caller means. *)
      p
    else begin
      match registered owner with
      | None -> raise Cannot_adopt
      | Some owner_loop ->
        let shared = create () in
        (* The callback is attached BY THE OWNER, on its own domain: attaching it
           ourselves is precisely what the ownership check forbids, and is the
           reason this function exists. If the promise resolves before the thunk
           runs, [on_any] fires at once, which is the same outcome. *)
        (match
           run_on owner_loop (fun () ->
             Lwt.on_any p
               (fun v -> resolve shared v)
               (fun e -> reject shared e))
         with
         | () -> ()
         | exception Loop_terminated -> raise Cannot_adopt);
        await shared
    end

(* +-----------------------------------------------------------------+
   | Mutual exclusion, conditions, counting                          |
   +-----------------------------------------------------------------+ *)

(* The three below share one shape and one protocol, worth stating once.

   Each keeps its state under an ORDINARY mutex, whose critical sections are a
   test and an assignment: no Lwt operation, no system call, no allocation. The
   waiters queue holds promises of other loops, which are DATA here and resolved
   only on their own domain.

   The protocol that needs care is HANDING THE RESOURCE OVER. Serving a waiter
   means choosing it under the lock and waking it outside, and in between its
   promise may be cancelled by its own domain. Then the resource has been handed
   to nobody, so the waking function hands it BACK, on the waiter's loop, by
   calling the release path again. Every round consumes one waiter, so this
   terminates. That is what the [~on_dead] argument of [wake] is for, and it is
   the only subtle thing in these hundred lines. *)

(* A first-in, first-out queue kept as a pair of lists. Short by nature: these are
   domain boundaries, not hot paths. Holds waiters below, and stream items too. *)
type 'a queue = { mutable front : 'a list; mutable back : 'a list }

let queue_create () = { front = []; back = [] }
let queue_push q x = q.back <- x :: q.back
let queue_push_front q x = q.front <- x :: q.front
let queue_length q = List.length q.front + List.length q.back

let rec queue_pop q =
  match q.front with
  | x :: rest -> q.front <- rest; Some x
  | [] -> (
    match q.back with
    | [] -> None
    | back -> q.front <- List.rev back; q.back <- []; queue_pop q)

(* Drops an element that has been withdrawn, by identity. O(n), on a queue that is
   short. *)
let queue_remove q x =
  q.front <- List.filter (fun y -> y != x) q.front;
  q.back <- List.filter (fun y -> y != x) q.back

module Mutex = struct
  type t = {
    guard : Stdlib.Mutex.t;
    mutable held : bool;
    waiters : unit waiter queue;
  }

  let create () =
    { guard = Stdlib.Mutex.create (); held = false; waiters = queue_create () }

  let is_locked t =
    Stdlib.Mutex.lock t.guard;
    let held = t.held in
    Stdlib.Mutex.unlock t.guard;
    held

  (* Hands the lock to the next live waiter, or releases it. Also the path taken
     when a served waiter turns out to have been cancelled. *)
  let rec hand_over t =
    Stdlib.Mutex.lock t.guard;
    match queue_pop t.waiters with
    | None ->
      t.held <- false;
      Stdlib.Mutex.unlock t.guard
    | Some w ->
      (* The lock stays held: it passes to [w] without becoming free, which is
         what keeps a third party from jumping the queue. *)
      Stdlib.Mutex.unlock t.guard;
      wake w () ~on_dead:(fun () -> hand_over t)

  let unlock t =
    Stdlib.Mutex.lock t.guard;
    if not t.held then (Stdlib.Mutex.unlock t.guard)
    else begin
      Stdlib.Mutex.unlock t.guard;
      hand_over t
    end

  let lock t =
    let w = new_waiter () in
    Stdlib.Mutex.lock t.guard;
    let taken =
      if t.held then (queue_push t.waiters w; false)
      else (t.held <- true; true)
    in
    Stdlib.Mutex.unlock t.guard;
    if taken then Lwt.return_unit
    else begin
      Lwt.on_cancel w.w_promise (fun () ->
        Stdlib.Mutex.lock t.guard;
        queue_remove t.waiters w;
        Stdlib.Mutex.unlock t.guard);
      w.w_promise
    end

  let with_lock t f =
    Lwt.bind (lock t) (fun () ->
      Lwt.finalize f (fun () -> unlock t; Lwt.return_unit))
end

module Semaphore = struct
  type t = {
    guard : Stdlib.Mutex.t;
    mutable count : int;
    waiters : unit waiter queue;
  }

  let create count =
    if count < 0 then invalid_arg "Lwt_multicore.Semaphore.create";
    { guard = Stdlib.Mutex.create (); count; waiters = queue_create () }

  let available t =
    Stdlib.Mutex.lock t.guard;
    let count = t.count in
    Stdlib.Mutex.unlock t.guard;
    count

  (* Gives the unit to the next live waiter, or puts it back in the count. *)
  let rec release t =
    Stdlib.Mutex.lock t.guard;
    match queue_pop t.waiters with
    | None ->
      t.count <- t.count + 1;
      Stdlib.Mutex.unlock t.guard
    | Some w ->
      Stdlib.Mutex.unlock t.guard;
      wake w () ~on_dead:(fun () -> release t)

  let acquire t =
    let w = new_waiter () in
    Stdlib.Mutex.lock t.guard;
    let taken =
      if t.count > 0 then (t.count <- t.count - 1; true)
      else (queue_push t.waiters w; false)
    in
    Stdlib.Mutex.unlock t.guard;
    if taken then Lwt.return_unit
    else begin
      Lwt.on_cancel w.w_promise (fun () ->
        Stdlib.Mutex.lock t.guard;
        queue_remove t.waiters w;
        Stdlib.Mutex.unlock t.guard);
      w.w_promise
    end

  let with_resource t f =
    Lwt.bind (acquire t) (fun () ->
      Lwt.finalize f (fun () -> release t; Lwt.return_unit))
end

module Condition = struct
  type 'a t = { guard : Stdlib.Mutex.t; waiters : 'a waiter queue }

  let create () = { guard = Stdlib.Mutex.create (); waiters = queue_create () }

  (* Nothing is handed over by a signal, so a cancelled waiter is simply not
     there any more and the value is dropped, exactly as [Lwt_condition] does
     when nobody is waiting. *)
  let rec signal t v =
    Stdlib.Mutex.lock t.guard;
    let w = queue_pop t.waiters in
    Stdlib.Mutex.unlock t.guard;
    match w with
    | None -> ()
    | Some w -> wake w v ~on_dead:(fun () -> signal t v)

  let broadcast t v =
    Stdlib.Mutex.lock t.guard;
    let rec drain acc =
      match queue_pop t.waiters with
      | Some w -> drain (w :: acc)
      | None -> acc
    in
    let waiters = drain [] in
    Stdlib.Mutex.unlock t.guard;
    List.iter (fun w -> wake w v ~on_dead:(fun () -> ())) waiters

  let wait ?mutex t =
    let w = new_waiter () in
    Stdlib.Mutex.lock t.guard;
    queue_push t.waiters w;
    Stdlib.Mutex.unlock t.guard;
    Lwt.on_cancel w.w_promise (fun () ->
      Stdlib.Mutex.lock t.guard;
      queue_remove t.waiters w;
      Stdlib.Mutex.unlock t.guard);
    (* Same discipline as [Lwt_condition.wait]: release the mutex while waiting
       and take it again afterwards, so that a signaller can get in. *)
    (match mutex with Some m -> Mutex.unlock m | None -> ());
    Lwt.finalize
      (fun () -> w.w_promise)
      (fun () -> match mutex with Some m -> Mutex.lock m | None -> Lwt.return_unit)
end

module Stream = struct
  exception Closed

  type 'a t = {
    guard : Stdlib.Mutex.t;
    capacity : int;
    items : 'a queue;
    (* Loops waiting to take. Each expects an option, [None] meaning "closed and
       drained", so that a consumer learns the end rather than waiting for it. *)
    takers : 'a option waiter queue;
    (* Loops waiting for room. They carry nothing: a woken pusher retries, which
       is what keeps cancellation from having to give a value back. *)
    room : unit waiter queue;
    mutable closed : bool;
  }

  let create ~capacity =
    if capacity < 1 then invalid_arg "Lwt_multicore.Stream.create";
    { guard = Stdlib.Mutex.create ();
      capacity;
      items = queue_create ();
      takers = queue_create ();
      room = queue_create ();
      closed = false }

  let length t =
    Stdlib.Mutex.lock t.guard;
    let n = queue_length t.items in
    Stdlib.Mutex.unlock t.guard;
    n

  let is_closed t =
    Stdlib.Mutex.lock t.guard;
    let closed = t.closed in
    Stdlib.Mutex.unlock t.guard;
    closed

  (* Hands [v] to a waiting taker, or stores it. Also the path taken when a taker
     that had been given [v] turns out to have been cancelled: the value comes
     back rather than being lost, and goes to the FRONT so that order is kept. *)
  let rec deposit t v =
    Stdlib.Mutex.lock t.guard;
    match queue_pop t.takers with
    | Some w ->
      Stdlib.Mutex.unlock t.guard;
      wake w (Some v) ~on_dead:(fun () -> deposit t v)
    | None ->
      queue_push_front t.items v;
      Stdlib.Mutex.unlock t.guard

  (* One unit of room has appeared: let the first waiting pusher retry. *)
  let rec offer_room t =
    Stdlib.Mutex.lock t.guard;
    match queue_pop t.room with
    | None -> Stdlib.Mutex.unlock t.guard
    | Some w ->
      Stdlib.Mutex.unlock t.guard;
      wake w () ~on_dead:(fun () -> offer_room t)

  let rec push t v =
    let w = new_waiter () in
    Stdlib.Mutex.lock t.guard;
    if t.closed then begin
      Stdlib.Mutex.unlock t.guard;
      Lwt.fail Closed
    end
    else
      match queue_pop t.takers with
      | Some taker ->
        (* Straight to a waiting consumer, without touching the buffer. *)
        Stdlib.Mutex.unlock t.guard;
        wake taker (Some v) ~on_dead:(fun () -> deposit t v);
        Lwt.return_unit
      | None ->
        if queue_length t.items < t.capacity then begin
          queue_push t.items v;
          Stdlib.Mutex.unlock t.guard;
          Lwt.return_unit
        end
        else begin
          (* Full: this is the back-pressure. Wait for room and RETRY rather than
             leaving the value with the queue, which is what makes cancelling a
             push harmless: nothing has been handed over. *)
          queue_push t.room w;
          Stdlib.Mutex.unlock t.guard;
          Lwt.on_cancel w.w_promise (fun () ->
            Stdlib.Mutex.lock t.guard;
            queue_remove t.room w;
            Stdlib.Mutex.unlock t.guard);
          Lwt.bind w.w_promise (fun () -> push t v)
        end

  let take t =
    let w = new_waiter () in
    Stdlib.Mutex.lock t.guard;
    match queue_pop t.items with
    | Some v ->
      Stdlib.Mutex.unlock t.guard;
      (* Taking freed a slot, so a blocked producer may go. *)
      offer_room t;
      Lwt.return (Some v)
    | None ->
      if t.closed then begin
        Stdlib.Mutex.unlock t.guard;
        Lwt.return_none
      end
      else begin
        queue_push t.takers w;
        Stdlib.Mutex.unlock t.guard;
        Lwt.on_cancel w.w_promise (fun () ->
          Stdlib.Mutex.lock t.guard;
          queue_remove t.takers w;
          Stdlib.Mutex.unlock t.guard);
        w.w_promise
      end

  let close t =
    Stdlib.Mutex.lock t.guard;
    let takers = ref [] and room = ref [] in
    if not t.closed then begin
      t.closed <- true;
      let rec drain q acc =
        match queue_pop q with Some w -> drain q (w :: acc) | None -> acc
      in
      takers := drain t.takers [];
      room := drain t.room []
    end;
    Stdlib.Mutex.unlock t.guard;
    (* Consumers learn the end; producers waiting for room learn there will be
       none, since pushing to a closed stream fails. *)
    List.iter (fun w -> wake w None ~on_dead:(fun () -> ())) !takers;
    (* A producer waiting for room is REJECTED rather than woken: there will be no
       room, and retrying would only find the stream closed. [wake] carries a
       value, so this one is spelled out. *)
    let reject_producer w =
      if Lwt.is_sleeping w.w_promise then Lwt.wakeup_exn w.w_resolver Closed
    in
    let here = self () in
    List.iter
      (fun w ->
        if w.w_loop == here then reject_producer w
        else
          match run_on w.w_loop (fun () -> reject_producer w) with
          | () -> ()
          | exception Loop_terminated -> ())
      !room
end

(* +-----------------------------------------------------------------+
   | A domain that serves requests                                   |
   +-----------------------------------------------------------------+ *)

(* Aliases, so that the submodules below can name the shared value and make one
   while defining a [create] of their own. *)
type 'a shared = 'a t

let new_shared = create

module Service = struct
  type ('req, 'res) t = {
    requests : ('req * 'res shared) Stream.t;
    stopped : unit shared;
    domain : unit Domain.t;
  }

  let create ?(capacity = 64) handler =
    let requests = Stream.create ~capacity in
    let stopped = new_shared () in
    let domain =
      Domain.spawn (fun () ->
        Lwt_main.run
          (let rec serve () =
             Lwt.bind (Stream.take requests) (function
               | None -> Lwt.return_unit
               | Some (req, reply) ->
                 (* Each request is served to completion before the next is
                    taken: a service is one loop, and concurrency between
                    requests is the caller's business, obtained by having several
                    services or by the handler returning early. *)
                 Lwt.bind
                   (Lwt.catch
                      (fun () -> Lwt.bind (handler req) (fun res ->
                         resolve reply res; Lwt.return_unit))
                      (fun exn -> reject reply exn; Lwt.return_unit))
                   serve)
           in
           serve ());
        (* Announced before this domain exits, so that [shutdown] can wait for the
           work to be finished rather than for the domain to be reaped. *)
        resolve stopped ())
    in
    { requests; stopped; domain }

  let call t req =
    let reply = new_shared () in
    Lwt.bind (Stream.push t.requests (req, reply)) (fun () -> await reply)

  let shutdown t =
    Stream.close t.requests;
    Lwt.bind (await t.stopped) (fun () ->
      (* The service has finished its work by now, so this is short. It is done
         at all so that no domain is left unreaped. *)
      Domain.join t.domain;
      Lwt.return_unit)
end

(* +-----------------------------------------------------------------+
   | Running work on another domain                                  |
   +-----------------------------------------------------------------+ *)

(* A pool of services, each on its own domain, all serving the same thing: a
   thunk. That is all [detach] needs, and building it on [Service] rather than on
   anything new is the point: if the service pattern is right, the offload is
   twenty lines.

   Round-robin rather than least-loaded, deliberately: choosing by load would mean
   reading every service's queue length across domains on every call, which costs
   more than it saves for work whose whole premise is that it is expensive. *)
module Pool = struct
  type t = {
    workers : (unit -> unit, unit) Service.t array;
    next : int Atomic.t;
  }

  let create ?capacity ?count () =
    let count =
      match count with
      | Some n when n > 0 -> n
      | Some _ -> invalid_arg "Lwt_multicore.Pool.create"
      | None -> max 1 (Domain.recommended_domain_count () - 1)
    in
    { workers =
        Array.init count (fun _ ->
          Service.create ?capacity (fun thunk -> thunk (); Lwt.return_unit));
      next = Atomic.make 0 }

  let size t = Array.length t.workers

  let detach t f x =
    let result = new_shared () in
    let i = Atomic.fetch_and_add t.next 1 mod Array.length t.workers in
    let thunk () =
      (* Runs on the worker's domain. The result travels as data; [resolve] is
         callable from anywhere. *)
      match f x with
      | v -> resolve result v
      | exception exn -> reject result exn
    in
    Lwt.bind (Service.call t.workers.(i) thunk) (fun () -> await result)

  let shutdown t =
    Lwt.join (Array.to_list (Array.map Service.shutdown t.workers))
end
