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
