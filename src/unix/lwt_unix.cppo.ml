(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* [Lwt_sequence] is deprecated – we don't want users outside Lwt using it.
   However, it is still used internally by Lwt. So, briefly disable warning 3
   ("deprecated"), and create a local, non-deprecated alias for
   [Lwt_sequence] that can be referred to by the rest of the code in this
   module without triggering any more warnings. *)
module Lwt_sequence = Lwt_sequence

open Lwt.Infix

(* +-----------------------------------------------------------------+
   | This domain's notification channel                              |
   +-----------------------------------------------------------------+ *)

(* Jobs, signal handlers and child waiting reach Lwt through a notification
   channel. There is now ONE PER LOOP: the C side keeps a table of channels and a
   notification id carries the index of the one it belongs to, so a completion
   wakes the loop that asked for the work.

   Jobs have caught up: they are per loop, and a completion wakes the loop that
   submitted the work. What has not, and is the rest of this phase: the signal
   handlers and child waiting, which keep the owner check below because a signal
   is a process-wide resource and needs a policy rather than a channel.

   So this module has an owner, and it is the domain that initialised it. On
   another domain, submitting a job would have the completion wake a promise the
   submitter owns from a domain that does not own it: a cross-domain wakeup, with
   no synchronisation, into another domain's run queue. That does not work, and
   it fails silently. The operations that need the owner therefore check, and say
   so.

   This is a temporary state of affairs, and the check is what makes it visible
   rather than mysterious: giving every domain its own notification descriptor
   means emptying the C singleton, which is the next phase's work. There is no
   inter-domain API here either way -- the restriction only ever gets weaker.

   The marker is a per-domain slot set to [true] exactly once, at module
   initialisation, which is more accurate than asking whether we are the main
   domain: the owner is whoever loaded the module. In the single-domain case,
   which is every existing program, it reads [true] and nothing changes. *)
[@@@alert "-lwt_internal"]

(* Builds the lock that guards the channel table, where the platform has no static
   initialiser for it (Windows). Called ONCE, here, at module initialisation: that
   runs on the main domain at program startup, before any other domain exists, which
   is what makes it race-free. Doing it lazily on first use is what TSan caught. *)
external init_notifications : unit -> unit = "lwt_unix_init_notifications"

let () = init_notifications ()

external new_notification_channel : unit -> Unix.file_descr * int
  = "lwt_unix_new_notification_channel"

external free_notification_channel : int -> unit
  = "lwt_unix_free_notification_channel"

external send_notification : int -> unit = "lwt_unix_send_notification_stub"

external recv_notifications : int -> int array = "lwt_unix_recv_notifications"

external encode_notification : int -> int -> int = "lwt_unix_encode_notification"

(* Forward cells: draining needs [call_notification], and retiring a loop needs
   [remove_signal]; both are defined below. Set once, each at the end of its
   section. *)
let drain_notifications : (int -> unit) ref = ref (fun _ -> ())
let drop_signal_subscriptions : (unit -> unit) ref = ref (fun () -> ())

(* PER DOMAIN: the channel this loop is woken through, and the engine event that
   watches it. Created on first use, so a domain that never touches Lwt_unix
   never opens a descriptor.

   The channel's index is what the C side needs, and the notification ids this
   domain hands out carry it, which is what lets a pool thread or a signal
   handler wake the right loop without asking anything about domains. *)
(* Declared here rather than where they are used, so that the per-domain record
   below can mention them. *)

type notification = int (* a simple ID, carrying its channel's index *)

module Signal_map = Map.Make(struct type t = int let compare a b = a - b end)

type signal_handler = {
  sh_num : int;
  sh_node : (signal_handler_id -> int -> unit) Lwt_sequence.node;
}

and signal_handler_id = signal_handler option ref

type resource_usage = { ru_utime : float; ru_stime : float }

type notif = {
  chan : int;
  event : Lwt_engine.event;
  (* The jobs this loop has in flight, with the function that aborts each, and
     their count. Per domain because a completion wakes the loop that submitted
     the job, so that is the loop that must remove the node and decrement. *)
  jobs : (unit Lwt.t * (exn -> unit)) Lwt_sequence.t;
  mutable job_count : int;
  (* The signals this loop is subscribed to, with the notification each arrives
     on and the handlers to run. Per domain because a signal is delivered to
     every loop that asked for it, on its own channel: the C side keeps one
     subscriber slot per channel. *)
  mutable signals :
    (notification * (signal_handler_id -> int -> unit) Lwt_sequence.t)
    Signal_map.t;
  (* Children this loop is waiting for, and whether it has subscribed to
     SIGCHLD. Per domain for the same reason: each loop reaps the children it
     waits for, woken by its own subscription. *)
  wait_children :
    ((int * Unix.process_status * resource_usage) Lwt.u
     * Unix.wait_flag list
     * int)
    Lwt_sequence.t;
  mutable sigchld_installed : bool;
}

let notif_slot : notif Lwt_dls.t =
  Lwt_dls.new_key (fun () ->
    let fd, chan = new_notification_channel () in
    let event = Lwt_engine.on_readable fd (fun _ -> !drain_notifications chan) in
    (* Retire the channel with the domain, but NOT on the main one, and that
       exception is the whole subtlety.

       ORDER, on a spawned domain. [Domain.at_exit] is LIFO, so what is registered
       LAST runs FIRST. [Lwt_main]'s exit-hook drain runs a loop, and an exit hook
       may well need a job (flushing an [Lwt_io] channel does), so the drain must
       happen while this channel still lives. That is why [Lwt_main]'s hooks slot
       calls {!ensure_channel} before registering its own drain: this registration
       then comes first and therefore runs last.

       ON THE MAIN DOMAIN that reasoning breaks, and retiring anything here is a
       BUG. The main domain's drain is a [Stdlib.at_exit], deliberately, so that
       Lwt's exit hooks keep running after a user's own handlers; and every
       [Domain.at_exit] fires BEFORE any [Stdlib.at_exit]. So retiring the channel
       here would leave the drain running without one, and the first exit hook that
       needs a job would hang the process at exit. That is not a corner case: the
       hook in question is [Lwt_io]'s [flush_all], so any program that wrote to
       Lwt_io.stdout and did not flush by hand would hang. Found by the soak of S6.

       Nothing is lost by skipping it: the process is ending, and the kernel closes
       descriptors better than we do.

       A job still in flight when this runs is safe rather than merely unlikely:
       the channel struct is never freed, only returned to a free list, and the
       generation in its ids means a late completion is dropped instead of waking
       whoever takes the slot next. Nobody is waiting for it either way, the
       domain being gone. *)
    if not (Lwt_dls.is_main_domain ()) then
      Lwt_dls.at_domain_exit (fun () ->
        (* Unsubscribe from every signal this loop subscribed to, FIRST.
           Otherwise the process-wide handler stays installed for a loop that no
           longer exists, and the signal keeps being swallowed: a program whose
           last subscriber died would stop dying on SIGTERM. *)
        !drop_signal_subscriptions ();
        Lwt_engine.stop_event event;
        free_notification_channel chan);
    { chan;
      event;
      jobs = Lwt_sequence.create ();
      job_count = 0;
      signals = Signal_map.empty;
      wait_children = Lwt_sequence.create ();
      sigchld_installed = false })

let[@inline] self_notif () = Lwt_dls.get notif_slot
let[@inline] self_channel () = (Lwt_dls.get notif_slot).chan

(* Forces this domain's channel into existence. Called by [Lwt_main] before it
   registers its exit drain, so that the drain runs while the channel lives; see
   the ORDER note above. *)
let ensure_channel () = ignore (Lwt_dls.get notif_slot)

(* Which domain initialised this module. Only [fork] still cares, and not for any
   reason to do with notifications: the runtime does not support forking while
   other domains are running, and Lwt's [fork] also reinitialises this domain's
   channel and empties its job sequence. *)
let is_loader_slot : bool Lwt_dls.t = Lwt_dls.new_key (fun () -> false)
let () = Lwt_dls.set is_loader_slot true

let[@inline never] not_the_loader name =
  failwith
    (name ^ ": only available on the domain that initialised Lwt_unix, since "
    ^ "the runtime does not support forking while other domains are running")

let[@inline] check_loader name =
  if not (Lwt_dls.get is_loader_slot) then not_the_loader name



(* +-----------------------------------------------------------------+
   | Configuration                                                   |
   +-----------------------------------------------------------------+ *)

type async_method =
  | Async_none
  | Async_detach

let default_async_method_var = ref Async_detach

let () =
  try
    match Sys.getenv "LWT_ASYNC_METHOD" with
    | "none" ->
      default_async_method_var := Async_none
    | "detach" ->
      default_async_method_var := Async_detach
    | str ->
      Printf.eprintf
        "%s: invalid lwt async method: '%s', must be 'none' or 'detach'\n%!"
        (Filename.basename Sys.executable_name) str
  with Not_found ->
    ()

let default_async_method () = !default_async_method_var
let set_default_async_method am = default_async_method_var := am

let async_method_key = Lwt.new_key ()

let async_method () =
  match Lwt.get async_method_key with
  | Some am -> am
  | None -> !default_async_method_var

let with_async_none f =
  Lwt.with_value async_method_key (Some Async_none) f

let with_async_detach f =
  Lwt.with_value async_method_key (Some Async_detach) f

(* +-----------------------------------------------------------------+
   | Notifications management                                        |
   +-----------------------------------------------------------------+ *)

(* Informations about a notifier *)
type notifier = {
  notify_handler : unit -> unit;
  (* The callback *)

  notify_once : bool;
  (* Whether to remove the notifier after the reception of the first
     notification *)
}

module Notifiers = Hashtbl.Make(struct
    type t = int
    let equal (x : int) (y : int) = x = y
    let hash (x : int) = x
  end)

(* PROCESS-WIDE, like the notification descriptor it indexes, and the one piece
   of state here that any domain may touch: [send_notification] is documented as
   thread-safe, so building and stopping a notification has to be safe from
   wherever the sending thread lives. A [Hashtbl] is not, hence the mutex, which
   also gives the memory barrier that makes an entry created on one domain
   visible to the owner reading it. It is taken for the table operations only,
   never while a handler runs: handlers run arbitrary Lwt code, including code
   that creates further notifications. *)
let notifiers = Notifiers.create 1024
let notifiers_mutex = Mutex.create ()

let[@inline] with_notifiers f =
  Mutex.lock notifiers_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock notifiers_mutex) f

(* See https://github.com/ocsigen/lwt/issues/277 and
   https://github.com/ocsigen/lwt/pull/278. *)
let current_notification_id = ref (0x7FFFFFFF - 1000)

(* The id carries the CALLING domain's channel, so the handler runs on the domain
   that created the notification, whichever domain sends it. The local counter
   stays process-wide and under the same lock; the C side masks it to whatever
   room the word has left, and the table below is what settles collisions. *)
let make_notification ?(once=false) f =
  let chan = self_channel () in
  with_notifiers (fun () ->
    let rec find local =
      let id = encode_notification chan local in
      if Notifiers.mem notifiers id then find (local + 1) else (id, local)
    in
    let id, local = find (!current_notification_id + 1) in
    current_notification_id := local;
    Notifiers.add notifiers id { notify_once = once; notify_handler = f };
    id)

let stop_notification id =
  with_notifiers (fun () -> Notifiers.remove notifiers id)

let set_notification id f =
  with_notifiers (fun () ->
    let notifier = Notifiers.find notifiers id in
    Notifiers.replace notifiers id { notifier with notify_handler = f })

let call_notification id =
  (* The lookup and the [once] removal are atomic together; the handler runs
     after the lock is released. *)
  match
    with_notifiers (fun () ->
      match Notifiers.find notifiers id with
      | exception Not_found -> None
      | notifier ->
        if notifier.notify_once then Notifiers.remove notifiers id;
        Some notifier.notify_handler)
  with
  | None -> ()
  | Some handler -> handler ()

(* +-----------------------------------------------------------------+
   | Sleepers                                                        |
   +-----------------------------------------------------------------+ *)

let sleep delay =
  let waiter, wakener = Lwt.task () in
  let ev = Lwt_engine.on_timer delay false (fun ev -> Lwt_engine.stop_event ev; Lwt.wakeup wakener ()) in
  Lwt.on_cancel waiter (fun () -> Lwt_engine.stop_event ev);
  waiter

let yield = Lwt.pause

let auto_yield timeout =
  let limit = ref (Unix.gettimeofday () +. timeout) in
  fun () ->
    let current = Unix.gettimeofday () in
    if current >= !limit then begin
      limit := current +. timeout;
      yield ();
    end else
      Lwt.return_unit

let auto_pause timeout =
  let limit = ref (Unix.gettimeofday () +. timeout) in
  fun () ->
    let current = Unix.gettimeofday () in
    if current >= !limit then begin
      limit := current +. timeout;
      Lwt.pause ();
    end else
      Lwt.return_unit

exception Timeout

let timeout d = sleep d >>= fun () -> raise Timeout

let with_timeout d f = Lwt.pick [timeout d; Lwt.apply f ()]

(* +-----------------------------------------------------------------+
   | Jobs                                                            |
   +-----------------------------------------------------------------+ *)

type 'a job

external start_job : 'a job -> async_method -> bool = "lwt_unix_start_job"
(* Starts the given job with given parameters. It returns [true]
   if the job is already terminated. *)

external check_job : 'a job -> int -> bool = "lwt_unix_check_job" "noalloc"
(* Check whether that a job has terminated or not. If it has not
   yet terminated, it is marked so it will send a notification
   when it finishes. *)
[@@ocaml.warning "-3"]

(* Each of these acts on THIS loop's jobs, which is the only set a domain has any
   business touching, and the only one it can service. *)
let rec abort_jobs_aux jobs exn =
  match Lwt_sequence.take_opt_l jobs with
  | Some (_, f) -> f exn; abort_jobs_aux jobs exn
  | None -> ()

let abort_jobs exn = abort_jobs_aux (self_notif ()).jobs exn
let cancel_jobs () = abort_jobs_aux (self_notif ()).jobs Lwt.Canceled

let wait_for_jobs () =
  Lwt.join
    (Lwt_sequence.fold_l (fun (w, _) l -> w :: l) (self_notif ()).jobs [])

let run_job_aux async_method job result =
  (* Starts the job. *)
  if start_job job async_method then
    (* The job has already terminated, read and return the result
       immediately. *)
    Lwt.of_result (result job)
  else begin
    (* Thread for the job. *)
    let waiter, wakener = Lwt.wait () in
    (* Add the job to this loop's sequence. One slot read for the whole
       operation: the notification the completion needs comes from the same
       record. *)
    let notif = self_notif () in
    let node = Lwt_sequence.add_l (
      (waiter >>= fun _ -> Lwt.return_unit),
      (fun exn -> if Lwt.state waiter = Lwt.Sleep then Lwt.wakeup_exn wakener exn))
      notif.jobs in
    notif.job_count <- notif.job_count + 1;
    ignore begin
      (* Create the notification for asynchronous wakeup. *)
      let notification =
        make_notification ~once:true
          (fun () ->
             Lwt_sequence.remove node;
             notif.job_count <- notif.job_count - 1;
             let result = result job in
             if Lwt.state waiter = Lwt.Sleep then Lwt.wakeup_result wakener result)
      in
      (* Give the job some time before we fallback to asynchronous
         notification. *)
      Lwt.pause () >>= fun () ->
      (* The job has terminated, send the result immediately. *)
      if check_job job notification then call_notification notification;
      Lwt.return_unit
    end;
    waiter
  end

let choose_async_method = function
  | Some async_method ->
    async_method
  | None ->
    match Lwt.get async_method_key with
    | Some am -> am
    | None -> !default_async_method_var

external self_result : 'a job -> 'a = "lwt_unix_self_result"
(* returns the result of a job using the [result] field of the C
   job structure. *)

external run_job_sync : 'a job -> 'a = "lwt_unix_run_job_sync"
(* Exeuctes a job synchronously and returns its result. *)

let self_result job =
  try
    Result.Ok (self_result job)
  with exn when Lwt.Exception_filter.run exn ->
    Result.Error exn

let run_job ?async_method job =
  let async_method = choose_async_method async_method in
  if async_method = Async_none then
    try
      Lwt.return (run_job_sync job)
    with exn when Lwt.Exception_filter.run exn ->
      Lwt.fail exn
  else
    run_job_aux async_method job self_result

(* +-----------------------------------------------------------------+
   | File descriptor wrappers                                        |
   +-----------------------------------------------------------------+ *)

type state = Opened | Closed | Aborted of exn

type file_descr = {
  fd : Unix.file_descr;
  (* The underlying unix file descriptor *)

  mutable state: state;
  (* The state of the file descriptor *)

  mutable set_flags : bool;
  (* Whether to set file flags *)

  mutable blocking : bool Lwt.t Lazy.t;
  (* Is the file descriptor in blocking or non-blocking mode *)

  mutable event_readable : Lwt_engine.event option;
  (* The event used to check the file descriptor for readability. *)

  mutable event_writable : Lwt_engine.event option;
  (* The event used to check the file descriptor for writability. *)

  hooks_readable : (unit -> unit) Lwt_sequence.t;
  (* Hooks to call when the file descriptor becomes readable. *)

  hooks_writable : (unit -> unit) Lwt_sequence.t;
  (* Hooks to call when the file descriptor becomes writable. *)

  owner : Lwt_dls.token;
  (* DOMAIN-AFFINE. The wrapper carries readiness events registered on ITS
     domain's engine, and hook sequences whose callbacks run there, so it belongs
     to the domain that created it. Checked in [check_descriptor], which every
     operation already calls: measured at 27 instructions per check, so some 6%
     on a socket write-then-read round trip. *)

  mutable io_kind : Unix.file_kind option;
  (* Cached [Unix.fstat] kind of the descriptor, computed lazily by {!fd_kind}.
     Used by a completion-based I/O backend (e.g. io_uring) to pick the right
     operation per descriptor type. Cached here because the record is stable for
     the lifetime of the descriptor, unlike the (reusable) raw fd number. *)
}

external is_socket : Unix.file_descr -> bool = "lwt_unix_is_socket" "noalloc"
[@@ocaml.warning "-3"]

external guess_blocking_job : Unix.file_descr -> bool job = "lwt_unix_guess_blocking_job"

let guess_blocking fd =
  run_job (guess_blocking_job fd)

let is_blocking ?blocking ?(set_flags=true) fd =
  if Sys.win32 then begin
    if is_socket fd then
      match blocking, set_flags with
      | Some state, false ->
        lazy(Lwt.return state)
      | Some true, true ->
        lazy(Unix.clear_nonblock fd;
             Lwt.return_true)
      | Some false, true ->
        lazy(Unix.set_nonblock fd;
             Lwt.return_false)
      | None, false ->
        lazy(Lwt.return_false)
      | None, true ->
        lazy(Unix.set_nonblock fd;
             Lwt.return_false)
    else
      match blocking with
      | Some state ->
        lazy(Lwt.return state)
      | None ->
        lazy(Lwt.return_true)
  end else begin
    match blocking, set_flags with
    | Some state, false ->
      lazy(Lwt.return state)
    | Some true, true ->
      lazy(Unix.clear_nonblock fd;
           Lwt.return_true)
    | Some false, true ->
      lazy(Unix.set_nonblock fd;
           Lwt.return_false)
    | None, false ->
      lazy(guess_blocking fd)
    | None, true ->
      lazy(guess_blocking fd >>= function
      | true ->
        Unix.clear_nonblock fd;
        Lwt.return_true
      | false ->
        Unix.set_nonblock fd;
        Lwt.return_false)
  end

let mk_ch ?blocking ?(set_flags=true) fd = {
  fd = fd;
  state = Opened;
  set_flags = set_flags;
  blocking = is_blocking ?blocking ~set_flags fd;
  event_readable = None;
  event_writable = None;
  hooks_readable = Lwt_sequence.create ();
  hooks_writable = Lwt_sequence.create ();
  io_kind = None;
  owner = Lwt_dls.self_token ();
}

let check_descriptor ch =
  (* Comparison written out rather than [check_owner]: this runs on every
     operation, and the call costs more than the check. *)
  if ch.owner <> Lwt_dls.self_token () then
    Lwt_dls.foreign "Lwt_unix file descriptor";
  match ch.state with
  | Opened ->
    ()
  | Aborted e ->
    raise e
  | Closed ->
    raise (Unix.Unix_error (Unix.EBADF, "check_descriptor", ""))

let state ch = ch.state

let blocking ch =
  check_descriptor ch;
  Lazy.force ch.blocking

let set_blocking ?(set_flags=true) ch blocking =
  check_descriptor ch;
  ch.set_flags <- set_flags;
  ch.blocking <- is_blocking ~blocking ~set_flags ch.fd

external unix_stub_readable : Unix.file_descr -> bool = "lwt_unix_readable"
external unix_stub_writable : Unix.file_descr -> bool = "lwt_unix_writable"

let rec unix_readable fd =
  try
    if Sys.win32 then
      Unix.select [fd] [] [] 0.0 <> ([], [], [])
    else
      unix_stub_readable fd
  with Unix.Unix_error (Unix.EINTR, _, _) ->
    unix_readable fd

let rec unix_writable fd =
  try
    if Sys.win32 then
      Unix.select [] [fd] [] 0.0 <> ([], [], [])
    else
      unix_stub_writable fd
  with Unix.Unix_error (Unix.EINTR, _, _) ->
    unix_writable fd

let readable ch =
  check_descriptor ch;
  unix_readable ch.fd

let writable ch =
  check_descriptor ch;
  unix_writable ch.fd

let set_state ch st =
  ch.state <- st

let clear_events ch =
  Lwt_sequence.iter_node_l (fun node -> Lwt_sequence.remove node; Lwt_sequence.get node ()) ch.hooks_readable;
  Lwt_sequence.iter_node_l (fun node -> Lwt_sequence.remove node; Lwt_sequence.get node ()) ch.hooks_writable;
  begin
    match ch.event_readable with
    | Some ev ->
      ch.event_readable <- None;
      Lwt_engine.stop_event ev
    | None ->
      ()
  end;
  begin
    match ch.event_writable with
    | Some ev ->
      ch.event_writable <- None;
      Lwt_engine.stop_event ev
    | None ->
      ()
  end

let abort ch e =
  Lwt_dls.check_owner "Lwt_unix.abort" ch.owner;
  if ch.state <> Closed then begin
    set_state ch (Aborted e);
    clear_events ch
  end

let unix_file_descr ch = ch.fd

let fd_kind ch =
  match ch.io_kind with
  | Some k -> k
  | None ->
    (* Default to [S_CHR] (a non-seekable, non-socket kind) if [fstat] fails, so
       a backend falls back to the safe offset-0 read/write path. *)
    let k = try (Unix.fstat ch.fd).Unix.st_kind with _ -> Unix.S_CHR in
    ch.io_kind <- Some k;
    k

let of_unix_file_descr = mk_ch

let stdin = of_unix_file_descr ~set_flags:false ~blocking:true Unix.stdin
let stdout = of_unix_file_descr ~set_flags:false ~blocking:true Unix.stdout
let stderr = of_unix_file_descr ~set_flags:false ~blocking:true Unix.stderr

(* +-----------------------------------------------------------------+
   | Actions on file descriptors                                     |
   +-----------------------------------------------------------------+ *)

type io_event = Read | Write

exception Retry
exception Retry_write
exception Retry_read

type 'a outcome =
  | Success of 'a
  | Exn of exn
  | Requeued of io_event

(* Wait a bit, then stop events that are no more used. *)
let stop_events ch =
  Lwt.on_success
    (Lwt.pause ())
    (fun () ->
       if Lwt_sequence.is_empty ch.hooks_readable then begin
         match ch.event_readable with
         | Some ev ->
           ch.event_readable <- None;
           Lwt_engine.stop_event ev
         | None ->
           ()
       end;
       if Lwt_sequence.is_empty ch.hooks_writable then begin
         match ch.event_writable with
         | Some ev ->
           ch.event_writable <- None;
           Lwt_engine.stop_event ev
         | None ->
           ()
       end)

let register_readable ch =
  if ch.event_readable = None then
    ch.event_readable <- Some(Lwt_engine.on_readable ch.fd (fun _ -> Lwt_sequence.iter_l (fun f -> f ()) ch.hooks_readable))

let register_writable ch =
  if ch.event_writable = None then
    ch.event_writable <- Some(Lwt_engine.on_writable ch.fd (fun _ -> Lwt_sequence.iter_l (fun f -> f ()) ch.hooks_writable))

(* Retry a queued syscall, [wakener] is the thread to wakeup if the
   action succeeds: *)
let rec retry_syscall node event ch wakener action =
  let res =
    try
      check_descriptor ch;
      Success(action ())
    with
    | Retry
    | Unix.Unix_error((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _)
    | Sys_blocked_io ->
      (* EINTR because we are catching SIG_CHLD hence the system
         call might be interrupted to handle the signal; this lets
         us restart the system call eventually. *)
      Requeued event
    | Retry_read ->
      Requeued Read
    | Retry_write ->
      Requeued Write
    | e when Lwt.Exception_filter.run e ->
      Exn e
  in
  match res with
  | Success v ->
    Lwt_sequence.remove !node;
    stop_events ch;
    Lwt.wakeup wakener v
  | Exn e ->
    Lwt_sequence.remove !node;
    stop_events ch;
    Lwt.wakeup_exn wakener e
  | Requeued event' ->
    if event <> event' then begin
      Lwt_sequence.remove !node;
      stop_events ch;
      match event' with
      | Read ->
        node := Lwt_sequence.add_r (fun () -> retry_syscall node Read ch wakener action) ch.hooks_readable ;
        register_readable ch
      | Write ->
        node := Lwt_sequence.add_r (fun () -> retry_syscall node Write ch wakener action) ch.hooks_writable;
        register_writable ch
    end

let dummy = Lwt_sequence.add_r ignore (Lwt_sequence.create ())

let register_action event ch action =
  let waiter, wakener = Lwt.task () in
  match event with
  | Read ->
    let node = ref dummy in
    node := Lwt_sequence.add_r (fun () -> retry_syscall node Read ch wakener action) ch.hooks_readable;
    Lwt.on_cancel waiter (fun () -> Lwt_sequence.remove !node; stop_events ch);
    register_readable ch;
    waiter
  | Write ->
    let node = ref dummy in
    node := Lwt_sequence.add_r (fun () -> retry_syscall node Write ch wakener action) ch.hooks_writable;
    Lwt.on_cancel waiter (fun () -> Lwt_sequence.remove !node; stop_events ch);
    register_writable ch;
    waiter

(* Wraps a system call *)
let wrap_syscall event ch action =
  check_descriptor ch;
  Lazy.force ch.blocking >>= fun blocking ->
  try
    if not blocking || (event = Read && unix_readable ch.fd) || (event = Write && unix_writable ch.fd) then
      Lwt.return (action ())
    else
      register_action event ch action
  with
  | Retry
  | Unix.Unix_error((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _)
  | Sys_blocked_io ->
    (* The action could not be completed immediately, register it: *)
    register_action event ch action
  | Retry_read ->
    register_action Read ch action
  | Retry_write ->
    register_action Write ch action
  | e when Lwt.Exception_filter.run e ->
    Lwt.reraise e

(* +-----------------------------------------------------------------+
   | Basic file input/output                                         |
   +-----------------------------------------------------------------+ *)

type open_flag =
  Unix.open_flag =
  | O_RDONLY
  | O_WRONLY
  | O_RDWR
  | O_NONBLOCK
  | O_APPEND
  | O_CREAT
  | O_TRUNC
  | O_EXCL
  | O_NOCTTY
  | O_DSYNC
  | O_SYNC
  | O_RSYNC
  | O_SHARE_DELETE
  | O_CLOEXEC
  | O_KEEPEXEC

external open_job : string -> Unix.open_flag list -> int -> (Unix.file_descr * bool) job = "lwt_unix_open_job"

let openfile name flags perms =
  if Sys.win32 then
    Lwt.return (of_unix_file_descr (Unix.openfile name flags perms))
  else
    run_job (open_job name flags perms) >>= fun (fd, blocking) ->
    Lwt.return (of_unix_file_descr ~blocking fd)

external close_job : Unix.file_descr -> unit job = "lwt_unix_close_job"

(* Notification to the completion-based I/O backend (see [completion_io]
   below) that a descriptor is being closed, so it can release per-descriptor
   state — e.g. cancel an armed multishot accept, which would otherwise keep a
   kernel reference to the socket alive and shadow a later descriptor with the
   same number. Synced by [set_completion_io].

   The close itself deliberately stays on the worker-pool job path: routing it
   through the ring was tried and measured SLOWER (the pool performs close(2)
   on another core, in parallel with the event loop, while a ring submission
   consumes main-loop budget and a completion slot per close). *)
let completion_close_hook : (Unix.file_descr -> unit) ref = ref ignore

let close ch =
  Lwt_dls.check_owner "Lwt_unix.close" ch.owner;
  if ch.state = Closed then check_descriptor ch;
  set_state ch Closed;
  clear_events ch;
  !completion_close_hook ch.fd;
  if Sys.win32 then
    Lwt.return (Unix.close ch.fd)
  else
    run_job (close_job ch.fd)

type bigarray =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

(* +-----------------------------------------------------------------+
   | Completion-based I/O backend (e.g. io_uring)                    |
   +-----------------------------------------------------------------+ *)

(* An optional completion-based I/O backend, installed by a library such as
   [lwt_uring]. When present, the basic read/write operations (including the
   bigarray ones used by [Lwt_io]) consult it before falling back to the default
   readiness/job path. Each function returns [Some promise] to take over the
   operation, or [None] to decline (e.g. when its engine is not currently
   installed), in which case the default path runs. This lets completion-based
   engines transparently speed up existing code. *)
type completion_io = {
  read : file_descr -> bytes -> int -> int -> int Lwt.t option;
  write : file_descr -> bytes -> int -> int -> int Lwt.t option;
  read_bigarray : file_descr -> bigarray -> int -> int -> int Lwt.t option;
  write_bigarray : file_descr -> bigarray -> int -> int -> int Lwt.t option;
  connect : file_descr -> Unix.sockaddr -> unit Lwt.t option;
  accept : file_descr -> (Unix.file_descr * Unix.sockaddr) Lwt.t option;
  (* The back end accepts on the given listening descriptor and returns the
     accepted descriptor — already non-blocking and without close-on-exec
     (Lwt's defaults) — with the peer address. [None] declines (back end not
     available, or unsupported); the default readiness path is used. *)
  on_close : Unix.file_descr -> unit;
  (* Called when Lwt_unix closes a descriptor: release any per-descriptor
     backend state (cancel an armed multishot accept, drop queued accepted
     connections). *)
}

let completion_io : completion_io option ref = ref None

let set_completion_io backend =
  completion_io := backend;
  completion_close_hook :=
    (match backend with Some io -> io.on_close | None -> ignore)

let wait_read ch =
  Lwt.catch
    (fun () ->
       if readable ch then
         Lwt.return_unit
       else
         register_action Read ch ignore)
    Lwt.reraise

external stub_read : Unix.file_descr -> Bytes.t -> int -> int -> int = "lwt_unix_read"
external read_job : Unix.file_descr -> Bytes.t -> int -> int -> int job = "lwt_unix_read_job"
external stub_pread :
  Unix.file_descr -> Bytes.t -> file_offset:int -> int -> int -> int =
    "lwt_unix_pread"
external pread_job :
  Unix.file_descr -> Bytes.t -> file_offset:int -> int -> int -> int job =
    "lwt_unix_pread_job"

let read ch buf pos len =
  if pos < 0 || len < 0 || pos > Bytes.length buf - len then
    invalid_arg "Lwt_unix.read"
  else
    let default () =
      Lazy.force ch.blocking >>= function
      | true ->
        wait_read ch >>= fun () ->
        run_job (read_job ch.fd buf pos len)
      | false ->
        wrap_syscall Read ch (fun () -> stub_read ch.fd buf pos len)
    in
    (* Only route an open descriptor to the completion backend; for a closed or
       aborted one the default path raises the expected exception. *)
    match ch.state, !completion_io with
    | Opened, Some io ->
      (match io.read ch buf pos len with Some p -> p | None -> default ())
    | _ -> default ()

let pread ch buf ~file_offset pos len =
  if pos < 0 || len < 0 || pos > Bytes.length buf - len then
    invalid_arg "Lwt_unix.pread"
  else
    Lazy.force ch.blocking >>= function
    | true ->
      wait_read ch >>= fun () ->
      run_job (pread_job ch.fd buf ~file_offset pos len)
    | false ->
      wrap_syscall Read ch (fun () -> stub_pread ch.fd buf ~file_offset pos len)

external stub_read_bigarray :
  Unix.file_descr -> bigarray -> int -> int -> int = "lwt_unix_bytes_read"
external read_bigarray_job :
  Unix.file_descr -> bigarray -> int -> int -> int job =
  "lwt_unix_bytes_read_job"

let read_bigarray function_name fd buf pos len =
  if pos < 0 || len < 0 || pos > Bigarray.Array1.dim buf - len then
    invalid_arg function_name
  else
    let default () =
      blocking fd >>= function
      | true ->
        wait_read fd >>= fun () ->
        run_job (read_bigarray_job (unix_file_descr fd) buf pos len)
      | false ->
        wrap_syscall Read fd (fun () ->
          stub_read_bigarray (unix_file_descr fd) buf pos len)
    in
    match fd.state, !completion_io with
    | Opened, Some io ->
      (match io.read_bigarray fd buf pos len with Some p -> p | None -> default ())
    | _ -> default ()

let wait_write ch =
  Lwt.catch
    (fun () ->
       if writable ch then
         Lwt.return_unit
       else
         register_action Write ch ignore)
    Lwt.reraise

external stub_write : Unix.file_descr -> Bytes.t -> int -> int -> int = "lwt_unix_write"
external write_job : Unix.file_descr -> Bytes.t -> int -> int -> int job = "lwt_unix_write_job"
external stub_pwrite :
  Unix.file_descr -> Bytes.t -> file_offset:int -> int -> int -> int =
    "lwt_unix_pwrite"
external pwrite_job :
  Unix.file_descr -> Bytes.t -> file_offset:int -> int -> int -> int job =
    "lwt_unix_pwrite_job"

let write ch buf pos len =
  if pos < 0 || len < 0 || pos > Bytes.length buf - len then
    invalid_arg "Lwt_unix.write"
  else
    let default () =
      Lazy.force ch.blocking >>= function
      | true ->
        wait_write ch >>= fun () ->
        run_job (write_job ch.fd buf pos len)
      | false ->
        wrap_syscall Write ch (fun () -> stub_write ch.fd buf pos len)
    in
    (* Only route an open descriptor to the completion backend; for a closed or
       aborted one the default path raises the expected exception. *)
    match ch.state, !completion_io with
    | Opened, Some io ->
      (match io.write ch buf pos len with Some p -> p | None -> default ())
    | _ -> default ()

let pwrite ch buf ~file_offset pos len =
  if pos < 0 || len < 0 || pos > Bytes.length buf - len then
    invalid_arg "Lwt_unix.pwrite"
  else
    Lazy.force ch.blocking >>= function
    | true ->
      wait_write ch >>= fun () ->
      run_job (pwrite_job ch.fd buf ~file_offset pos len)
    | false ->
      wrap_syscall Write ch (fun () -> stub_pwrite ch.fd buf ~file_offset pos len)

let write_string ch buf pos len =
  let buf = Bytes.unsafe_of_string buf in
  write ch buf pos len

let pwrite_string ch buf ~file_offset pos len =
  let buf = Bytes.unsafe_of_string buf in
  pwrite ch buf ~file_offset pos len

external stub_write_bigarray :
  Unix.file_descr -> bigarray -> int -> int -> int = "lwt_unix_bytes_write"
external write_bigarray_job :
  Unix.file_descr -> bigarray -> int -> int -> int job =
  "lwt_unix_bytes_write_job"

let write_bigarray function_name fd buf pos len =
  if pos < 0 || len < 0 || pos > Bigarray.Array1.dim buf - len then
    invalid_arg function_name
  else
    let default () =
      blocking fd >>= function
      | true ->
        wait_write fd >>= fun () ->
        run_job (write_bigarray_job (unix_file_descr fd) buf pos len)
      | false ->
        wrap_syscall Write fd (fun () ->
          stub_write_bigarray (unix_file_descr fd) buf pos len)
    in
    match fd.state, !completion_io with
    | Opened, Some io ->
      (match io.write_bigarray fd buf pos len with
       | Some p -> p
       | None -> default ())
    | _ -> default ()

module IO_vectors =
struct
  type _bigarray = bigarray

  type buffer =
    | Bytes of bytes
    | Bigarray of _bigarray

  type io_vector =
    {buffer : buffer;
     mutable offset : int;
     mutable length : int}

  (* This representation does not give constant amortized time append across all
     possible operation sequences, but it does for expected typical usage, in
     which some number of append operations is followed by some number of
     flatten operations. *)
  type t =
    {mutable prefix : io_vector list;
     mutable reversed_suffix : io_vector list;
     mutable count : int}

  let create () = {prefix = []; reversed_suffix = []; count = 0}

  let byte_count {prefix; reversed_suffix; _} =
    let count_buff = List.fold_left (fun acc {length; _} -> acc + length) 0 in
    count_buff prefix + count_buff reversed_suffix

  let append io_vectors io_vector =
    io_vectors.reversed_suffix <- io_vector::io_vectors.reversed_suffix;
    io_vectors.count <- io_vectors.count + 1

  let append_bytes io_vectors buffer offset length =
    append io_vectors {buffer = Bytes buffer; offset; length}

  let append_bigarray io_vectors buffer offset length =
    append io_vectors {buffer = Bigarray buffer; offset; length}

  let flatten io_vectors =
    match io_vectors.reversed_suffix with
    | [] -> ()
    | _ ->
      io_vectors.prefix <-
        io_vectors.prefix @ (List.rev io_vectors.reversed_suffix);
      io_vectors.reversed_suffix <- []

  let drop io_vectors count =
    flatten io_vectors;
    let rec loop count prefix =
      if count <= 0 then prefix
      else
        match prefix with
        | [] -> []
        | {length; _}::rest when length <= count ->
          io_vectors.count <- io_vectors.count - 1;
          loop (count - length) rest
        | first::_ ->
          first.offset <- first.offset + count;
          first.length <- first.length - count;
          prefix
    in
    io_vectors.prefix <- loop count io_vectors.prefix

  let is_empty io_vectors =
    flatten io_vectors;
    let rec loop = function
      | [] -> true
      | {length = 0; _}::rest -> loop rest
      | _ -> false
    in
    loop io_vectors.prefix

  external stub_iov_max : unit -> int option = "lwt_unix_iov_max"

  let system_limit =
    if Sys.win32 then None
    else stub_iov_max ()

  let check tag io_vector =
    let buffer_length =
      match io_vector.buffer with
      | Bytes s -> Bytes.length s
      | Bigarray a -> Bigarray.Array1.dim a
    in

    if io_vector.length < 0 ||
       io_vector.offset < 0 ||
       io_vector.offset + io_vector.length > buffer_length then
      invalid_arg tag
end

(* Flattens the I/O vectors into a single list, checks their bounds, and
   evaluates to the minimum of: the number of vectors and the system's
   IOV_MAX. *)
let check_io_vectors function_name io_vectors =
  IO_vectors.flatten io_vectors;
  List.iter (IO_vectors.check function_name) io_vectors.IO_vectors.prefix;

  match IO_vectors.system_limit with
  | Some limit when io_vectors.IO_vectors.count > limit -> limit
  | _ -> io_vectors.IO_vectors.count

external stub_readv :
  Unix.file_descr -> IO_vectors.io_vector list -> int -> int =
  "lwt_unix_readv"

external readv_job : Unix.file_descr -> IO_vectors.t -> int -> int job =
  "lwt_unix_readv_job"

let readv fd io_vectors =
  let count = check_io_vectors "Lwt_unix.readv" io_vectors in

  if Sys.win32 then
    match io_vectors.IO_vectors.prefix with
    | [] ->
      Lwt.return 0
    | first::_ ->
      match first.buffer with
      | Bytes buffer ->
        read fd buffer first.offset first.length
      | Bigarray buffer ->
        read_bigarray "Lwt_unix.readv" fd buffer first.offset first.length

  else
    Lazy.force fd.blocking >>= function
    | true ->
      wait_read fd >>= fun () ->
      run_job (readv_job fd.fd io_vectors count)
    | false ->
      wrap_syscall Read fd (fun () ->
        stub_readv fd.fd io_vectors.IO_vectors.prefix count)

external stub_writev :
  Unix.file_descr -> IO_vectors.io_vector list -> int -> int =
  "lwt_unix_writev"

external writev_job : Unix.file_descr -> IO_vectors.t -> int -> int job =
  "lwt_unix_writev_job"

let writev fd io_vectors =
  let count = check_io_vectors "Lwt_unix.writev" io_vectors in

  if Sys.win32 then
    match io_vectors.IO_vectors.prefix with
    | [] ->
      Lwt.return 0
    | first::_ ->
      match first.buffer with
      | Bytes buffer ->
        write fd buffer first.offset first.length
      | Bigarray buffer ->
        write_bigarray "Lwt_unix.writev" fd buffer first.offset first.length

  else
    Lazy.force fd.blocking >>= function
    | true ->
      wait_write fd >>= fun () ->
      run_job (writev_job fd.fd io_vectors count)
    | false ->
      wrap_syscall Write fd (fun () ->
        stub_writev fd.fd io_vectors.IO_vectors.prefix count)

(* +-----------------------------------------------------------------+
   | Seeking and truncating                                          |
   +-----------------------------------------------------------------+ *)

type seek_command =
  Unix.seek_command =
  | SEEK_SET
  | SEEK_CUR
  | SEEK_END

external lseek_job :
  Unix.file_descr -> int -> Unix.seek_command -> int job = "lwt_unix_lseek_job"

let lseek ch offset whence =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.lseek ch.fd offset whence)
  else
    run_job (lseek_job ch.fd offset whence)

external truncate_job : string -> int -> unit job = "lwt_unix_truncate_job"

let truncate name offset =
  if Sys.win32 then
    Lwt.return (Unix.truncate name offset)
  else
    run_job (truncate_job name offset)

external ftruncate_job :
  Unix.file_descr -> int -> unit job = "lwt_unix_ftruncate_job"

let ftruncate ch offset =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.ftruncate ch.fd offset)
  else
    run_job (ftruncate_job ch.fd offset)

(* +-----------------------------------------------------------------+
   | File system synchronisation                                     |
   +-----------------------------------------------------------------+ *)

external fdatasync_job : Unix.file_descr -> unit job = "lwt_unix_fdatasync_job"

let fdatasync ch =
  check_descriptor ch;
  run_job (fdatasync_job ch.fd)

external fsync_job : Unix.file_descr -> unit job = "lwt_unix_fsync_job"

let fsync ch =
  check_descriptor ch;
  run_job (fsync_job ch.fd)

(* +-----------------------------------------------------------------+
   | File status                                                     |
   +-----------------------------------------------------------------+ *)

type file_perm = Unix.file_perm

type file_kind =
  Unix.file_kind =
  | S_REG
  | S_DIR
  | S_CHR
  | S_BLK
  | S_LNK
  | S_FIFO
  | S_SOCK

type stats =
  Unix.stats =
  {
    st_dev : int;
    st_ino : int;
    st_kind : file_kind;
    st_perm : file_perm;
    st_nlink : int;
    st_uid : int;
    st_gid : int;
    st_rdev : int;
    st_size : int;
    st_atime : float;
    st_mtime : float;
    st_ctime : float;
  }

external stat_job : string -> Unix.stats job = "lwt_unix_stat_job"

let stat name =
  if Sys.win32 then
    Lwt.return (Unix.stat name)
  else
    run_job (stat_job name)

external lstat_job : string -> Unix.stats job = "lwt_unix_lstat_job"

let lstat name =
  if Sys.win32 then
    Lwt.return (Unix.lstat name)
  else
    run_job (lstat_job name)

external fstat_job : Unix.file_descr -> Unix.stats job = "lwt_unix_fstat_job"

let fstat ch =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.fstat ch.fd)
  else
    run_job (fstat_job ch.fd)

let file_exists name =
  Lwt.try_bind
    (fun () -> stat name)
    (fun _ -> Lwt.return_true)
    (fun e ->
       match e with
       | Unix.Unix_error _ -> Lwt.return_false
       | _ -> Lwt.reraise e)

external utimes_job : string -> float -> float -> unit job =
  "lwt_unix_utimes_job"

let utimes path atime mtime =
  if Sys.win32 then
    Lwt.return (Unix.utimes path atime mtime)
  else
    run_job (utimes_job path atime mtime)

external isatty_job : Unix.file_descr -> bool job = "lwt_unix_isatty_job"

let isatty ch =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.isatty ch.fd)
  else
    run_job (isatty_job ch.fd)

(* +-----------------------------------------------------------------+
   | File operations on large files                                  |
   +-----------------------------------------------------------------+ *)

module LargeFile =
struct

  type stats =
    Unix.LargeFile.stats =
    {
      st_dev : int;
      st_ino : int;
      st_kind : file_kind;
      st_perm : file_perm;
      st_nlink : int;
      st_uid : int;
      st_gid : int;
      st_rdev : int;
      st_size : int64;
      st_atime : float;
      st_mtime : float;
      st_ctime : float;
    }

  external lseek_64_job :
    Unix.file_descr -> int64 -> Unix.seek_command -> int64 job =
    "lwt_unix_lseek_64_job"

  let lseek ch offset whence =
    check_descriptor ch;
    if Sys.win32 then
      Lwt.return (Unix.LargeFile.lseek ch.fd offset whence)
    else
      run_job (lseek_64_job ch.fd offset whence)

  external truncate_64_job :
    string -> int64 -> unit job = "lwt_unix_truncate_64_job"

  let truncate name offset =
    if Sys.win32 then
      Lwt.return (Unix.LargeFile.truncate name offset)
    else
      run_job (truncate_64_job name offset)

  external ftruncate_64_job :
    Unix.file_descr -> int64 -> unit job = "lwt_unix_ftruncate_64_job"

  let ftruncate ch offset =
    check_descriptor ch;
    if Sys.win32 then
      Lwt.return (Unix.LargeFile.ftruncate ch.fd offset)
    else
      run_job (ftruncate_64_job ch.fd offset)

  external stat_job : string -> Unix.LargeFile.stats job = "lwt_unix_stat_64_job"

  let stat name =
    if Sys.win32 then
      Lwt.return (Unix.LargeFile.stat name)
    else
      run_job (stat_job name)

  external lstat_job : string -> Unix.LargeFile.stats job = "lwt_unix_lstat_64_job"

  let lstat name =
    if Sys.win32 then
      Lwt.return (Unix.LargeFile.lstat name)
    else
      run_job (lstat_job name)

  external fstat_job : Unix.file_descr -> Unix.LargeFile.stats job = "lwt_unix_fstat_64_job"

  let fstat ch =
    check_descriptor ch;
    if Sys.win32 then
      Lwt.return (Unix.LargeFile.fstat ch.fd)
    else
      run_job (fstat_job ch.fd)

  let file_exists name =
    Lwt.try_bind
      (fun () -> stat name)
      (fun _ -> Lwt.return_true)
      (fun e ->
         match e with
         | Unix.Unix_error _ -> Lwt.return_false
         | _ -> Lwt.reraise e)

end

(* +-----------------------------------------------------------------+
   | Operations on file names                                        |
   +-----------------------------------------------------------------+ *)

external unlink_job : string -> unit job = "lwt_unix_unlink_job"

let unlink name =
  if Sys.win32 then
    Lwt.return (Unix.unlink name)
  else
    run_job (unlink_job name)

external rename_job : string -> string -> unit job = "lwt_unix_rename_job"

let rename name1 name2 =
  if Sys.win32 then
    Lwt.return (Unix.rename name1 name2)
  else
    run_job (rename_job name1 name2)

external link_job : string -> string -> unit job = "lwt_unix_link_job"

let link oldpath newpath =
  if Sys.win32 then
    Lwt.return (Unix.link oldpath newpath)
  else
    run_job (link_job oldpath newpath)

(* +-----------------------------------------------------------------+
   | File permissions and ownership                                  |
   +-----------------------------------------------------------------+ *)

external chmod_job : string -> int -> unit job = "lwt_unix_chmod_job"

let chmod name mode =
  if Sys.win32 then
    Lwt.return (Unix.chmod name mode)
  else
    run_job (chmod_job name mode)

external fchmod_job : Unix.file_descr -> int -> unit job = "lwt_unix_fchmod_job"

let fchmod ch mode =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.fchmod ch.fd mode)
  else
    run_job (fchmod_job ch.fd mode)

external chown_job : string -> int -> int -> unit job = "lwt_unix_chown_job"

let chown name uid gid =
  if Sys.win32 then
    Lwt.return (Unix.chown name uid gid)
  else
    run_job (chown_job name uid gid)

external fchown_job :
  Unix.file_descr -> int -> int -> unit job = "lwt_unix_fchown_job"

let fchown ch uid gid =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.fchown ch.fd uid gid)
  else
    run_job (fchown_job ch.fd uid gid)

type access_permission =
  Unix.access_permission =
  | R_OK
  | W_OK
  | X_OK
  | F_OK

external access_job :
  string -> Unix.access_permission list -> unit job = "lwt_unix_access_job"

let access name mode =
  if Sys.win32 then
    Lwt.return (Unix.access name mode)
  else
    run_job (access_job name mode)

(* +-----------------------------------------------------------------+
   | Operations on file descriptors                                  |
   +-----------------------------------------------------------------+ *)

let dup ?cloexec ch =
  check_descriptor ch;
  let fd = Unix.dup ?cloexec ch.fd in
  {
    fd = fd;
    state = Opened;
    set_flags = ch.set_flags;
    blocking =
      if ch.set_flags then
        lazy(Lazy.force ch.blocking >>= function blocking ->
               Lazy.force (is_blocking ~blocking fd))
      else
        ch.blocking;
    event_readable = None;
    event_writable = None;
    hooks_readable = Lwt_sequence.create ();
    hooks_writable = Lwt_sequence.create ();
    io_kind = None;
    (* [check_descriptor] above already established that we own [ch], so the
       duplicate is ours too. *)
    owner = ch.owner;
  }

let dup2 ?cloexec ch1 ch2 =
  check_descriptor ch1;
  Unix.dup2 ?cloexec ch1.fd ch2.fd;
  ch2.set_flags <- ch1.set_flags;
  ch2.blocking <- (
    if ch2.set_flags then
      lazy(Lazy.force ch1.blocking >>= function blocking ->
             Lazy.force (is_blocking ~blocking ch2.fd))
    else
      ch1.blocking
  )

let set_close_on_exec ch =
  check_descriptor ch;
  Unix.set_close_on_exec ch.fd

let clear_close_on_exec ch =
  check_descriptor ch;
  Unix.clear_close_on_exec ch.fd

(* +-----------------------------------------------------------------+
   | Directories                                                     |
   +-----------------------------------------------------------------+ *)

external mkdir_job : string -> int -> unit job = "lwt_unix_mkdir_job"

let mkdir name perms =
  if Sys.win32 then
    Lwt.return (Unix.mkdir name perms)
  else
    run_job (mkdir_job name perms)

external rmdir_job : string -> unit job = "lwt_unix_rmdir_job"

let rmdir name =
  if Sys.win32 then
    Lwt.return (Unix.rmdir name)
  else
    run_job (rmdir_job name)

external chdir_job : string -> unit job = "lwt_unix_chdir_job"

let chdir name =
  if Sys.win32 then
    Lwt.return (Unix.chdir name)
  else
    run_job (chdir_job name)

external getcwd_job : unit -> string job = "lwt_unix_getcwd_job"

let getcwd () =
  if Sys.win32 then
    Lwt.return (Unix.getcwd ())
  else
    run_job (getcwd_job ())

external chroot_job : string -> unit job = "lwt_unix_chroot_job"

let chroot name =
  if Sys.win32 then
    Lwt.return (Unix.chroot name)
  else
    run_job (chroot_job name)

type dir_handle = Unix.dir_handle

external opendir_job : string -> Unix.dir_handle job = "lwt_unix_opendir_job"

let opendir name =
  if Sys.win32 then
    Lwt.return (Unix.opendir name)
  else
    run_job (opendir_job name)

external valid_dir : Unix.dir_handle -> bool = "lwt_unix_valid_dir"
external readdir_job : Unix.dir_handle -> string job = "lwt_unix_readdir_job"

let readdir handle =
  if Sys.win32 then
    Lwt.return (Unix.readdir handle)
  else
  if valid_dir handle then
    run_job (readdir_job handle)
  else
    Lwt.fail (Unix.(Unix_error (EBADF, "Lwt_unix.readdir", "")))

external readdir_n_job : Unix.dir_handle -> int -> string array job = "lwt_unix_readdir_n_job"

let readdir_n handle count =
  if count < 0 then
    Lwt.fail (Invalid_argument "Lwt_unix.readdir_n")
  else if Sys.win32 then
    let array = Array.make count "" in
    let rec fill i =
      if i = count then
        Lwt.return array
      else
        match array.(i) <- Unix.readdir handle with
        | exception End_of_file -> Lwt.return (Array.sub array 0 i)
        | () -> fill (i + 1)
    in
    fill 0
  else
  if valid_dir handle then
    run_job (readdir_n_job handle count)
  else
    Lwt.fail (Unix.(Unix_error (EBADF, "Lwt_unix.readdir_n", "")))

external rewinddir_job : Unix.dir_handle -> unit job = "lwt_unix_rewinddir_job"

let rewinddir handle =
  if Sys.win32 then
    Lwt.return (Unix.rewinddir handle)
  else
  if valid_dir handle then
    run_job (rewinddir_job handle)
  else
    Lwt.fail (Unix.(Unix_error (EBADF, "Lwt_unix.rewinddir", "")))

external closedir_job : Unix.dir_handle -> unit job = "lwt_unix_closedir_job"
external invalidate_dir : Unix.dir_handle -> unit = "lwt_unix_invalidate_dir"

let closedir handle =
  if Sys.win32 then
    Lwt.return (Unix.closedir handle)
  else
  if valid_dir handle then
    run_job (closedir_job handle) >>= fun () ->
    invalidate_dir handle;
    Lwt.return_unit
  else
    Lwt.fail (Unix.(Unix_error (EBADF, "Lwt_unix.closedir", "")))

type list_directory_state  =
  | LDS_not_started
  | LDS_listing of Unix.dir_handle
  | LDS_done

let cleanup_dir_handle state =
  match !state with
  | LDS_listing handle ->
    ignore (closedir handle)
  | LDS_not_started | LDS_done ->
    ()

let files_of_directory path =
  let chunk_size = 1024 in
  let state = ref LDS_not_started in
  Lwt_stream.concat
    (Lwt_stream.from
       (fun () ->
          match !state with
          | LDS_not_started ->
            opendir path >>= fun handle ->
            Lwt.catch
              (fun () -> readdir_n handle chunk_size)
              (fun exn ->
                 closedir handle >>= fun () ->
                 Lwt.reraise exn) >>= fun entries ->
            if Array.length entries < chunk_size then begin
              state := LDS_done;
              closedir handle >>= fun () ->
              Lwt.return (Some(Lwt_stream.of_array entries))
            end else begin
              state := LDS_listing handle;
              Gc.finalise cleanup_dir_handle state;
              Lwt.return (Some(Lwt_stream.of_array entries))
            end
          | LDS_listing handle ->
            Lwt.catch
              (fun () -> readdir_n handle chunk_size)
              (fun exn ->
                 closedir handle >>= fun () ->
                 Lwt.reraise exn) >>= fun entries ->
            if Array.length entries < chunk_size then begin
              state := LDS_done;
              closedir handle >>= fun () ->
              Lwt.return (Some(Lwt_stream.of_array entries))
            end else
              Lwt.return (Some(Lwt_stream.of_array entries))
          | LDS_done ->
            Lwt.return_none))

(* +-----------------------------------------------------------------+
   | Pipes and redirections                                          |
   +-----------------------------------------------------------------+ *)

let pipe ?cloexec () =
  let (out_fd, in_fd) = Unix.pipe ?cloexec () in
  (mk_ch ~blocking:Sys.win32 out_fd, mk_ch ~blocking:Sys.win32 in_fd)

let pipe_in ?cloexec () =
   let (out_fd, in_fd) = Unix.pipe ?cloexec () in
  (mk_ch ~blocking:Sys.win32 out_fd, in_fd)

let pipe_out ?cloexec () =
  let (out_fd, in_fd) = Unix.pipe ?cloexec () in
  (out_fd, mk_ch ~blocking:Sys.win32 in_fd)

external mkfifo_job : string -> int -> unit job = "lwt_unix_mkfifo_job"

let mkfifo name perms =
  if Sys.win32 then
    Lwt.return (Unix.mkfifo name perms)
  else
    run_job (mkfifo_job name perms)

(* +-----------------------------------------------------------------+
   | Symbolic links                                                  |
   +-----------------------------------------------------------------+ *)

external symlink_job : string -> string -> unit job = "lwt_unix_symlink_job"

let symlink ?to_dir name1 name2 =
  if Sys.win32 then
    Lwt.return (Unix.symlink ?to_dir name1 name2)
  else
    run_job (symlink_job name1 name2)

external readlink_job : string -> string job = "lwt_unix_readlink_job"

let readlink name =
  if Sys.win32 then
    Lwt.return (Unix.readlink name)
  else
    run_job (readlink_job name)

(* +-----------------------------------------------------------------+
   | Locking                                                         |
   +-----------------------------------------------------------------+ *)

type lock_command =
  Unix.lock_command =
  | F_ULOCK
  | F_LOCK
  | F_TLOCK
  | F_TEST
  | F_RLOCK
  | F_TRLOCK

external lockf_job : Unix.file_descr -> Unix.lock_command -> int -> unit job = "lwt_unix_lockf_job"

let lockf ch cmd size =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.lockf ch.fd cmd size)
  else
    run_job (lockf_job ch.fd cmd size)

(* +-----------------------------------------------------------------+
   | User id, group id                                               |
   +-----------------------------------------------------------------+ *)

type passwd_entry =
  Unix.passwd_entry =
  {
    pw_name : string;
    pw_passwd : string;
    pw_uid : int;
    pw_gid : int;
    pw_gecos : string;
    pw_dir : string;
    pw_shell : string
  }

type group_entry =
  Unix.group_entry =
  {
    gr_name : string;
    gr_passwd : string;
    gr_gid : int;
    gr_mem : string array
  }

external getlogin_job : unit -> string job = "lwt_unix_getlogin_job"

let getlogin () =
  if Sys.win32 || Lwt_config.android then
    Lwt.return (Unix.getlogin ())
  else
    run_job (getlogin_job ())

external getpwnam_job : string -> Unix.passwd_entry job = "lwt_unix_getpwnam_job"

let getpwnam name =
  if Sys.win32 || Lwt_config.android then
    Lwt.return (Unix.getpwnam name)
  else
    run_job (getpwnam_job name)

external getgrnam_job : string -> Unix.group_entry job = "lwt_unix_getgrnam_job"

let getgrnam name =
  if Sys.win32 || Lwt_config.android then
    Lwt.return (Unix.getgrnam name)
  else
    run_job (getgrnam_job name)

external getpwuid_job : int -> Unix.passwd_entry job = "lwt_unix_getpwuid_job"

let getpwuid uid =
  if Sys.win32 || Lwt_config.android then
    Lwt.return (Unix.getpwuid uid)
  else
    run_job (getpwuid_job uid)

external getgrgid_job : int -> Unix.group_entry job = "lwt_unix_getgrgid_job"

let getgrgid gid =
  if Sys.win32 || Lwt_config.android then
    Lwt.return (Unix.getgrgid gid)
  else
    run_job (getgrgid_job gid)

(* +-----------------------------------------------------------------+
   | Sockets                                                         |
   +-----------------------------------------------------------------+ *)

type msg_flag =
  Unix.msg_flag =
  | MSG_OOB
  | MSG_DONTROUTE
  | MSG_PEEK

external stub_recv : Unix.file_descr -> Bytes.t -> int -> int -> Unix.msg_flag list -> int = "lwt_unix_recv"

let recv ch buf pos len flags =
  if pos < 0 || len < 0 || pos > Bytes.length buf - len then
    invalid_arg "Lwt_unix.recv"
  else
    let do_recv = if Sys.win32 then Unix.recv else stub_recv in
    wrap_syscall Read ch (fun () -> do_recv ch.fd buf pos len flags)

external stub_send : Unix.file_descr -> Bytes.t -> int -> int -> Unix.msg_flag list -> int = "lwt_unix_send"

let send ch buf pos len flags =
  if pos < 0 || len < 0 || pos > Bytes.length buf - len then
    invalid_arg "Lwt_unix.send"
  else
    let do_send = if Sys.win32 then Unix.send else stub_send in
    wrap_syscall Write ch (fun () -> do_send ch.fd buf pos len flags)

external stub_recvfrom : Unix.file_descr -> Bytes.t -> int -> int -> Unix.msg_flag list -> int * Unix.sockaddr = "lwt_unix_recvfrom"

let recvfrom ch buf pos len flags =
  if pos < 0 || len < 0 || pos > Bytes.length buf - len then
    invalid_arg "Lwt_unix.recvfrom"
  else
    let do_recvfrom = if Sys.win32 then Unix.recvfrom else stub_recvfrom in
    wrap_syscall Read ch (fun () -> do_recvfrom ch.fd buf pos len flags)

external stub_sendto : Unix.file_descr -> Bytes.t -> int -> int -> Unix.msg_flag list -> Unix.sockaddr -> int = "lwt_unix_sendto_byte" "lwt_unix_sendto"

let sendto ch buf pos len flags addr =
  if pos < 0 || len < 0 || pos > Bytes.length buf - len then
    invalid_arg "Lwt_unix.sendto"
  else
    let do_sendto = if Sys.win32 then Unix.sendto else stub_sendto in
    wrap_syscall Write ch (fun () -> do_sendto ch.fd buf pos len flags addr)

external stub_recv_msg :
  Unix.file_descr -> int -> IO_vectors.io_vector list ->
    int * Unix.file_descr list =
  "lwt_unix_recv_msg"

let recv_msg ~socket ~io_vectors =
  let count = check_io_vectors "Lwt_unix.recv_msg" io_vectors in
  wrap_syscall Read socket (fun () ->
    stub_recv_msg socket.fd count io_vectors.IO_vectors.prefix)

external stub_send_msg :
  Unix.file_descr ->
  int -> IO_vectors.io_vector list ->
  int -> Unix.file_descr list -> Unix.sockaddr option ->
    int = "lwt_unix_send_msg_byte" "lwt_unix_send_msg"

let send_msg ~socket ~io_vectors ~fds =
  let vector_count = check_io_vectors "Lwt_unix.send_msg" io_vectors in
  let fd_count = List.length fds in
  wrap_syscall Write socket (fun () ->
    stub_send_msg
      socket.fd vector_count io_vectors.IO_vectors.prefix fd_count fds None)

let send_msgto ~socket ~io_vectors ~fds ~dest =
  let vector_count = check_io_vectors "Lwt_unix.send_msgto" io_vectors in
  let fd_count = List.length fds in
  wrap_syscall Write socket (fun () ->
    stub_send_msg
      socket.fd vector_count io_vectors.IO_vectors.prefix fd_count fds (Some dest))

type inet_addr = Unix.inet_addr

type socket_domain =
  Unix.socket_domain =
  | PF_UNIX
  | PF_INET
  | PF_INET6

type socket_type =
  Unix.socket_type =
  | SOCK_STREAM
  | SOCK_DGRAM
  | SOCK_RAW
  | SOCK_SEQPACKET

type sockaddr = Unix.sockaddr = ADDR_UNIX of string | ADDR_INET of inet_addr * int

let socket ?cloexec dom typ proto =
  let s = Unix.socket ?cloexec dom typ proto in
  mk_ch ~blocking:false s

type shutdown_command =
  Unix.shutdown_command =
  | SHUTDOWN_RECEIVE
  | SHUTDOWN_SEND
  | SHUTDOWN_ALL

let shutdown ch shutdown_command =
  check_descriptor ch;
  Unix.shutdown ch.fd shutdown_command

external stub_socketpair : ?cloexec:bool -> socket_domain -> socket_type -> int -> Unix.file_descr * Unix.file_descr = "lwt_unix_socketpair_stub"

let socketpair ?cloexec dom typ proto =
  let (s1, s2) =
#if OCAML_VERSION >= (4, 14, 0)
    if Sys.win32 && (dom <> Unix.PF_UNIX) then
      stub_socketpair ?cloexec dom typ proto
    else Unix.socketpair ?cloexec dom typ proto in
#else
    if Sys.win32 then stub_socketpair ?cloexec dom typ proto
    else begin
      let (s1, s2) = Unix.socketpair dom typ proto in
      if cloexec = Some true then begin
        Unix.set_close_on_exec s1;
        Unix.set_close_on_exec s2
      end;
      (s1, s2)
    end in
#endif
  (mk_ch ~blocking:false s1, mk_ch ~blocking:false s2)

external accept4 :
  ?cloexec:bool -> nonblock:bool ->
  Unix.file_descr -> Unix.file_descr * Unix.sockaddr = "lwt_unix_accept4"

let accept_and_set_nonblock ?cloexec ch_fd =
  if Lwt_config._HAVE_ACCEPT4 then
    let (fd, addr) = accept4 ?cloexec ~nonblock:true ch_fd in
    (mk_ch ~blocking:false ~set_flags:false fd, addr)
  else
    let (fd, addr) = Unix.accept ?cloexec ch_fd in
    (mk_ch ~blocking:false fd, addr)

let accept ?cloexec ch =
  let default () =
    wrap_syscall Read ch (fun _ -> accept_and_set_nonblock ?cloexec ch.fd)
  in
  (* The completion back end accepts with Lwt's default flags (non-blocking,
     no close-on-exec): only take over when the caller wants exactly that. *)
  match cloexec, ch.state, !completion_io with
  | (None | Some false), Opened, Some io -> (
    match io.accept ch with
    | Some p ->
      Lwt.bind p (fun (fd, addr) ->
        Lwt.return (mk_ch ~blocking:false ~set_flags:false fd, addr))
    | None -> default ())
  | _ -> default ()

let accept_n ?cloexec ch n =
  let l = ref [] in
  Lazy.force ch.blocking >>= fun blocking ->
  Lwt.catch
    (fun () ->
       wrap_syscall Read ch begin fun () ->
         begin
           try
             for _i = 1 to n do
               if blocking && not (unix_readable ch.fd) then raise Retry;
               l := accept_and_set_nonblock ?cloexec ch.fd :: !l
             done
           with
           | (Unix.Unix_error((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) | Retry) when !l <> [] ->
             (* Ignore blocking errors if we have at least one file-descriptor: *)
             ()
         end;
         (List.rev !l, None)
       end)
    (fun exn -> Lwt.return (List.rev !l, Some exn))

let connect ch addr =
  let default () =
    if Sys.win32 then
      (* [in_progress] tell whether connection has started but not
         terminated: *)
      let in_progress = ref false in
      wrap_syscall Write ch begin fun () ->
        if !in_progress then
          (* Nothing works without this test and i have no idea why... *)
          if writable ch then
            try
              Unix.connect ch.fd addr
            with
            | Unix.Unix_error (Unix.EISCONN, _, _) ->
              (* This is the windows way of telling that the connection
                 has completed. *)
              ()
          else
            raise Retry
        else
          try
            Unix.connect ch.fd addr
          with
          | Unix.Unix_error (Unix.EWOULDBLOCK, _, _) ->
            in_progress := true;
            raise Retry
      end
    else
      (* [in_progress] tell whether connection has started but not
         terminated: *)
      let in_progress = ref false in
      wrap_syscall Write ch begin fun () ->
        if !in_progress then
          (* If the connection is in progress, [getsockopt_error] tells
             whether it succceed: *)
          match Unix.getsockopt_error ch.fd with
          | None ->
            (* The socket is connected *)
            ()
          | Some err ->
            (* An error happened: *)
            raise (Unix.Unix_error(err, "connect", ""))
        else
          try
            (* We should pass only one time here, unless the system call
               is interrupted by a signal: *)
            Unix.connect ch.fd addr
          with
          | Unix.Unix_error (Unix.EINPROGRESS, _, _) ->
            in_progress := true;
            raise Retry
      end
  in
  match ch.state, !completion_io with
  | Opened, Some io ->
    (match io.connect ch addr with Some p -> p | None -> default ())
  | _ -> default ()

external bind_job : Unix.file_descr -> Unix.sockaddr -> unit job =
  "lwt_unix_bind_job"

let bind fd addr =
  check_descriptor fd;
  match Sys.win32, addr with
  | true, _ | false, Unix.ADDR_INET _ -> Lwt.return (Unix.bind fd.fd addr)
  | false, Unix.ADDR_UNIX _ -> run_job (bind_job fd.fd addr)

let listen ch cnt =
  check_descriptor ch;
  Unix.listen ch.fd cnt

external somaxconn : unit -> int = "lwt_unix_somaxconn"

let getpeername ch =
  check_descriptor ch;
  Unix.getpeername ch.fd

let getsockname ch =
  check_descriptor ch;
  Unix.getsockname ch.fd

type credentials = {
  cred_pid : int;
  cred_uid : int;
  cred_gid : int;
}

external stub_get_credentials : Unix.file_descr -> credentials = "lwt_unix_get_credentials"

let get_credentials ch =
  check_descriptor ch;
  stub_get_credentials ch.fd

(* +-----------------------------------------------------------------+
   | Socket options                                                  |
   +-----------------------------------------------------------------+ *)

type socket_bool_option =
  Unix.socket_bool_option =
  | SO_DEBUG
  | SO_BROADCAST
  | SO_REUSEADDR
  | SO_KEEPALIVE
  | SO_DONTROUTE
  | SO_OOBINLINE
  | SO_ACCEPTCONN
  | TCP_NODELAY
  | IPV6_ONLY
#if OCAML_VERSION >= (4, 12, 0)
  | SO_REUSEPORT
#endif

type socket_int_option =
  Unix.socket_int_option =
  | SO_SNDBUF
  | SO_RCVBUF
  | SO_ERROR [@ocaml.deprecated "Use Unix.getsockopt_error instead."]
  | SO_TYPE
  | SO_RCVLOWAT
  | SO_SNDLOWAT

type socket_optint_option = Unix.socket_optint_option = SO_LINGER

type socket_float_option =
  Unix.socket_float_option =
  | SO_RCVTIMEO
  | SO_SNDTIMEO

let getsockopt ch opt =
  check_descriptor ch;
  Unix.getsockopt ch.fd opt

let setsockopt ch opt x =
  check_descriptor ch;
  Unix.setsockopt ch.fd opt x

let getsockopt_int ch opt =
  check_descriptor ch;
  Unix.getsockopt_int ch.fd opt

let setsockopt_int ch opt x =
  check_descriptor ch;
  Unix.setsockopt_int ch.fd opt x

let getsockopt_optint ch opt =
  check_descriptor ch;
  Unix.getsockopt_optint ch.fd opt

let setsockopt_optint ch opt x =
  check_descriptor ch;
  Unix.setsockopt_optint ch.fd opt x

let getsockopt_float ch opt =
  check_descriptor ch;
  Unix.getsockopt_float ch.fd opt

let setsockopt_float ch opt x =
  check_descriptor ch;
  Unix.setsockopt_float ch.fd opt x

let getsockopt_error ch =
  check_descriptor ch;
  Unix.getsockopt_error ch.fd

(* +-----------------------------------------------------------------+
   | Multicast functions                                             |
   +-----------------------------------------------------------------+ *)

external stub_mcast_set_loop : Unix.file_descr -> bool -> unit = "lwt_unix_mcast_set_loop"

external stub_mcast_set_ttl : Unix.file_descr -> int -> unit = "lwt_unix_mcast_set_ttl"

type mcast_action = Add | Drop

external stub_mcast_modify_membership :
  Unix.file_descr -> mcast_action -> Unix.inet_addr -> Unix.inet_addr -> unit =
  "lwt_unix_mcast_modify_membership"

let mcast_set_loop ch flag =
  check_descriptor ch;
  stub_mcast_set_loop ch.fd flag

let mcast_set_ttl ch ttl =
  check_descriptor ch;
  stub_mcast_set_ttl ch.fd ttl

let mcast_add_membership ch ?(ifname = Unix.inet_addr_any) addr =
  check_descriptor ch;
  stub_mcast_modify_membership ch.fd Add ifname addr

let mcast_drop_membership ch ?(ifname = Unix.inet_addr_any) addr =
  check_descriptor ch;
  stub_mcast_modify_membership ch.fd Drop ifname addr

(* +-----------------------------------------------------------------+
   | Host and protocol databases                                     |
   +-----------------------------------------------------------------+ *)

type host_entry =
  Unix.host_entry =
  {
    h_name : string;
    h_aliases : string array;
    h_addrtype : socket_domain;
    h_addr_list : inet_addr array
  }

type protocol_entry =
  Unix.protocol_entry =
  {
    p_name : string;
    p_aliases : string array;
    p_proto : int
  }

type service_entry =
  Unix.service_entry =
  {
    s_name : string;
    s_aliases : string array;
    s_port : int;
    s_proto : string
  }

external gethostname_job : unit -> string job = "lwt_unix_gethostname_job"

let gethostname () =
  if Sys.win32 then
    Lwt.return (Unix.gethostname ())
  else
    run_job (gethostname_job ())

let hostent_mutex = Lwt_mutex.create ()

external gethostbyname_job : string -> Unix.host_entry job = "lwt_unix_gethostbyname_job"

let gethostbyname name =
  if Sys.win32 then
    Lwt.return (Unix.gethostbyname name)
  else if Lwt_config._HAVE_REENTRANT_HOSTENT then
    run_job (gethostbyname_job name)
  else
    Lwt_mutex.with_lock hostent_mutex ( fun () ->
      run_job (gethostbyname_job name) )

external gethostbyaddr_job : Unix.inet_addr -> Unix.host_entry job = "lwt_unix_gethostbyaddr_job"

let gethostbyaddr addr =
  if Sys.win32 then
    Lwt.return (Unix.gethostbyaddr addr)
  else if Lwt_config._HAVE_REENTRANT_HOSTENT then
    run_job (gethostbyaddr_job addr)
  else
    Lwt_mutex.with_lock hostent_mutex ( fun () ->
      run_job (gethostbyaddr_job addr) )

let protoent_mutex =
  if Sys.win32 || Lwt_config._HAVE_NETDB_REENTRANT then
    hostent_mutex
  else
    Lwt_mutex.create ()

external getprotobyname_job : string -> Unix.protocol_entry job = "lwt_unix_getprotobyname_job"

let getprotobyname name =
  if Sys.win32 then
    Lwt.return (Unix.getprotobyname name)
  else if Lwt_config._HAVE_NETDB_REENTRANT then
    run_job (getprotobyname_job name)
  else
    Lwt_mutex.with_lock protoent_mutex ( fun () ->
      run_job (getprotobyname_job name))

external getprotobynumber_job : int -> Unix.protocol_entry job = "lwt_unix_getprotobynumber_job"

let getprotobynumber number =
  if Sys.win32 then
    Lwt.return (Unix.getprotobynumber number)
  else if Lwt_config._HAVE_NETDB_REENTRANT then
    run_job (getprotobynumber_job number)
  else
    Lwt_mutex.with_lock protoent_mutex ( fun () ->
      run_job (getprotobynumber_job number))

(* TODO: Not used anywhere, and that might be a bug. *)
let _servent_mutex =
  if Sys.win32 || Lwt_config._HAVE_NETDB_REENTRANT then
    hostent_mutex
  else
    Lwt_mutex.create ()

external getservbyname_job : string -> string -> Unix.service_entry job = "lwt_unix_getservbyname_job"

let getservbyname name x =
  if Sys.win32 then
    Lwt.return (Unix.getservbyname name x)
  else if Lwt_config._HAVE_NETDB_REENTRANT then
    run_job (getservbyname_job name x)
  else
    Lwt_mutex.with_lock protoent_mutex ( fun () ->
      run_job (getservbyname_job name x) )

external getservbyport_job : int -> string -> Unix.service_entry job = "lwt_unix_getservbyport_job"

let getservbyport port x =
  if Sys.win32 then
    Lwt.return (Unix.getservbyport port x)
  else if Lwt_config._HAVE_NETDB_REENTRANT then
    run_job (getservbyport_job port x)
  else
    Lwt_mutex.with_lock protoent_mutex ( fun () ->
      run_job (getservbyport_job port x) )

type addr_info =
  Unix.addr_info =
  {
    ai_family : socket_domain;
    ai_socktype : socket_type;
    ai_protocol : int;
    ai_addr : sockaddr;
    ai_canonname : string;
  }

type getaddrinfo_option =
  Unix.getaddrinfo_option =
  | AI_FAMILY of socket_domain
  | AI_SOCKTYPE of socket_type
  | AI_PROTOCOL of int
  | AI_NUMERICHOST
  | AI_CANONNAME
  | AI_PASSIVE

external getaddrinfo_job : string -> string -> Unix.getaddrinfo_option list -> Unix.addr_info list job = "lwt_unix_getaddrinfo_job"

(* A numeric host (parsed by [inet_addr_of_string]) with an all-digits (or
   empty) service resolves without consulting DNS: adding [AI_NUMERICHOST]
   (a no-op on an already-numeric input) guarantees it, and the libc parses
   an all-digits service directly (no name-service lookup either). The call
   therefore cannot block and needs no worker-pool job. This is the common
   case of every client connecting by IP:port — e.g. an HTTP client doing one
   resolution per connection otherwise pays a thread round-trip per
   request. *)
let getaddrinfo_numeric_fast_path host service opts =
  let numeric_host =
    host <> "" && (match Unix.inet_addr_of_string host with
                   | (_ : Unix.inet_addr) -> true
                   | exception _ -> false)
  in
  let numeric_service =
    service = "" || String.for_all (fun c -> c >= '0' && c <= '9') service
  in
  if numeric_host && numeric_service then
    Some (Unix.getaddrinfo host service (Unix.AI_NUMERICHOST :: opts))
  else
    None

let getaddrinfo host service opts =
  if Sys.win32 then
    Lwt.return (Unix.getaddrinfo host service opts)
  else
    match getaddrinfo_numeric_fast_path host service opts with
    | Some result -> Lwt.return result
    | None ->
      run_job (getaddrinfo_job host service opts) >>= fun l ->
      Lwt.return (List.rev l)

type name_info =
  Unix.name_info =
  {
    ni_hostname : string;
    ni_service : string;
  }

type getnameinfo_option =
  Unix.getnameinfo_option =
  | NI_NOFQDN
  | NI_NUMERICHOST
  | NI_NAMEREQD
  | NI_NUMERICSERV
  | NI_DGRAM

external getnameinfo_job : Unix.sockaddr -> Unix.getnameinfo_option list -> Unix.name_info job = "lwt_unix_getnameinfo_job"

let getnameinfo addr opts =
  if Sys.win32 then
    Lwt.return (Unix.getnameinfo addr opts)
  else
    run_job (getnameinfo_job addr opts)

(* +-----------------------------------------------------------------+
   | Terminal interface                                              |
   +-----------------------------------------------------------------+ *)

type terminal_io =
  Unix.terminal_io =
  {
    mutable c_ignbrk : bool;
    mutable c_brkint : bool;
    mutable c_ignpar : bool;
    mutable c_parmrk : bool;
    mutable c_inpck : bool;
    mutable c_istrip : bool;
    mutable c_inlcr : bool;
    mutable c_igncr : bool;
    mutable c_icrnl : bool;
    mutable c_ixon : bool;
    mutable c_ixoff : bool;
    mutable c_opost : bool;
    mutable c_obaud : int;
    mutable c_ibaud : int;
    mutable c_csize : int;
    mutable c_cstopb : int;
    mutable c_cread : bool;
    mutable c_parenb : bool;
    mutable c_parodd : bool;
    mutable c_hupcl : bool;
    mutable c_clocal : bool;
    mutable c_isig : bool;
    mutable c_icanon : bool;
    mutable c_noflsh : bool;
    mutable c_echo : bool;
    mutable c_echoe : bool;
    mutable c_echok : bool;
    mutable c_echonl : bool;
    mutable c_vintr : char;
    mutable c_vquit : char;
    mutable c_verase : char;
    mutable c_vkill : char;
    mutable c_veof : char;
    mutable c_veol : char;
    mutable c_vmin : int;
    mutable c_vtime : int;
    mutable c_vstart : char;
    mutable c_vstop : char;
  }

type setattr_when =
  Unix.setattr_when =
  | TCSANOW
  | TCSADRAIN
  | TCSAFLUSH

type flush_queue =
  Unix.flush_queue =
  | TCIFLUSH
  | TCOFLUSH
  | TCIOFLUSH

type flow_action =
  Unix.flow_action =
  | TCOOFF
  | TCOON
  | TCIOFF
  | TCION

external tcgetattr_job : Unix.file_descr -> Unix.terminal_io job = "lwt_unix_tcgetattr_job"

let tcgetattr ch =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.tcgetattr ch.fd)
  else
    run_job (tcgetattr_job ch.fd)

external tcsetattr_job : Unix.file_descr -> Unix.setattr_when -> Unix.terminal_io -> unit job = "lwt_unix_tcsetattr_job"

let tcsetattr ch when_ attrs =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.tcsetattr ch.fd when_ attrs)
  else
    run_job (tcsetattr_job ch.fd when_ attrs)

external tcsendbreak_job :
  Unix.file_descr -> int -> unit job = "lwt_unix_tcsendbreak_job"

let tcsendbreak ch delay =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.tcsendbreak ch.fd delay)
  else
    run_job (tcsendbreak_job ch.fd delay)

external tcdrain_job : Unix.file_descr -> unit job = "lwt_unix_tcdrain_job"

let tcdrain ch =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.tcdrain ch.fd)
  else
    run_job (tcdrain_job ch.fd)

external tcflush_job :
  Unix.file_descr -> Unix.flush_queue -> unit job = "lwt_unix_tcflush_job"

let tcflush ch q =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.tcflush ch.fd q)
  else
    run_job (tcflush_job ch.fd q)

external tcflow_job :
  Unix.file_descr -> Unix.flow_action -> unit job = "lwt_unix_tcflow_job"

let tcflow ch act =
  check_descriptor ch;
  if Sys.win32 then
    Lwt.return (Unix.tcflow ch.fd act)
  else
    run_job (tcflow_job ch.fd act)

(* +-----------------------------------------------------------------+
   | Reading notifications                                           |
   +-----------------------------------------------------------------+ *)

(* Closes the forward cell opened at the top: from here on, a channel's event
   drains that channel. *)
let () =
  drain_notifications :=
    fun chan -> Array.iter call_notification (recv_notifications chan)

(* +-----------------------------------------------------------------+
   | Signals                                                         |
   +-----------------------------------------------------------------+ *)

external set_signal : int -> int -> bool -> unit = "lwt_unix_set_signal"
external remove_signal : int -> int -> bool -> unit = "lwt_unix_remove_signal"
external init_signals : unit -> unit = "lwt_unix_init_signals"
external handle_signal : int -> unit = "lwt_unix_handle_signal"

let () = init_signals ()

let set_signal signum notification =
  set_signal signum notification (Lwt_engine.forwards_signal signum)

let remove_signal signum notification =
  remove_signal signum notification (Lwt_engine.forwards_signal signum)

(* Reports on the CALLING loop's subscriptions. *)
let signal_count () =
  Signal_map.fold
    (fun _signum (_notification, actions) len -> len + Lwt_sequence.length actions)
    (self_notif ()).signals
    0

(* Subscribes THIS loop to the signal, if it is not already, and adds the
   handler. Any domain may do this: the C side keeps one subscriber slot per
   channel and wakes them all, so every subscribed loop gets the signal at
   home. *)
let on_signal_full signum handler =
  let notif = self_notif () in
  let id = ref None in
  let _, actions =
    try
      Signal_map.find signum notif.signals
    with Not_found ->
      let actions = Lwt_sequence.create () in
      let notification =
        make_notification
          (fun () ->
             Lwt_sequence.iter_l
               (fun f -> f id signum)
               actions)
      in
      (try
         set_signal signum notification
       with exn when Lwt.Exception_filter.run exn ->
         stop_notification notification;
         raise exn);
      notif.signals <- Signal_map.add signum (notification, actions) notif.signals;
      (notification, actions)
  in
  let node = Lwt_sequence.add_r handler actions in
  id := Some { sh_num = signum; sh_node = node };
  id

let on_signal signum f = on_signal_full signum (fun _notification num -> f num)

let disable_signal_handler id =
  match !id with
  | None ->
    ()
  | Some sh ->
    let notif = self_notif () in
    id := None;
    Lwt_sequence.remove sh.sh_node;
    match Signal_map.find_opt sh.sh_num notif.signals with
    | None ->
      (* Registered on another loop: its handler list is not ours to unsubscribe
         from, and the node above was all we could remove. *)
      ()
    | Some (notification, actions) ->
      if Lwt_sequence.is_empty actions then begin
        remove_signal sh.sh_num notification;
        notif.signals <- Signal_map.remove sh.sh_num notif.signals;
        stop_notification notification
      end

(* Closes the forward cell opened at the top: retiring a loop drops its
   subscriptions. Only this domain's, and it is the only one that could. *)
let () =
  drop_signal_subscriptions :=
    fun () ->
      let notif = self_notif () in
      Signal_map.iter
        (fun signum (notification, _) -> remove_signal signum notification)
        notif.signals;
      notif.signals <- Signal_map.empty

let reinstall_signal_handler signum =
  match Signal_map.find signum (self_notif ()).signals with
  | exception Not_found -> ()
  | notification, _ ->
    set_signal signum notification

(* +-----------------------------------------------------------------+
   | Processes                                                       |
   +-----------------------------------------------------------------+ *)

external reset_after_fork : unit -> unit = "lwt_unix_reset_after_fork"

(* TODO: replace fork with something thread+domain safe *)
let fork () =
  (* It reinitialises the notification descriptor and empties the job sequence,
     both of which belong to the owner; and [Unix.fork] itself is unsupported by
     the runtime while other domains are running. *)
  check_loader "Lwt_unix.fork";
  match Unix.fork () with
  | 0 ->
    (* Let the engine handle the fork *)
    Lwt_engine.fork ();
    (* Reset threading. *)
    reset_after_fork ();
    (* Replace this domain's notification channel: the inherited descriptors are
       the parent's, and its buffer may hold ids the parent will service. The
       old channel is retired first, which closes those descriptors.

       Only THIS domain's channel is dealt with, which is enough: [fork] is
       refused off the loading domain, and the runtime does not support forking
       with several domains running, so there is one channel to replace. *)
    let old = Lwt_dls.get notif_slot in
    Lwt_engine.stop_event old.event;
    free_notification_channel old.chan;
    let fd, chan = new_notification_channel () in
    let event =
      Lwt_engine.on_readable fd (fun _ -> !drain_notifications chan)
    in
    (* The child keeps the parent's job sequence, which the lines below empty and
       cancel; only the channel is replaced. *)
    Lwt_dls.set notif_slot { old with chan; event };
    (* Collect all pending jobs. *)
    let jobs = (self_notif ()).jobs in
    let l = Lwt_sequence.fold_l (fun (_, f) l -> f :: l) jobs [] in
    (* Remove them all. *)
    Lwt_sequence.iter_node_l Lwt_sequence.remove jobs;
    (* And cancel them all. We yield first so that if the program
       do an exec just after, it won't be executed. *)
    Lwt.on_termination (Lwt.pause ()) (fun () -> List.iter (fun f -> f Lwt.Canceled) l);
    0
  | pid ->
    pid

type process_status =
  Unix.process_status =
  | WEXITED of int
  | WSIGNALED of int
  | WSTOPPED of int

type wait_flag =
  Unix.wait_flag =
  | WNOHANG
  | WUNTRACED

let has_wait4 = not Sys.win32

external stub_wait4 : Unix.wait_flag list -> int -> int * Unix.process_status * resource_usage = "lwt_unix_wait4"

let do_wait4 flags pid =
  if Sys.win32 || Lwt_config.android then
    let pid, status = Unix.waitpid flags pid in
    (pid, status, { ru_utime = 0.0; ru_stime = 0.0 })
  else
    stub_wait4 flags pid


(* Reports on the CALLING loop's waits. *)
let wait_count () = Lwt_sequence.length (self_notif ()).wait_children

(* The lazy value is forced when calling [Lwt_main.run]. This is to avoid
   installing the sigchld_handler in cases where the user is not really using
   Lwt (just linking against it for some reason). *)
(* Do not force this directly: use [install_sigchld_handler] below. Forcing a
   [lazy] from two domains at once raises [Lazy.Undefined] in one of them, and
   [Lwt_main.run] forces this one two lines before entering the scheduler, so N
   loops would trip on it immediately. The value stays exposed because it is part
   of this module's interface. *)
(* Reaps the children THIS loop is waiting for. Runs on the loop that subscribed,
   because that is where its SIGCHLD notification is delivered. Two loops each
   waiting for their own child both get woken and each finds its own; two loops
   waiting for ANY child race for whichever exits, which is what waiting for any
   child means. *)
let sigchld_handler () =
  let wait_children = (self_notif ()).wait_children in
  Lwt_sequence.iter_node_l begin fun node ->
    let wakener, flags, pid = Lwt_sequence.get node in
    try
      let (pid', _, _) as v = do_wait4 flags pid in
      if pid' <> 0 then begin
        Lwt_sequence.remove node;
        Lwt.wakeup wakener v
      end
    with e when Lwt.Exception_filter.run e ->
      Lwt_sequence.remove node;
      Lwt.wakeup_exn wakener e
  end wait_children

(* PER LOOP, now that a signal reaches every subscriber: each loop that waits for
   a child subscribes for itself, and reaps its own. The flag is in the
   per-domain record, so no lock is needed where the process-wide version had an
   atomic and a mutex.

   Called by [Lwt_main.run] on every domain, and idempotent per domain. *)
let install_sigchld_handler () =
  let notif = self_notif () in
  if not notif.sigchld_installed then begin
    notif.sigchld_installed <- true;
    if not Sys.win32 then ignore (on_signal Sys.sigchld (fun _ ->
      sigchld_handler ()))
  end

let sigchld_handler_installer = lazy (install_sigchld_handler ())

let _waitpid flags pid =
  Lwt.catch
    (fun () -> Lwt.return (Unix.waitpid flags pid))
    Lwt.reraise

let waitpid =
  if Sys.win32 then
    _waitpid
  else
    fun flags pid ->
      install_sigchld_handler ();
      if List.mem Unix.WNOHANG flags then
        _waitpid flags pid
      else
        let flags = Unix.WNOHANG :: flags in
        _waitpid flags pid >>= fun ((pid', _) as res) ->
        if pid' <> 0 then
          Lwt.return res
        else begin
          let (res, w) = Lwt.task () in
          let node =
            Lwt_sequence.add_l (w, flags, pid) (self_notif ()).wait_children
          in
          Lwt.on_cancel res (fun _ -> Lwt_sequence.remove node);
          res >>= fun (pid, status, _) ->
          Lwt.return (pid, status)
        end

let wait4 flags pid =
  install_sigchld_handler ();
  if Sys.win32 || Lwt_config.android then
    Lwt.return (do_wait4 flags pid)
  else
  if List.mem Unix.WNOHANG flags then
    Lwt.return (do_wait4 flags pid)
  else
    let flags = Unix.WNOHANG :: flags in
    let (pid', _, _) as res = do_wait4 flags pid in
    if pid' <> 0 then
      Lwt.return res
    else begin
      let (res, w) = Lwt.task () in
      let node =
        Lwt_sequence.add_l (w, flags, pid) (self_notif ()).wait_children
      in
      Lwt.on_cancel res (fun _ -> Lwt_sequence.remove node);
      res
    end

let wait () = waitpid [] (-1)

external system_job : string -> int job = "lwt_unix_system_job"

#if OCAML_VERSION >= (5, 0, 0)
external unix_exit : int -> 'a = "caml_unix_exit"
#else
external unix_exit : int -> 'a = "unix_exit"
#endif

let system cmd =
  if Sys.win32 then
    run_job (system_job ("cmd.exe /c " ^ cmd)) >>= fun code ->
    Lwt.return (Unix.WEXITED code)
  else
    match fork () with
    | 0 ->
      begin try
          Unix.execv "/bin/sh" [| "/bin/sh"; "-c"; cmd |]
        with _ ->
          (* Do not run at_exit hooks *)
          unix_exit 127
      end
    | id ->
      waitpid [] id >|= snd

(* +-----------------------------------------------------------------+
   | Misc                                                            |
   +-----------------------------------------------------------------+ *)

let handle_unix_error f x =
  Lwt.catch
    (fun () -> f x)
    (fun exn ->
       Unix.handle_unix_error (fun () -> raise exn) ())

(* +-----------------------------------------------------------------+
   | System thread pool                                              |
   +-----------------------------------------------------------------+ *)

[@@@ocaml.warning "-3"]
external pool_size : unit -> int = "lwt_unix_pool_size" "noalloc"
external set_pool_size : int -> unit = "lwt_unix_set_pool_size" "noalloc"
external thread_count : unit -> int = "lwt_unix_thread_count" "noalloc"
external thread_waiting_count : unit -> int = "lwt_unix_thread_waiting_count" "noalloc"
[@@@ocaml.warning "+3"]

(* +-----------------------------------------------------------------+
   | CPUs                                                            |
   +-----------------------------------------------------------------+ *)

external get_cpu : unit -> int = "lwt_unix_get_cpu"

external stub_get_affinity : int -> int list = "lwt_unix_get_affinity"
external stub_set_affinity : int -> int list -> unit = "lwt_unix_set_affinity"

let get_affinity ?(pid=0) () = stub_get_affinity pid
let set_affinity ?(pid=0) l = stub_set_affinity pid l

(* +-----------------------------------------------------------------+
   | Error printing                                                  |
   +-----------------------------------------------------------------+ *)

let () =
  Printexc.register_printer
    (function
      | Unix.Unix_error(error, func, arg) ->
        let error =
          match error with
          | Unix.E2BIG -> "E2BIG"
          | Unix.EACCES -> "EACCES"
          | Unix.EAGAIN -> "EAGAIN"
          | Unix.EBADF -> "EBADF"
          | Unix.EBUSY -> "EBUSY"
          | Unix.ECHILD -> "ECHILD"
          | Unix.EDEADLK -> "EDEADLK"
          | Unix.EDOM -> "EDOM"
          | Unix.EEXIST -> "EEXIST"
          | Unix.EFAULT -> "EFAULT"
          | Unix.EFBIG -> "EFBIG"
          | Unix.EINTR -> "EINTR"
          | Unix.EINVAL -> "EINVAL"
          | Unix.EIO -> "EIO"
          | Unix.EISDIR -> "EISDIR"
          | Unix.EMFILE -> "EMFILE"
          | Unix.EMLINK -> "EMLINK"
          | Unix.ENAMETOOLONG -> "ENAMETOOLONG"
          | Unix.ENFILE -> "ENFILE"
          | Unix.ENODEV -> "ENODEV"
          | Unix.ENOENT -> "ENOENT"
          | Unix.ENOEXEC -> "ENOEXEC"
          | Unix.ENOLCK -> "ENOLCK"
          | Unix.ENOMEM -> "ENOMEM"
          | Unix.ENOSPC -> "ENOSPC"
          | Unix.ENOSYS -> "ENOSYS"
          | Unix.ENOTDIR -> "ENOTDIR"
          | Unix.ENOTEMPTY -> "ENOTEMPTY"
          | Unix.ENOTTY -> "ENOTTY"
          | Unix.ENXIO -> "ENXIO"
          | Unix.EPERM -> "EPERM"
          | Unix.EPIPE -> "EPIPE"
          | Unix.ERANGE -> "ERANGE"
          | Unix.EROFS -> "EROFS"
          | Unix.ESPIPE -> "ESPIPE"
          | Unix.ESRCH -> "ESRCH"
          | Unix.EXDEV -> "EXDEV"
          | Unix.EWOULDBLOCK -> "EWOULDBLOCK"
          | Unix.EINPROGRESS -> "EINPROGRESS"
          | Unix.EALREADY -> "EALREADY"
          | Unix.ENOTSOCK -> "ENOTSOCK"
          | Unix.EDESTADDRREQ -> "EDESTADDRREQ"
          | Unix.EMSGSIZE -> "EMSGSIZE"
          | Unix.EPROTOTYPE -> "EPROTOTYPE"
          | Unix.ENOPROTOOPT -> "ENOPROTOOPT"
          | Unix.EPROTONOSUPPORT -> "EPROTONOSUPPORT"
          | Unix.ESOCKTNOSUPPORT -> "ESOCKTNOSUPPORT"
          | Unix.EOPNOTSUPP -> "EOPNOTSUPP"
          | Unix.EPFNOSUPPORT -> "EPFNOSUPPORT"
          | Unix.EAFNOSUPPORT -> "EAFNOSUPPORT"
          | Unix.EADDRINUSE -> "EADDRINUSE"
          | Unix.EADDRNOTAVAIL -> "EADDRNOTAVAIL"
          | Unix.ENETDOWN -> "ENETDOWN"
          | Unix.ENETUNREACH -> "ENETUNREACH"
          | Unix.ENETRESET -> "ENETRESET"
          | Unix.ECONNABORTED -> "ECONNABORTED"
          | Unix.ECONNRESET -> "ECONNRESET"
          | Unix.ENOBUFS -> "ENOBUFS"
          | Unix.EISCONN -> "EISCONN"
          | Unix.ENOTCONN -> "ENOTCONN"
          | Unix.ESHUTDOWN -> "ESHUTDOWN"
          | Unix.ETOOMANYREFS -> "ETOOMANYREFS"
          | Unix.ETIMEDOUT -> "ETIMEDOUT"
          | Unix.ECONNREFUSED -> "ECONNREFUSED"
          | Unix.EHOSTDOWN -> "EHOSTDOWN"
          | Unix.EHOSTUNREACH -> "EHOSTUNREACH"
          | Unix.ELOOP -> "ELOOP"
          | Unix.EOVERFLOW -> "EOVERFLOW"
          | Unix.EUNKNOWNERR n -> Printf.sprintf "EUNKNOWNERR %d" n
        in
        Some(Printf.sprintf "Unix.Unix_error(Unix.%s, %S, %S)" error func arg)
      | _ ->
        None)

module Versioned =
struct
  let bind_1 ch addr =
    check_descriptor ch;
    Unix.bind ch.fd addr

  let bind_2 = bind

  let recv_msg_2 = recv_msg

  let send_msg_2 = send_msg
end

let write_job_count_runtimte_event () =
  Lwt_rte.emit_job_count (self_notif ()).job_count
