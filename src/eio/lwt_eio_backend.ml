(* PoC (branch lwt-on-eio-poc): run Lwt on top of Eio's runtime.

   Two layers:
   - Level A (shared event loop): an [Lwt_engine] whose readiness/timers are
     driven by Eio, so Lwt and Eio share ONE event loop in the same domain.
     This mirrors Thomas Leonard's [lwt_eio] (ISC), reduced to the essentials
     (no debug mode, no SIGCHLD sharing) for the PoC.
   - Level B (the novel part): route Lwt's OWN read/write/connect through Eio's
     io_uring completion ops, via [Lwt_unix.set_completion_io]. This is the same
     seam that [src/uring/lwt_uring.ml] fills with a private ring; here it is
     filled with Eio's ring, so Lwt's I/O rides the SHARED Eio ring.

   Linux only (uses [Eio_linux.Low_level]); not installed (private library);
   experimental branch, not for release. *)

open Eio.Std
module Ll = Eio_linux.Low_level

type Lwt_engine.engine_id += Engine_id__eio

(* Count of I/O operations that went through Eio's completion path. Lets the
   test prove Lwt's read/write actually rode Eio's ring, without strace. *)
let eio_ops = ref 0
let eio_op_count () = !eio_ops
let reset_eio_op_count () = eio_ops := 0

(* Forced to make the current [Lwt_engine.iter] return (an Eio fiber has woken
   an Lwt thread). *)
let ready = ref (lazy ())
let notify () = Lazy.force !ready

(* The switch owning every fiber that services Lwt operations. Lwt is not
   structured, so these fibers cannot take a caller switch; they hang off this
   loop-global one (exactly [lwt_eio]'s design and its documented compromise). *)
let loop_switch = ref None

let get_loop_switch () =
  match !loop_switch with
  | Some sw -> sw
  | None -> failwith "Lwt_eio_backend: must be called within with_event_loop"

let active () = Option.is_some !loop_switch

(* Cache of Eio Fd wrappers, keyed by the raw descriptor (see [submit]). Cleared
   when the event loop tears down, since the wrappers are registered on its
   switch. Caveat: within one loop it does not survive fd-number reuse after
   close; a production backend would invalidate on close. *)
let fd_cache : (Unix.file_descr, Eio_unix.Fd.t) Hashtbl.t = Hashtbl.create 64

let eio_fd sw raw_fd =
  match Hashtbl.find_opt fd_cache raw_fd with
  | Some efd -> efd
  | None ->
    let efd = Eio_unix.Fd.of_unix ~sw ~blocking:false ~close_unix:false raw_fd in
    Hashtbl.replace fd_cache raw_fd efd;
    efd

(* ===================== Level A: Lwt_engine backed by Eio ===================== *)

(* Run [fn] in a background fiber and return the [unit Lazy.t] unregister action
   Lwt_engine expects (forcing it cancels the fiber). *)
let fork_daemon ~sw fn =
  let cancel = ref (lazy ()) in
  Fiber.fork ~sw (fun () ->
      try
        Eio.Cancel.sub (fun cc ->
            cancel :=
              lazy (try Eio.Cancel.cancel cc Exit with Invalid_argument _ -> ());
            fn ())
      with Eio.Cancel.Cancelled Exit -> ());
  (* The forked fiber runs first, so [cancel] is set by now. *)
  !cancel

let make_engine ~sw ~clock =
  object
    inherit Lwt_engine.abstract
    method id = Engine_id__eio

    method private cleanup =
      (try Switch.fail sw Exit with Invalid_argument _ -> ())

    method private register_readable fd callback =
      fork_daemon ~sw (fun () ->
          while true do
            Eio_unix.await_readable fd;
            Eio.Cancel.protect (fun () ->
                callback ();
                notify ())
          done)

    method private register_writable fd callback =
      fork_daemon ~sw (fun () ->
          while true do
            Eio_unix.await_writable fd;
            Eio.Cancel.protect (fun () ->
                callback ();
                notify ())
          done)

    method private register_timer delay repeat callback =
      fork_daemon ~sw (fun () ->
          if repeat then
            while true do
              Eio.Time.sleep clock delay;
              Eio.Cancel.protect (fun () ->
                  callback ();
                  notify ())
            done
          else begin
            Eio.Time.sleep clock delay;
            Eio.Cancel.protect (fun () ->
                callback ();
                notify ())
          end)

    method iter block =
      if block then begin
        let p, r = Promise.create () in
        ready := lazy (Promise.resolve r ());
        Promise.await p
      end
      else Fiber.yield ()
  end

let main ~clock user_promise =
  let old_engine = Lwt_engine.get () in
  (try
     Switch.run (fun sw ->
         if Option.is_some !loop_switch then
           invalid_arg "Lwt_eio_backend: event loop already running";
         Switch.on_release sw (fun () ->
             loop_switch := None;
             Hashtbl.clear fd_cache);
         loop_switch := Some sw;
         Lwt_engine.set ~destroy:false (make_engine ~sw ~clock);
         (* An Eio fiber may resume an Lwt thread while inside [iter]; a
            [Lwt.pause] there would otherwise not wake up. *)
         Lwt.register_pause_notifier (fun _ -> notify ());
         Lwt_main.run user_promise;
         raise Exit)
   with Exit -> ());
  Lwt_engine.set old_engine

let with_event_loop ~clock fn =
  let p, r = Lwt.wait () in
  Switch.run @@ fun sw ->
  Fiber.fork ~sw (fun () -> main ~clock p);
  Fun.protect
    (fun () -> fn ())
    ~finally:(fun () ->
      Lwt.wakeup r ();
      notify ())

(* ============ Level B: Lwt I/O through Eio's io_uring completion ops ========= *)

(* Regular files read/write at their current position (offset -1); sockets and
   pipes ignore the offset (0). Same choice as lwt_uring's [read_op]/[write_op]. *)
let file_offset (kind : Unix.file_kind) =
  match kind with
  | Unix.S_REG | Unix.S_BLK -> Optint.Int63.minus_one
  | _ -> Optint.Int63.zero

(* Run a completion op [perform efd] on Eio's ring in a fresh fiber, and bridge
   its result to a cancelable Lwt promise. Cancelling the Lwt promise cancels
   the in-flight Eio op (which cancels the io_uring submission). A fiber fork is
   required per op because Eio's public completion API ([Low_level.readv], ...)
   is direct-style (it suspends the caller) rather than callback-style: this is
   the structural cost of routing Lwt's callback-based [completion_io] through
   Eio, which the [uring]-library-based maison backend (lwt_uring.ml:submit_io)
   avoids. *)
let submit perform raw_fd =
  let sw = get_loop_switch () in
  let efd = eio_fd sw raw_fd in
  let waiter, wakener = Lwt.task () in
  let cc = ref None in
  incr eio_ops;
  Fiber.fork ~sw (fun () ->
      Eio.Cancel.sub (fun cancel ->
          cc := Some cancel;
          match perform efd with
          | n ->
            Lwt.wakeup wakener n;
            notify ()
          | exception Eio.Cancel.Cancelled _ ->
            (* The Lwt promise is already rejected with [Canceled]. *)
            ()
          | exception ex ->
            Lwt.wakeup_exn wakener ex;
            notify ()));
  Lwt.on_cancel waiter (fun () ->
      Option.iter
        (fun cancel ->
          try Eio.Cancel.cancel cancel Lwt.Canceled
          with Invalid_argument _ -> ())
        !cc);
  waiter

let read_perform kind cs efd =
  try Ll.readv ~file_offset:(file_offset kind) efd [ cs ] with End_of_file -> 0

let write_perform kind cs efd =
  Ll.writev_single ~file_offset:(file_offset kind) efd [ cs ]

(* Installed once; self-gates on [active ()] so it takes over Lwt's I/O only
   while the Eio loop is running, and declines (returns [None]) otherwise, so
   Lwt falls back to its default readiness path. *)
let completion_backend : Lwt_unix.completion_io =
  let read ch buf pos len =
    if not (active ()) then None
    else
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.create len in
      Some
        (Lwt.map
           (fun n ->
             Cstruct.blit_to_bytes cs 0 buf pos n;
             n)
           (submit (read_perform kind cs) fd))
  in
  let write ch buf pos len =
    if not (active ()) then None
    else
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.create len in
      Cstruct.blit_from_bytes buf pos cs 0 len;
      Some (submit (write_perform kind cs) fd)
  in
  let read_bigarray ch buf pos len =
    if not (active ()) then None
    else
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.of_bigarray ~off:pos ~len buf in
      Some (submit (read_perform kind cs) fd)
  in
  let write_bigarray ch buf pos len =
    if not (active ()) then None
    else
      let fd = Lwt_unix.unix_file_descr ch and kind = Lwt_unix.fd_kind ch in
      let cs = Cstruct.of_bigarray ~off:pos ~len buf in
      Some (submit (write_perform kind cs) fd)
  in
  let connect ch addr =
    if not (active ()) then None
    else
      let fd = Lwt_unix.unix_file_descr ch in
      Some
        (Lwt.map
           (fun (_ : int) -> ())
           (submit
              (fun efd ->
                Ll.connect efd addr;
                0)
              fd))
  in
  { Lwt_unix.read; write; read_bigarray; write_bigarray; connect }

let enable_completion_io () = Lwt_unix.set_completion_io (Some completion_backend)
let disable_completion_io () = Lwt_unix.set_completion_io None

(* ============================= Interop bridges ============================== *)
(* Same shape as lwt_eio's bridges. *)

module Promise = struct
  let await_lwt lwt_promise =
    let p, r = Promise.create () in
    Lwt.on_any lwt_promise (Promise.resolve_ok r) (Promise.resolve_error r);
    Promise.await_exn p

  let await_eio eio_promise =
    let sw = get_loop_switch () in
    let p, r = Lwt.wait () in
    Fiber.fork ~sw (fun () ->
        let x = Promise.await eio_promise in
        Lwt.wakeup r x;
        notify ());
    p
end

let run_eio fn =
  let sw = get_loop_switch () in
  let p, r = Lwt.task () in
  let cc = ref None in
  Fiber.fork ~sw (fun () ->
      Eio.Cancel.sub (fun cancel ->
          cc := Some cancel;
          match fn () with
          | x ->
            Lwt.wakeup r x;
            notify ()
          | exception ex ->
            Lwt.wakeup_exn r ex;
            notify ()));
  Lwt.on_cancel p (fun () ->
      Option.iter
        (fun cancel -> Eio.Cancel.cancel cancel Lwt.Canceled)
        !cc);
  p

let run_lwt fn =
  Fiber.check ();
  let p = fn () in
  try
    Fiber.check ();
    Promise.await_lwt p
  with Eio.Cancel.Cancelled _ as ex ->
    Lwt.cancel p;
    raise ex
