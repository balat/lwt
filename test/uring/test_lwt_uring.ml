(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)



(* Standalone smoke test for the io_uring Lwt engine. It drives real [Lwt_unix]
   operations (timers, descriptor readiness, cancellation) through the engine,
   checking that it is a correct drop-in for libev/select. *)

open Lwt.Infix

let failures = ref 0

let check name cond =
  if cond then Printf.printf "ok   - %s\n%!" name
  else begin
    incr failures;
    Printf.printf "FAIL - %s\n%!" name
  end

(* A timer resolves after roughly the requested delay. *)
let test_timer () =
  let t0 = Unix.gettimeofday () in
  Lwt_main.run (Lwt_unix.sleep 0.05);
  let dt = Unix.gettimeofday () -. t0 in
  check "timer fires after its delay" (dt >= 0.03 && dt <= 1.0)

(* Concurrent timers resolve in time order. *)
let test_timer_order () =
  let order = ref [] in
  let record n () = Lwt_unix.sleep n >|= fun () -> order := n :: !order in
  Lwt_main.run (Lwt.join [record 0.06 (); record 0.01 (); record 0.03 ()]);
  check "timers resolve in delay order" (!order = [0.06; 0.03; 0.01])

(* A huge or infinite delay never expires, instead of overflowing the
   nanosecond conversion and expiring at once. *)
let test_huge_timers () =
  let outcome delay =
    Lwt_main.run
      (Lwt.pick
         [ (Lwt_unix.sleep delay >|= fun () -> `Fired);
           (Lwt_unix.sleep 0.05 >|= fun () -> `Not_fired) ])
  in
  check "sleep 1e10 and sleep infinity do not fire at once"
    (outcome 1e10 = `Not_fired && outcome infinity = `Not_fired)

(* Descriptor readiness: a socketpair round-trip exercises both
   register_readable and register_writable. *)
let test_socketpair () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.set_nonblock a;
  Unix.set_nonblock b;
  let a = Lwt_unix.of_unix_file_descr a in
  let b = Lwt_unix.of_unix_file_descr b in
  let msg = Bytes.of_string "ping-pong" in
  let n = Bytes.length msg in
  let received = Bytes.create n in
  let result =
    Lwt_main.run begin
      let writer = Lwt_unix.write a msg 0 n in
      let reader = Lwt_unix.read b received 0 n in
      writer >>= fun written ->
      reader >>= fun read ->
      Lwt.return (written, read)
    end
  in
  let written, read = result in
  check "socketpair write/read round-trip"
    (written = n && read = n && Bytes.equal received msg);
  Lwt_main.run (Lwt_unix.close a >>= fun () -> Lwt_unix.close b)

(* A cancelled timer rejects with [Lwt.Canceled] and does not delay the loop. *)
let test_cancel () =
  let cancelled = ref false in
  Lwt_main.run begin
    let long = Lwt_unix.sleep 10. in
    Lwt.on_failure long (fun _ -> cancelled := true);
    Lwt.cancel long;
    Lwt.catch (fun () -> long) (function
      | Lwt.Canceled -> Lwt.return_unit
      | exn -> Lwt.fail exn)
  end;
  check "cancelling a timer rejects with Canceled" !cancelled

(* Paused promises and engine I/O cooperate (Lwt_main alternates iter false /
   iter true). *)
let test_pause_and_timer () =
  let steps = ref 0 in
  Lwt_main.run begin
    Lwt.pause () >>= fun () ->
    incr steps;
    Lwt_unix.sleep 0.01 >>= fun () ->
    incr steps;
    Lwt.pause () >>= fun () ->
    incr steps;
    Lwt.return_unit
  end;
  check "pause and timers interleave correctly" (!steps = 3)

(* Stopping more events in one lap than the submission queue holds: every
   cancel reaches the kernel, so every closed socket is really closed and its
   peer sees end of file. *)
let test_cancel_storm () =
  Lwt_engine.set (new Lwt_uring.uring ~queue_depth:4 ());
  let n = 64 in
  let pairs =
    List.init n (fun _ -> Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0)
  in
  let fds = List.map (fun (a, _) -> Lwt_unix.of_unix_file_descr a) pairs in
  List.iter
    (fun fd ->
      Lwt.async (fun () ->
        Lwt.catch (fun () -> Lwt_unix.wait_read fd) (fun _ -> Lwt.return_unit)))
    fds;
  Lwt_main.run (Lwt.pause ());
  Lwt_main.run (Lwt_list.iter_p Lwt_unix.close fds);
  Lwt_main.run (Lwt_unix.sleep 0.2);
  let at_eof (_, peer) =
    match Unix.select [ peer ] [] [] 0. with
    | [], _, _ -> false
    | _ -> Unix.read peer (Bytes.create 1) 0 1 = 0
  in
  let closed = List.length (List.filter at_eof pairs) in
  List.iter (fun (_, peer) -> Unix.close peer) pairs;
  Lwt_uring.set ();
  if closed <> n then Printf.printf "# %d of %d sockets closed\n%!" closed n;
  check "a storm of cancels larger than the queue loses none" (closed = n)

(* More descriptors ready at once than the submission queue holds (256 by
   default), each watched by an event that stays active and fires on every lap
   while its descriptor is ready, as a level-triggered engine does. The loop
   must still return to run other work, here a timer. An alarm turns a hang
   into a failure. *)
let test_many_ready () =
  let n = 300 in
  let pairs =
    List.init n (fun _ -> Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0)
  in
  List.iter (fun (_, b) -> ignore (Unix.write_substring b "x" 0 1)) pairs;
  let fired = ref 0 in
  let events =
    List.map (fun (a, _) -> Lwt_engine.on_readable a (fun _ -> incr fired)) pairs
  in
  let previous =
    Sys.signal Sys.sigalrm
      (Sys.Signal_handle
         (fun _ ->
           print_endline "FAIL - more ready descriptors than the queue: hang";
           exit 1))
  in
  ignore (Unix.alarm 10);
  Lwt_main.run (Lwt_unix.sleep 0.05);
  ignore (Unix.alarm 0);
  Sys.set_signal Sys.sigalrm previous;
  List.iter Lwt_engine.stop_event events;
  List.iter (fun (a, b) -> Unix.close a; Unix.close b) pairs;
  check "more ready descriptors than the queue holds do not stall the loop"
    (!fired >= n)

(* Fail rather than hang: [f] runs under an alarm of [seconds]. *)
let under_alarm seconds name f =
  let previous =
    Sys.signal Sys.sigalrm
      (Sys.Signal_handle
         (fun _ ->
           Printf.printf "FAIL - %s: the loop hung\n%!" name;
           exit 1))
  in
  ignore (Unix.alarm seconds);
  let v = f () in
  ignore (Unix.alarm 0);
  Sys.set_signal Sys.sigalrm previous;
  v

(* More descriptors ready at once than the submission queue holds, each with a
   Lwt_unix.wait_read: where the core defers wakeups, the event a waiter leaves
   stays active through the lap, so this is the livelock through the plain
   Lwt_unix API. Every waiter must be woken, and the loop must go on. *)
let test_many_ready_polls () =
  let n = 300 in
  let pairs =
    List.init n (fun _ -> Lwt_unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0)
  in
  let woken =
    under_alarm 10 "300 ready wait_read" (fun () ->
      Lwt_main.run
        (Lwt.pick
           [ (let waits = List.map (fun (a, _) -> Lwt_unix.wait_read a) pairs in
              Lwt_unix.sleep 0.02 >>= fun () ->
              List.iter
                (fun (_, b) ->
                  ignore
                    (Unix.write_substring (Lwt_unix.unix_file_descr b) "x" 0 1))
                pairs;
              Lwt.join waits >|= fun () -> true);
             (Lwt_unix.sleep 5. >|= fun () -> false) ]))
  in
  List.iter
    (fun (a, b) ->
      Lwt_main.run (Lwt_unix.close a >>= fun () -> Lwt_unix.close b))
    pairs;
  check "more ready descriptors than the queue holds: all woken" woken

(* Tearing the engine down while a recv on an idle socket and an accept wait:
   the teardown must return, and the read's promise be rejected. *)
let test_teardown_in_flight () =
  let a, b = Lwt_unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let pending_read = Lwt_unix.read a (Bytes.create 8) 0 8 in
  let listener = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_main.run
    (Lwt_unix.bind listener (Unix.ADDR_INET (Unix.inet_addr_loopback, 0))
     >>= fun () ->
     Lwt_unix.listen listener 8;
     let (_ : (Lwt_unix.file_descr * Unix.sockaddr) Lwt.t) =
       Lwt_unix.accept listener
     in
     Lwt_unix.sleep 0.02);
  under_alarm 10 "teardown" (fun () ->
    Lwt_engine.set (new Lwt_engine.select);
    Lwt_main.run (Lwt_unix.sleep 0.01));
  check "and the read in flight is rejected"
    (match Lwt.state pending_read with Lwt.Fail _ -> true | _ -> false);
  Lwt_uring.set ();
  Lwt_main.run
    (Lwt_list.iter_p Lwt_unix.close [ a; b; listener ])

(* Completion-based I/O (Lwt_uring.Io): the kernel performs the transfer; no
   readiness wait. *)
let test_io_socketpair () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let msg = Bytes.of_string "completion!" in
  let n = Bytes.length msg in
  let got = Bytes.create n in
  let written, read =
    Lwt_main.run begin
      Lwt_uring.Io.write a msg 0 n >>= fun written ->
      Lwt_uring.Io.read b got 0 n >>= fun read ->
      Lwt.return (written, read)
    end
  in
  check "Io completion-based socketpair round-trip"
    (written = n && read = n && Bytes.equal got msg);
  Unix.close a;
  Unix.close b

(* Out-of-bounds positions are rejected before anything is submitted. *)
let test_io_bounds () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let rejected f =
    match f () with
    | (_ : int Lwt.t) -> false
    | exception Invalid_argument _ -> true
  in
  let buf = Bytes.create 4 in
  check "Io.read and Io.write check their bounds"
    (rejected (fun () -> Lwt_uring.Io.read a buf 2 8)
     && rejected (fun () -> Lwt_uring.Io.write b buf (-1) 1));
  Unix.close a;
  Unix.close b

(* io_uring can read a regular file asynchronously: readiness engines cannot
   poll regular files at all. *)
let test_io_regular_file () =
  let path = Filename.temp_file "lwt_uring_test" ".dat" in
  let contents = "the quick brown fox" in
  let oc = open_out_bin path in
  output_string oc contents;
  close_out oc;
  let n = String.length contents in
  let fd = Unix.openfile path [ Unix.O_RDONLY ] 0 in
  let buf = Bytes.create n in
  let read = Lwt_main.run (Lwt_uring.Io.read fd buf 0 n) in
  Unix.close fd;
  Sys.remove path;
  check "Io reads a regular file asynchronously"
    (read = n && Bytes.equal buf (Bytes.of_string contents))

let test_io_bigarray () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let n = 8 in
  let src = Bigarray.Array1.create Bigarray.char Bigarray.c_layout n in
  for i = 0 to n - 1 do Bigarray.Array1.set src i (Char.chr (65 + i)) done;
  let dst = Bigarray.Array1.create Bigarray.char Bigarray.c_layout n in
  let ok =
    Lwt_main.run begin
      Lwt_uring.Io.write_bigarray a src 0 n >>= fun _ ->
      Lwt_uring.Io.read_bigarray b dst 0 n >>= fun r ->
      Lwt.return (r = n)
    end
  in
  let same = ref ok in
  for i = 0 to n - 1 do
    if Bigarray.Array1.get src i <> Bigarray.Array1.get dst i then same := false
  done;
  check "Io zero-copy bigarray round-trip" !same;
  Unix.close a;
  Unix.close b

(* After [Lwt_unix.dup2] puts a regular file under a descriptor that held a
   socket, a routed write uses the operation for files, not the cached one for
   sockets. *)
let test_dup2_kind () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let sock = Lwt_unix.of_unix_file_descr a in
  ignore (Lwt_main.run (Lwt_unix.write_string sock "x" 0 1));
  let path = Filename.temp_file "lwt_uring_test" ".dat" in
  let file =
    Lwt_unix.of_unix_file_descr
      (Unix.openfile path [ Unix.O_WRONLY; Unix.O_TRUNC ] 0o600)
  in
  Lwt_unix.dup2 file sock;
  let written =
    Lwt_main.run
      (Lwt.catch
         (fun () -> Lwt_unix.write_string sock "file" 0 4)
         (fun _ -> Lwt.return (-1)))
  in
  Lwt_main.run (Lwt_unix.close sock >>= fun () -> Lwt_unix.close file);
  Unix.close b;
  let ic = open_in_bin path in
  let contents = really_input_string ic (in_channel_length ic) in
  close_in ic;
  Sys.remove path;
  check "dup2 forgets the cached kind of the replaced file"
    (written = 4 && contents = "file")

(* A routed write to a socket whose peer has closed fails with EPIPE, and does
   not raise SIGPIPE, which by default would kill the process. *)
let test_epipe () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.close b;
  let a = Lwt_unix.of_unix_file_descr a in
  let outcome =
    Lwt_main.run
      (Lwt.catch
         (fun () -> Lwt_unix.write_string a "x" 0 1 >|= fun _ -> `Written)
         (function
           | Unix.Unix_error (Unix.EPIPE, _, _) -> Lwt.return `Epipe
           | _ -> Lwt.return `Other))
  in
  Lwt_main.run (Lwt_unix.close a);
  check "a write to a closed peer fails with EPIPE, without SIGPIPE"
    (outcome = `Epipe)

(* Lwt_unix.connect routed through io_uring (IORING_OP_CONNECT): a loopback TCP
   connection is established through the ring (the accept side uses the default
   path, whose readiness already runs on the engine), then exchanges data. *)
let test_connect () =
  let lsock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt lsock Unix.SO_REUSEADDR true;
  let msg = Bytes.of_string "uring-connect" in
  let n = Bytes.length msg in
  let got = Bytes.create n in
  let read =
    Lwt_main.run begin
      Lwt_unix.bind lsock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0))
      >>= fun () ->
      Lwt_unix.listen lsock 1;
      let addr = Lwt_unix.getsockname lsock in
      let server =
        Lwt_unix.accept lsock >>= fun (fd, _) ->
        Lwt_unix.read fd got 0 n >>= fun r ->
        Lwt_unix.close fd >>= fun () -> Lwt.return r
      in
      let client =
        let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
        Lwt_unix.connect fd addr >>= fun () ->
        Lwt_unix.write fd msg 0 n >>= fun _ -> Lwt_unix.close fd
      in
      Lwt.both server client >>= fun (r, ()) -> Lwt.return r
    end
  in
  check "connect routed through io_uring" (read = n && Bytes.equal got msg);
  Lwt_main.run (Lwt_unix.close lsock)

(* The outcome of [p] after one more lap of the loop: [`Pending] if it is still
   waiting. *)
let settled p =
  Lwt_main.run
    (Lwt.pick
       [ Lwt.catch
           (fun () -> p >|= fun _ -> `Resolved)
           (fun e -> Lwt.return (`Rejected e));
         (Lwt_unix.sleep 0.2 >|= fun () -> `Pending) ])

(* Closing or aborting a descriptor fails the read in flight on it, as the
   default path fails its waiters, and the kernel lets go of the file, so the
   peer sees end of file. *)
let test_close_fails_io () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let a = Lwt_unix.of_unix_file_descr a in
  let read = Lwt_unix.read a (Bytes.create 1) 0 1 in
  Lwt_main.run (Lwt.pause ());
  Lwt.async (fun () -> Lwt_unix.close a);
  let outcome = settled read in
  let peer_at_eof =
    match Unix.select [ b ] [] [] 0.2 with
    | [], _, _ -> false
    | _ -> Unix.read b (Bytes.create 1) 0 1 = 0
  in
  Unix.close b;
  check "close fails the read in flight with EBADF"
    (match outcome with
     | `Rejected (Unix.Unix_error (Unix.EBADF, _, _)) -> true
     | _ -> false);
  check "after close, the peer sees end of file" peer_at_eof;
  let c, d = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let c = Lwt_unix.of_unix_file_descr c in
  let read = Lwt_unix.read c (Bytes.create 1) 0 1 in
  Lwt_main.run (Lwt.pause ());
  Lwt_unix.abort c Exit;
  let outcome = settled read in
  Lwt_main.run (Lwt_unix.close c);
  Unix.close d;
  check "abort fails the read in flight with its exception"
    (match outcome with `Rejected Exit -> true | _ -> false)

(* A write prepared just before [close] must not reach the next file to get the
   same descriptor number: the kernel resolves the number when the operation is
   submitted. *)
let test_close_then_reuse () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let a = Lwt_unix.of_unix_file_descr a in
  let write =
    Lwt.catch
      (fun () -> Lwt_unix.write_string a "SECRET" 0 6 >|= ignore)
      (fun _ -> Lwt.return_unit)
  in
  let close = Lwt_unix.close a in
  (* Let the worker close the file, then take its number, before the loop runs
     again. *)
  Unix.sleepf 0.05;
  let c, d = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Lwt_main.run (Lwt.join [ write; close ]);
  let leaked =
    match Unix.select [ d ] [] [] 0.1 with [], _, _ -> false | _ -> true
  in
  List.iter Unix.close [ b; c; d ];
  check "a write prepared before close does not reach the next owner"
    (not leaked)

(* Cancelled in the lap that requested them, a read and a write never reach the
   kernel, as with the default engines: no byte is taken or sent, and an Lwt_io
   channel loses nothing. *)
let test_cancel_before_submission () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  ignore (Unix.write_substring b "ping" 0 4);
  let a = Lwt_unix.of_unix_file_descr a in
  let buf = Bytes.make 4 '.' in
  let read = Lwt_unix.read a buf 0 4 in
  Lwt.cancel read;
  let write = Lwt_unix.write_string a "pong" 0 4 in
  Lwt.cancel write;
  Lwt_main.run (Lwt_unix.sleep 0.05);
  let peer_got_nothing =
    match Unix.select [ b ] [] [] 0. with [], _, _ -> true | _ -> false
  in
  let ic = Lwt_io.of_fd ~mode:Lwt_io.input a in
  let first = Lwt_io.read ~count:4 ic in
  Lwt.cancel first;
  let next = Lwt_main.run (Lwt_io.read ~count:4 ic) in
  Lwt_main.run (Lwt_io.close ic);
  Unix.close b;
  check "a read or write cancelled before submission never happens"
    (Bytes.to_string buf = "...." && peer_got_nothing && next = "ping")

(* A read cancelled once in the kernel, while the data arrives: whichever wins,
   the next read gets the data, even if it is requested before the cancelled
   read has completed. *)
let test_cancel_in_flight () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let a = Lwt_unix.of_unix_file_descr a in
  let first = Lwt_unix.read a (Bytes.create 4) 0 4 in
  Lwt_main.run (Lwt_unix.sleep 0.01);
  ignore (Unix.write_substring b "ping" 0 4);
  Lwt.cancel first;
  let buf = Bytes.make 4 '.' in
  let next =
    Lwt_main.run
      (Lwt.pick
         [ Lwt_unix.read a buf 0 4;
           (Lwt_unix.sleep 1. >|= fun () -> -1) ])
  in
  check "a read cancelled in the kernel loses no data"
    (Lwt.state first = Lwt.Fail Lwt.Canceled
     && next = 4 && Bytes.to_string buf = "ping");
  (* The same through Lwt_io, with a timeout that cancels a waiting read. *)
  let ic = Lwt_io.of_fd ~mode:Lwt_io.input a in
  let timed_out =
    Lwt_main.run
      (Lwt.pick
         [ (Lwt_io.read ~count:4 ic >|= fun _ -> false);
           (Lwt_unix.sleep 0.05 >|= fun () -> true) ])
  in
  ignore (Unix.write_substring b "pong" 0 4);
  let after = Lwt_main.run (Lwt_io.read ~count:4 ic) in
  check "an Lwt_io read cut by a timeout leaves the stream intact"
    (timed_out && after = "pong");
  (* Closing fails a read held behind a cancelled one. *)
  let held_first = Lwt_unix.read a (Bytes.create 4) 0 4 in
  Lwt_main.run (Lwt_unix.sleep 0.01);
  Lwt.cancel held_first;
  let held = Lwt_unix.read a (Bytes.create 4) 0 4 in
  Lwt.async (fun () -> Lwt_unix.close a);
  let outcome = settled held in
  Unix.close b;
  check "close fails a read held behind a cancelled one"
    (match outcome with
     | `Rejected (Unix.Unix_error (Unix.EBADF, _, _)) -> true
     | _ -> false)

(* With [~deferred], the kernel posts completions only when the ring is entered
   asking for events: a loop that never goes idle must still get its I/O. A
   system thread writes 0.1 s after the read is submitted, while the loop keeps
   pausing. *)
let test_deferred () =
  Lwt_uring.set ~deferred:true ();
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let a = Lwt_unix.of_unix_file_descr a in
  let read = Lwt_unix.read a (Bytes.create 4) 0 4 in
  let writer =
    Thread.create
      (fun () ->
        Thread.delay 0.1;
        ignore (Unix.write_substring b "ping" 0 4))
      ()
  in
  let t0 = Unix.gettimeofday () in
  let rec spin () =
    if Lwt.is_sleeping read && Unix.gettimeofday () -. t0 < 2. then
      Lwt.pause () >>= spin
    else Lwt.return_unit
  in
  Lwt_main.run (spin ());
  Thread.join writer;
  let got = Lwt.state read = Lwt.Return 4 in
  Lwt_main.run (Lwt_unix.close a);
  Unix.close b;
  Lwt_uring.set ();
  check "with ~deferred, a loop that never idles still gets its I/O" got

(* Replacing a uring engine that watches a descriptor and has a completion-based
   read in flight: the readiness wait moves to the new engine, and the read is
   cancelled, its promise rejected with [ECANCELED]. *)
let test_replace_busy_engine () =
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let c, d = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let a = Lwt_unix.of_unix_file_descr a
  and b = Lwt_unix.of_unix_file_descr b
  and c = Lwt_unix.of_unix_file_descr c
  and d = Lwt_unix.of_unix_file_descr d in
  let pending_read = Lwt_unix.read b (Bytes.create 1) 0 1 in
  let readable = Lwt_unix.wait_read d in
  let replaced =
    match Lwt_uring.set () with
    | () -> true
    | exception e ->
      Printf.printf "# replacing the engine raised %s\n%!"
        (Printexc.to_string e);
      false
  in
  let read_outcome, waited =
    Lwt_main.run begin
      Lwt_unix.write_string c "x" 0 1 >>= fun _ ->
      Lwt.both
        (Lwt.catch
           (fun () -> pending_read >|= fun _ -> `Read)
           (function
             | Lwt.Canceled -> Lwt.return `Cancelled
             | e -> Lwt.return (`Failed e)))
        (readable >|= fun () -> true)
    end
  in
  check "replacing a busy uring engine cancels its in-flight I/O"
    (replaced && read_outcome = `Cancelled && waited);
  Lwt_main.run
    (Lwt_list.iter_p Lwt_unix.close [ a; b; c; d ])

(* A callback run by the engine may replace it: the timer's continuation installs
   a fresh uring engine, which destroys the one that is reaping. *)
let test_replace_from_callback () =
  let ok =
    match
      Lwt_main.run begin
        Lwt_unix.sleep 0.001 >>= fun () ->
        Lwt_uring.set ();
        Lwt_unix.sleep 0.001
      end
    with
    | () -> true
    | exception e ->
      Printf.printf "# %s\n%!" (Printexc.to_string e);
      false
  in
  check "a uring engine can be replaced from one of its callbacks" ok

(* The child of [Lwt_unix.fork] gets a ring of its own and keeps running on
   io_uring (here a timer and a routed write), while the parent goes on using
   the inherited ring to read the child's reply. *)
let test_fork () =
  let r, w = Unix.pipe () in
  match Lwt_unix.fork () with
  | 0 ->
    Unix.close r;
    let w = Lwt_unix.of_unix_file_descr w in
    let reply =
      match Lwt_engine.id () with
      | Lwt_uring.Engine_id__uring -> "uring"
      | _ -> "other"
    in
    Lwt_main.run begin
      Lwt_unix.sleep 0.01 >>= fun () ->
      Lwt_unix.write_string w reply 0 (String.length reply) >>= fun _ ->
      Lwt_unix.close w
    end;
    Unix._exit 0
  | pid ->
    Unix.close w;
    let r = Lwt_unix.of_unix_file_descr r in
    let buf = Bytes.create 5 in
    let n = Lwt_main.run (Lwt_unix.read r buf 0 5) in
    let _, status = Unix.waitpid [] pid in
    Lwt_main.run (Lwt_unix.close r);
    check "the child of a fork runs Lwt on a ring of its own"
      (n = 5 && Bytes.sub_string buf 0 n = "uring" && status = Unix.WEXITED 0)


(* Where io_uring is unavailable (another system, a switch without the uring
   library, or a kernel that lacks or forbids io_uring), the package says so and
   leaves the current engine alone. *)
let test_unavailable () =
  let before = Lwt_engine.id () in
  check "set raises Lwt_sys.Not_available"
    (match Lwt_uring.set () with
     | () -> false
     | exception Lwt_sys.Not_available _ -> true);
  check "the engine is left alone" (Lwt_engine.id () = before);
  check "set_if_available declines" (not (Lwt_uring.set_if_available ()));
  check "the engine is still left alone" (Lwt_engine.id () = before)

(* An engine that is created but not installed takes no I/O: a read runs on the
   current engine. *)
let test_created_not_installed () =
  let unused = new Lwt_uring.uring () in
  let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  let a = Lwt_unix.of_unix_file_descr a in
  ignore (Unix.write_substring b "ping" 0 4);
  let n =
    Lwt_main.run
      (Lwt.pick
         [ Lwt_unix.read a (Bytes.create 4) 0 4;
           (Lwt_unix.sleep 1. >|= fun () -> -1) ])
  in
  unused#destroy;
  Lwt_main.run (Lwt_unix.close a);
  Unix.close b;
  check "an engine created but not installed takes no I/O" (n = 4)

let is_uring () =
  match Lwt_engine.id () with Lwt_uring.Engine_id__uring -> true | _ -> false

let test_available () =
  test_created_not_installed ();
  Unix.putenv "LWT_URING" "0";
  check "LWT_URING=0 makes set_if_available decline"
    (not (Lwt_uring.set_if_available ()) && not (is_uring ()));
  Unix.putenv "LWT_URING" "1";
  check "set_if_available installs the engine"
    (Lwt_uring.set_if_available () && is_uring ());
  Lwt_uring.set ();
  (match Lwt_engine.id () with
   | Lwt_uring.Engine_id__uring -> check "uring engine installed" true
   | _ -> check "uring engine installed" false);
  test_timer ();
  test_timer_order ();
  test_huge_timers ();
  test_socketpair ();
  test_cancel ();
  test_pause_and_timer ();
  test_cancel_storm ();
  test_many_ready ();
  test_many_ready_polls ();
  test_io_socketpair ();
  test_io_bounds ();
  test_io_regular_file ();
  test_io_bigarray ();
  test_connect ();
  test_epipe ();
  test_dup2_kind ();
  test_close_fails_io ();
  test_close_then_reuse ();
  test_cancel_before_submission ();
  test_cancel_in_flight ();
  test_deferred ();
  test_replace_busy_engine ();
  test_replace_from_callback ();
  test_teardown_in_flight ();
  test_fork ()

let () =
  if Lwt_uring.available () then test_available ()
  else begin
    print_endline "# io_uring is not available here: testing the fallback";
    test_unavailable ()
  end;
  if !failures = 0 then Printf.printf "\nAll tests passed.\n%!"
  else begin
    Printf.printf "\n%d test(s) failed.\n%!" !failures;
    exit 1
  end
