(* Correctness tests for the io_uring back end (Linux only). *)

open Lwt_effects
open Lwt_effects.Syntax

let failures = ref 0

let check name cond =
  if cond then Printf.printf "ok   - %s\n" name
  else begin
    incr failures;
    Printf.printf "FAIL - %s\n" name
  end

(* Cstruct-based I/O over a socketpair, with the scheduler driven by io_uring. *)
let () =
  let r =
    Lwt_effects_uring.run (fun () ->
      let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
      let msg = "hello uring" in
      let writer =
        async (fun () ->
          let buf = Cstruct.of_string msg in
          let n = Lwt_effects_uring.Io.write a buf in
          return n)
      in
      let reader =
        async (fun () ->
          let buf = Cstruct.create (String.length msg) in
          let n = Lwt_effects_uring.Io.read b buf in
          return (Cstruct.to_string (Cstruct.sub buf 0 n)))
      in
      let* s, _ = both reader writer in
      Unix.close a;
      Unix.close b;
      return s)
  in
  check "io_uring Cstruct read/write" (r = "hello uring")

(* Zero-copy I/O through the registered fixed buffer. *)
let () =
  let r =
    Lwt_effects_uring.run (fun () ->
      let module F = Lwt_effects_uring.Fixed in
      let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
      let msg = "fixed buffer" in
      let writer =
        async (fun () ->
          let c = F.alloc () in
          F.blit_string msg c;
          let n = F.write ~len:(String.length msg) a c in
          F.free c;
          return n)
      in
      let reader =
        async (fun () ->
          let c = F.alloc () in
          let n = F.read ~len:(String.length msg) b c in
          let s = F.to_string ~len:n c in
          F.free c;
          return s)
      in
      let* s, _ = both reader writer in
      Unix.close a;
      Unix.close b;
      return s)
  in
  check "io_uring fixed-buffer read/write" (r = "fixed buffer")

(* accept/connect over a loopback TCP socket, driven by io_uring poll. *)
let () =
  let r =
    Lwt_effects_uring.run (fun () ->
      let ls = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Unix.setsockopt ls Unix.SO_REUSEADDR true;
      Unix.bind ls (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
      Unix.listen ls 1;
      Unix.set_nonblock ls;
      let port =
        match Unix.getsockname ls with
        | Unix.ADDR_INET (_, p) -> p
        | Unix.ADDR_UNIX _ -> failwith "expected inet"
      in
      let server =
        async (fun () ->
          let c, _ = Lwt_effects_uring.Io.accept ls in
          let buf = Cstruct.create 16 in
          let n = Lwt_effects_uring.Io.read c buf in
          Unix.close c;
          return (Cstruct.to_string (Cstruct.sub buf 0 n)))
      in
      let client =
        async (fun () ->
          let s = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
          Unix.set_nonblock s;
          Lwt_effects_uring.Io.connect s
            (Unix.ADDR_INET (Unix.inet_addr_loopback, port));
          let n = Lwt_effects_uring.Io.write s (Cstruct.of_string "tcp ok") in
          Unix.close s;
          return n)
      in
      let* msg = server in
      let* _ = client in
      Unix.close ls;
      return msg)
  in
  check "io_uring accept/connect (loopback TCP)" (r = "tcp ok")

(* Monadic (non-blocking, async-typed) io_uring I/O + Compat bind. *)
let () =
  let r =
    Lwt_effects_uring.run (fun () ->
      let open Lwt_effects in
      let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
      let wbuf = Cstruct.of_string "monadic uring" in
      let rbuf = Cstruct.create 32 in
      let writer =
        async (fun () ->
          Lwt_effects_uring.Io.write_m a wbuf >>= fun _ -> Lwt_effects.return_unit)
      in
      let reader =
        async (fun () ->
          Lwt_effects_uring.Io.read_m b rbuf >>= fun n ->
          Lwt_effects.return (Cstruct.to_string (Cstruct.sub rbuf 0 n)))
      in
      both reader writer >>= fun (s, _) ->
      Unix.close a;
      Unix.close b;
      Lwt_effects.return s)
  in
  check "io_uring monadic read_m/write_m" (r = "monadic uring")

let () =
  if !failures = 0 then print_endline "\nAll io_uring tests passed."
  else begin
    Printf.printf "\n%d io_uring test(s) failed.\n" !failures;
    exit 1
  end
