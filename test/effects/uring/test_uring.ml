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

let () =
  if !failures = 0 then print_endline "\nAll io_uring tests passed."
  else begin
    Printf.printf "\n%d io_uring test(s) failed.\n" !failures;
    exit 1
  end
