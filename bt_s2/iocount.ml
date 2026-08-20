(* Instruction-count probe for S2's two hot placements, on the same principle as
   S0's: run with N and 2N and subtract, so startup and harness cost cancel and
   what is left is the marginal cost per operation. Callgrind makes the
   difference exact instead of statistical.

   - "chan": the buffered single-character path of Lwt_io, on a channel over a
     bigarray, so there is no I/O at all and the count is the fast path plus the
     ownership check step 8 added to it.
   - "fd":   Lwt_unix.write then read of one byte over a socketpair, which is the
     descriptor path: check_descriptor, plus the same check. Callgrind counts
     USER instructions only, so the kernel side of the two syscalls is invisible;
     read the delta as instructions, not as time.
*)

let () =
  let which = Sys.argv.(1) in
  let n = int_of_string Sys.argv.(2) in
  match which with
  | "chan" ->
    let buf = Lwt_bytes.create (4 * 1024 * 1024) in
    let oc = Lwt_io.of_bytes ~mode:Lwt_io.output buf in
    for i = 1 to n do
      ignore (Lwt_io.write_char oc (Char.chr (i land 255)))
    done;
    Printf.printf "chan %d chars\n" n
  | "fd" ->
    (* ONE [Lwt_main.run] around the whole loop: calling it per round-trip would
       measure the loop's own entry cost, which real code pays once. *)
    let a, b = Lwt_unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
    let out = Bytes.of_string "x" and inp = Bytes.create 1 in
    let rec go i acc =
      if i = 0 then Lwt.return acc
      else
        Lwt.bind (Lwt_unix.write b out 0 1) (fun _ ->
          Lwt.bind (Lwt_unix.read a inp 0 1) (fun k -> go (i - 1) (acc + k)))
    in
    let total = Lwt_main.run (go n 0) in
    Printf.printf "fd %d round-trips, %d bytes\n" n total
  | _ -> assert false
