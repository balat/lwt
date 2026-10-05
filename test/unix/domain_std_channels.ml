(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The standard channels from every loop. Lwt_io.stdout and the others are
   created when the module is initialised, on the main domain, and used to
   belong to it: printing from another loop raised Invalid_argument. Each now
   stands for one channel per domain, over the same descriptor, flushed when
   that domain's loop is retired.

   Standard output is redirected to a file for the duration, so that what each
   domain printed can be read back. Needs other domains, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let lines = 20

(* Prints from a loop of its own, with neither a flush nor a close: what is
   printed must come out when the domain's loop is retired. *)
let printer name () =
  Lwt_main.run
    (let rec go i =
       if i = lines then Lwt.return_unit
       else
         Lwt.bind (Lwt_io.printf "%s io %d\n" name i) (fun () ->
           Lwt.bind (Lwt_fmt.printf "%s fmt %d@." name i) (fun () ->
             go (i + 1)))
     in
     go 0)

let read_file path =
  let ic = open_in_bin path in
  let n = in_channel_length ic in
  let s = really_input_string ic n in
  close_in ic;
  s

let () =
  ignore (Unix.alarm 30);
  let path = Filename.temp_file "lwt_std" ".out" in
  let saved = Unix.dup Unix.stdout in
  let file = Unix.openfile path [ Unix.O_WRONLY; Unix.O_TRUNC ] 0o600 in
  Unix.dup2 file Unix.stdout;
  Unix.close file;

  let raised = Atomic.make None in
  let guard name f () =
    try f () with exn -> Atomic.set raised (Some (name ^ ": " ^ Printexc.to_string exn))
  in
  let a = Domain.spawn (guard "A" (printer "A")) in
  let b = Domain.spawn (guard "B" (printer "B")) in
  printer "M" ();
  Domain.join a;
  Domain.join b;

  (* Closing another domain's standard output leaves the descriptor open. *)
  Domain.join
    (Domain.spawn
       (guard "C" (fun () -> Lwt_main.run (Lwt_io.close Lwt_io.stdout))));
  Lwt_main.run (Lwt_io.printf "after\n");
  Lwt_main.run (Lwt_io.flush Lwt_io.stdout);

  Unix.dup2 saved Unix.stdout;
  Unix.close saved;
  let out = read_file path in
  Sys.remove path;

  check
    (match Atomic.get raised with
     | None -> "no domain raised"
     | Some e -> "no domain raised, but " ^ e)
    (Atomic.get raised = None);
  let all = String.split_on_char '\n' out in
  let has l = List.mem l all in
  List.iter
    (fun name ->
      for i = 0 to lines - 1 do
        check (Printf.sprintf "%s io %d is printed, whole" name i)
          (has (Printf.sprintf "%s io %d" name i));
        check (Printf.sprintf "%s fmt %d is printed, whole" name i)
          (has (Printf.sprintf "%s fmt %d" name i))
      done)
    [ "A"; "B"; "M" ];
  check "the main domain prints after another closed its stdout" (has "after");

  (* And standard input, from another loop. *)
  let input = Filename.temp_file "lwt_std" ".in" in
  let oc = open_out_bin input in
  output_string oc "first\nsecond\n";
  close_out oc;
  let saved_in = Unix.dup Unix.stdin in
  let fd = Unix.openfile input [ Unix.O_RDONLY ] 0 in
  Unix.dup2 fd Unix.stdin;
  Unix.close fd;
  let line =
    Domain.join
      (Domain.spawn (fun () ->
         try Lwt_main.run (Lwt_io.read_line Lwt_io.stdin)
         with exn -> "raised " ^ Printexc.to_string exn))
  in
  Unix.dup2 saved_in Unix.stdin;
  Unix.close saved_in;
  Sys.remove input;
  check (Printf.sprintf "another loop reads standard input (%S)" line)
    (line = "first");

  if !failures > 0 then exit 1;
  print_endline "the standard channels from every loop: ok"
