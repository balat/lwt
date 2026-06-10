(* Tests for Lwt interoperability of the Lwt_effects scheduler. *)

open Lwt_effects
open Lwt_effects.Syntax

let failures = ref 0

let check name cond =
  if cond then Printf.printf "ok   - %s\n%!" name
  else begin
    incr failures;
    Printf.printf "FAIL - %s\n%!" name
  end

(* Await a real Lwt_unix timer from inside a fiber. *)
let () =
  let r =
    run (fun () ->
      let* () = of_lwt (Lwt_unix.sleep 0.01) in
      return 42)
  in
  check "await_lwt Lwt_unix.sleep" (r = 42)

(* A Lwt computation that uses Lwt.pause must make progress under our idle. *)
let () =
  let r =
    run (fun () ->
      return
        (await_lwt
           (let open Lwt.Infix in
            Lwt.pause () >>= fun () ->
            Lwt.pause () >>= fun () -> Lwt.return "paused-ok")))
  in
  check "await_lwt with Lwt.pause chain" (r = "paused-ok")

(* A rejected Lwt promise raises through await_lwt. *)
let () =
  let r =
    run (fun () ->
      catch
        (fun () ->
          let _ : unit = await_lwt (Lwt.fail (Failure "boom")) in
          return "no")
        (fun e ->
          match e with Failure m -> return ("caught:" ^ m) | _ -> return "wrong"))
  in
  check "await_lwt propagates rejection" (r = "caught:boom")

(* Round trip: effect promise -> Lwt.t -> effect promise, all under our run. *)
let () =
  let r =
    run (fun () ->
      let ep = async (fun () -> let+ () = pause () in 7) in
      await_lwt (to_lwt ep) |> return)
  in
  check "to_lwt / of_lwt round trip" (r = 7)

(* Concurrency between a fiber awaiting Lwt and a native fiber. *)
let () =
  let order = ref [] in
  run (fun () ->
    let a =
      async (fun () ->
        let* () = of_lwt (Lwt_unix.sleep 0.02) in
        order := "lwt" :: !order;
        return ())
    in
    let b =
      async (fun () ->
        let* () = sleep 0.005 in
        order := "eff" :: !order;
        return ())
    in
    let* () = a in
    b);
  check "Lwt and effect fibers interleave" (List.rev !order = [ "eff"; "lwt" ])

let () =
  if !failures = 0 then print_endline "\nAll interop tests passed."
  else begin
    Printf.printf "\n%d interop test(s) failed.\n" !failures;
    exit 1
  end
