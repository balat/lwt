open Test
open Lwt

(* Used for the "structure let" test, below. This is wrapped up by the PPX in a
   call to Lwt_main.run, which is executed at module load time. We can't use a
   local module inside the tester function, because that function is run inside
   an outer call to Lwt_main.run, and nested calls to Lwt_main.run are not
   allowed. *)
[@@@ocaml.warning "-22"]
let%lwt structure_let_result : bool = Lwt.return_true
[@@@ocaml.warning "+22"]

let __trace_ctxt = "test" (* TODO: figure out how to make this implicit *)

let suite = suite "ppx" [
  test "let"
    (fun () ->
       let%lwt x = return 3 in
       return (x + 1 = 4)
    ) ;

  test "nested let"
    (fun () ->
       let%lwt x = return 3 in
       let%lwt y = return 4 in
       return (x + y = 7)
    ) ;

  test "and let"
    (fun () ->
       let%lwt x = return 3
       and y = return 4 in
       return (x + y = 7)
    ) ;

  test "match"
    (fun () ->
       let x = Lwt.return (Some 3) in
       match%lwt x with
       | Some x -> return (x + 1 = 4)
       | None -> return false
    ) ;

  test "match-exn"
    (fun () ->
       let x = Lwt.return (Some 3) in
       let x' = Lwt.fail Not_found in
       let%lwt a =
         match%lwt x with
         | exception Not_found -> return false
         | Some x -> return (x = 3)
         | None -> return false
       and b =
         match%lwt x' with
         | exception Not_found -> return true
         | _ -> return false
       in
       Lwt.return (a && b)
    ) ;

  test "if"
    (fun () ->
       let x = Lwt.return_true in
       let%lwt a =
         if%lwt x then Lwt.return_true else Lwt.return_false
       in
       let%lwt b =
         if%lwt x>|= not then Lwt.return_false else Lwt.return_true
       in
       (if%lwt x >|= not then Lwt.return_unit) >>= fun () ->
       Lwt.return (a && b)
    ) ;

  test "for" (* Test for proper sequencing *)
    (fun () ->
       let r = ref [] in
       let f x =
         let%lwt () = Lwt_unix.sleep 0.2 in Lwt.return (r := x :: !r)
       in
       let%lwt () =
         for%lwt x = 3 to 5 do f x done
       in return (!r = [5 ; 4 ; 3])
    ) ;

  test "while" (* Test for proper sequencing *)
    (fun () ->
       let r = ref [] in
       let f x =
         let%lwt () = Lwt_unix.sleep 0.2 in Lwt.return (r := x :: !r)
       in
       let%lwt () =
         let c = ref 2 in
         while%lwt !c < 5 do incr c ; f !c done
       in return (!r = [5 ; 4 ; 3])
    ) ;

  test "assert"
    (fun () ->
       let%lwt () = assert%lwt true
       in return true
    ) ;

  test "try"
    (fun () ->
       try%lwt
         Lwt.fail Not_found
       with _ -> return true
    ) [@warning("@8@11")] ;

  test "try raise"
    (fun () ->
       try%lwt
         raise Not_found
       with _ -> return true
    ) [@warning("@8@11")] ;

  test "try fallback"
    (fun () ->
       try%lwt
         try%lwt
           Lwt.fail Not_found
         with Failure _ -> return false
       with Not_found -> return true
    ) [@warning("@8@11")] ;

  test "finally body"
    (fun () ->
       let x = ref false in
       begin
         (try%lwt
           return_unit
         with
         | _ -> return_unit
         ) [%finally x := true; return_unit]
       end >>= fun () ->
       return !x
    ) ;

  test "finally exn"
    (fun () ->
       let x = ref false in
       begin
         (try%lwt
           raise Not_found
         with
         | _ -> return_unit
         ) [%finally x := true; return_unit]
       end >>= fun () ->
       return !x
    ) ;

  test "finally exn default"
    (fun () ->
       let x = ref false in
       try%lwt
         ( raise Not_found )[%finally x := true; return_unit]
         >>= fun () ->
         return false
       with Not_found ->
         return !x
    ) ;

  test "structure let"
    (fun () ->
       Lwt.return structure_let_result
    ) ;

  (* as reported in https://github.com/ocsigen/lwt/issues/1085 *)
  test "1085-int"
    (fun () ->
      let%lwt (_ : int) = Lwt.return 0 in
      Lwt.return_true
    ) ;
  test "1085-int-again"
    (fun () ->
      let%lwt _ : int = Lwt.return 0 in
      Lwt.return_true
    ) ;
  test "1085-any"
    (fun () ->
      let%lwt _ : _ = Lwt.return 0 in
      Lwt.return_true
    ) ;

  (* offband report of bug, doesn't trigger but let's add to the testsuite anyway *)
  test "record-field-infer"
    (fun () ->
      let module M = struct type t = { a : int; b : int } end in
      let module MM = struct type t = { a : float; b : char; } end in
      let%lwt { a = _; _ } : M.t = Lwt.return { M.a = 0; b = 0 } in
      Lwt.return_true
    )[@ocaml.warning "-34-69"] ;
  (* What the ppx's [add_loc] is for. Each ppx bind that a rejection crosses
     re-raises at that source line, which APPENDS a "Re-raised at" frame, so a
     backtrace shows the chain of locations the exception travelled through
     instead of nothing. Nested functions, deliberately: a rejection propagates
     out through one bind per level, which is what builds the chain. A flat
     sequence of binds in one body would only ever cross the first.

     Discriminating: with the core ignoring [add_loc], this count is zero. *)
  test "backtrace: let%lwt reconstructs the chain"
    (fun () ->
      let recorded = Printexc.backtrace_status () in
      Printexc.record_backtrace true;
      let start, wake = Lwt.wait () in
      let level3 () =
        let%lwt () = start in
        Lwt.return_unit
      in
      let level2 () =
        let%lwt () = level3 () in
        Lwt.return_unit
      in
      let level1 () =
        let%lwt () = level2 () in
        Lwt.return_unit
      in
      let count needle haystack =
        let n = String.length needle and l = String.length haystack in
        let rec go i acc =
          if i + n > l then acc
          else if String.sub haystack i n = needle then go (i + n) (acc + 1)
          else go (i + 1) acc
        in
        go 0 0
      in
      let frames = ref 0 in
      let chain = level1 () in
      Lwt.wakeup_later_exn wake Exit;
      Lwt.bind
        (Lwt.catch
           (fun () -> chain)
           (fun _ ->
             frames := count "Re-raised at" (Printexc.get_backtrace ());
             Lwt.return_unit))
        (fun () ->
          if not recorded then Printexc.record_backtrace false;
          Lwt.return (!frames >= 2)));

  test "record-field-infer-brckt"
    (fun () ->
      let module M = struct type t = { a : int; b : int } end in
      let module MM = struct type t = { a : float; b : char; } end in
      let%lwt ({ a = _; _ } : M.t) = Lwt.return { M.a = 0; b = 0 } in
      Lwt.return_true
    )[@ocaml.warning "-34-69"] ;
]

let _ = Test.run "ppx" [ suite ]
