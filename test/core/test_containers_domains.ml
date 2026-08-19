(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The domain-affine containers: a mutex, a condition variable, an mvar, a pool
   and a stream. None of their state is made of promises the core could check for
   us -- locking a free mutex, or putting into an empty mvar, mutates a field with
   no promise in sight -- so each is stamped with its domain at creation and each
   operation checks.

   The module-level mutex or condition variable that libraries use to serialise
   access to a resource is exactly the pattern that was being trampled in silence,
   which is why this test exists.

   Needs a second domain, hence OCaml 5. *)

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

let contains needle m =
  let n = String.length needle in
  let rec go i =
    i + n <= String.length m
    && (String.sub m i n = needle || go (i + 1))
  in
  go 0

(* [true] if [f] is refused because the value belongs to another domain, as
   opposed to any other [Invalid_argument]. *)
let refused f =
  match f () with
  | _ -> false
  | exception Invalid_argument m -> contains "belongs to another domain" m
  | exception _ -> false

let on_other_domain f = Domain.join (Domain.spawn f)

let () =
  (* Everything below is created here and used over there. *)
  let m = Lwt_mutex.create () in
  let cv : int Lwt_condition.t = Lwt_condition.create () in
  let mv = Lwt_mvar.create_empty () in
  let pool = Lwt_pool.create 1 (fun () -> Lwt.return 0) in
  let stream, push = Lwt_stream.create () in
  push (Some 1);

  check "Lwt_mutex.lock is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_mutex.lock m)));
  check "Lwt_mutex.unlock is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_mutex.unlock m)));
  check "Lwt_condition.signal is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_condition.signal cv 1)));
  check "Lwt_condition.broadcast is refused elsewhere"
    (on_other_domain (fun () ->
       refused (fun () -> Lwt_condition.broadcast cv 1)));
  check "Lwt_condition.wait is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_condition.wait cv)));
  check "Lwt_mvar.put is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_mvar.put mv 1)));
  check "Lwt_mvar.take is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_mvar.take mv)));
  check "Lwt_pool.use is refused elsewhere"
    (on_other_domain (fun () ->
       refused (fun () -> Lwt_pool.use pool (fun _ -> Lwt.return_unit))));
  check "Lwt_pool.clear is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_pool.clear pool)));
  (* One element is queued, so this consumes rather than feeds: both primitives
     are checked, and this is the one a shared queue would corrupt. *)
  check "reading a stream is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_stream.get stream)));
  check "pushing to a stream is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> push (Some 2))));
  check "cloning a stream is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_stream.clone stream)));

  (* An empty stream, so the other domain reaches [feed] rather than [consume]. *)
  let empty = fst (Lwt_stream.create ()) in
  check "feeding a stream is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> Lwt_stream.get empty)));

  (* A bounded push takes the same treatment. *)
  let _bstream, bpush = Lwt_stream.create_bounded 4 in
  check "a bounded push is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> bpush#push 1)));
  check "resizing a bounded push is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> bpush#resize 8)));
  check "closing a bounded push is refused elsewhere"
    (on_other_domain (fun () -> refused (fun () -> bpush#close)));

  (* And the other domain's own containers work perfectly well. *)
  check "another domain uses its own containers"
    (on_other_domain (fun () ->
       let m = Lwt_mutex.create () in
       let mv = Lwt_mvar.create 7 in
       let s, p = Lwt_stream.create () in
       p (Some 3);
       p None;
       match
         ( Lwt.state (Lwt_mutex.lock m),
           Lwt.state (Lwt_mvar.take mv),
           Lwt.state (Lwt_stream.get s) )
       with
       | Lwt.Return (), Lwt.Return 7, Lwt.Return (Some 3) -> true
       | _ -> false));

  (* Ours still work here, unharmed by all of the above. *)
  check "ours still work here"
    (match
       ( Lwt.state (Lwt_mutex.lock m),
         Lwt.state (Lwt_stream.get stream),
         Lwt_mutex.is_locked m )
     with
     | Lwt.Return (), Lwt.Return (Some 1), true -> true
     | _ -> false);

  if !failures > 0 then exit 1;
  print_endline "domain-affine containers: ok"
