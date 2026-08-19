(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* An [Lwt.key] is a single process-wide value, typically created once at module
   initialisation. Looking a value up in a storage must therefore not go through
   mutable state held in the key: two domains owning perfectly isolated storages
   would corrupt each other's reads through it, and the corruption is silent (a
   stale value, or [None]).

   Each domain below owns a storage binding the SAME key to its OWN value, and
   must read that value back on every lookup. The lookups go through the
   internal storage API rather than [Lwt.get], because [Lwt.get] reads the
   process-wide current storage, which is a separate concern.

   This test needs real parallelism to be conclusive: systhreads of one domain
   never run OCaml code simultaneously, so they hit the window between two
   accesses to a shared cell far too rarely to be a regression test. Hence a
   separate executable, built only from OCaml 5. The workers also rendezvous
   before hammering, so that domain startup does not eat the overlap the test
   depends on. *)

[@@@alert "-trespassing"]

module Storage = Lwt.Private.Sequence_associated_storage

[@@@alert "+trespassing"]

let domains = 4
let iterations = 200_000

(* Spin until every worker has arrived, so that all of them hammer at once. *)
let arrived = Atomic.make 0

let rendezvous () =
  ignore (Atomic.fetch_and_add arrived 1);
  while Atomic.get arrived < domains do
    Domain.cpu_relax ()
  done

(* Returns the number of lookups that did not see this domain's own value. *)
let hammer (key : int Lwt.key) (mine : int) () : int =
  let storage = Storage.modify_storage key (Some mine) Storage.empty_storage in
  rendezvous ();
  let mismatches = ref 0 in
  for _ = 1 to iterations do
    match Storage.get_from_storage key storage with
    | Some v when v = mine -> ()
    | Some _ | None -> incr mismatches
  done;
  !mismatches

let () =
  let key : int Lwt.key = Lwt.new_key () in
  let others =
    List.init (domains - 1) (fun i -> Domain.spawn (hammer key (i + 1)))
  in
  let here = hammer key domains () in
  let mismatches = List.fold_left (fun acc d -> acc + Domain.join d) here others in
  if mismatches > 0 then begin
    Printf.eprintf
      "Lwt.key storage is not domain-safe: %d mismatched lookups out of %d, \
       over %d domains\n"
      mismatches (domains * iterations) domains;
    exit 1
  end;
  (* A key created on one domain must also be usable from another, and two keys
     of the same type must not alias. *)
  let k1 : string Lwt.key = Lwt.new_key () in
  let k2 : string Lwt.key = Lwt.new_key () in
  let storage =
    Storage.modify_storage k1 (Some "one") Storage.empty_storage
    |> Storage.modify_storage k2 (Some "two")
  in
  let check () =
    assert (Storage.get_from_storage k1 storage = Some "one");
    assert (Storage.get_from_storage k2 storage = Some "two")
  in
  check ();
  Domain.join (Domain.spawn check);
  (* Keys created concurrently must be distinct. Their ids come from a shared
     counter, and two keys sharing an id share a slot in the storage, so the
     second binding shadows the first and the first no longer projects. *)
  let keys_per_domain = 20_000 in
  let make () = List.init keys_per_domain (fun _ -> Lwt.new_key ()) in
  let other = Domain.spawn make in
  let mine = make () in
  let keys = mine @ Domain.join other in
  let storage =
    List.fold_left
      (fun (storage, i) key -> (Storage.modify_storage key (Some i) storage, i + 1))
      (Storage.empty_storage, 0) keys
    |> fst
  in
  let shadowed =
    List.fold_left
      (fun (n, i) key ->
        ((if Storage.get_from_storage key storage = Some i then n else n + 1), i + 1))
      (0, 0) keys
    |> fst
  in
  if shadowed > 0 then begin
    Printf.eprintf
      "Lwt.new_key is not domain-safe: %d of %d concurrently created keys \
       share an id with another\n"
      shadowed (List.length keys);
    exit 1
  end
