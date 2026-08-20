(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* The worked example from the module's documentation, as a test, so that the
   documentation cannot drift away from what compiles and runs.

   The shape: several producer loops feed a bounded channel, a pool of domains does
   the expensive part, one service owns a table nobody else touches, and the main
   loop aggregates. Every promise is local; what crosses is integers and strings.

   Needs a second domain, hence OCaml 5. *)

open Lwt.Infix

let failures = ref 0

let check name b =
  if not b then (Printf.eprintf "FAILED: %s\n" name; incr failures)

(* The expensive part, deliberately dumb: something that costs and that the
   caller wants off its loop. *)
let digest n =
  let rec go acc i = if i = 0 then acc else go ((acc * 31) + i) (i - 1) in
  go n 2000

let () =
  let items = 60 in

  (* One domain owning a table. Nobody else touches it, which is what makes it
     safe without a lock. *)
  let table = Hashtbl.create 16 in
  let store =
    Lwt_multicore.Service.create (fun (k, v) ->
      Hashtbl.replace table k v;
      Lwt.return (Hashtbl.length table))
  in

  (* A pool of domains for the digesting. *)
  let pool = Lwt_multicore.Pool.create ~count:2 () in

  (* A bounded channel from the producers to us: capacity 8, so that producers
     wait rather than filling memory if we fall behind. *)
  let work : int Lwt_multicore.Stream.t =
    Lwt_multicore.Stream.create ~capacity:8
  in

  (* Two producer loops, each on its own domain, each pushing its share. *)
  let producer lo hi =
    Domain.spawn (fun () ->
      Lwt_main.run
        (let rec go i =
           if i > hi then Lwt.return_unit
           else Lwt_multicore.Stream.push work i >>= fun () -> go (i + 1)
         in
         go lo))
  in
  let p1 = producer 1 (items / 2) in
  let p2 = producer ((items / 2) + 1) items in

  (* And us: take, digest on the pool, store through the service, until we have
     seen every item. Joining the producers happens after, since [Domain.join]
     blocks and has no business on a loop. *)
  let total =
    Lwt_main.run
      (let rec consume seen =
         if seen >= items then Lwt.return seen
         else
           Lwt_multicore.Stream.take work >>= function
           | None -> Lwt.return seen
           | Some n ->
             Lwt_multicore.Pool.detach pool digest n >>= fun d ->
             Lwt_multicore.Service.call store (n, d) >>= fun stored ->
             consume stored
       in
       consume 0)
  in
  Domain.join p1;
  Domain.join p2;
  Lwt_multicore.Stream.close work;

  check "every item went through the whole pipeline" (total = items);
  check "the service's table holds them all" (Hashtbl.length table = items);
  check "and the digests are the right ones"
    (Hashtbl.fold (fun k v ok -> ok && v = digest k) table true);

  Lwt_main.run (Lwt_multicore.Pool.shutdown pool);
  Lwt_main.run (Lwt_multicore.Service.shutdown store);

  if !failures > 0 then exit 1;
  print_endline "worked example: producers, pool, service: ok"
