(* This file is part of Lwt, released under the MIT license. See LICENSE.md for
   details, or visit https://github.com/ocsigen/lwt/blob/master/LICENSE.md. *)

(* lwt_uring is not available in this build: say so rather than silently
   measuring the default engine. *)

let install () =
  failwith "LWT_BENCH_URING=1: lwt_uring is not available in this build"
