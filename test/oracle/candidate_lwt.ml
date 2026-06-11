(* Oracle for B2b conformance: a candidate Lwt backed by the effect core,
   constrained by an UNCHANGED copy of src/core/lwt.mli. The build errors are the
   exact conformance gap (missing names + type/shape mismatches) to close for the
   in-place core swap. Not shipped; a measurement harness. *)

include Lwt_effects

(* ------------------------------------------------------------------ *)
(* Conformance batch 1: mechanical combinators absent from the effect  *)
(* core's public API. Pure derived definitions on the monadic core.    *)
(* ------------------------------------------------------------------ *)

let ( <?> ) a b = choose [ a; b ]
let ( <&> ) a b = join [ a; b ]
let ( =<< ) f p = bind p f
let ( =|< ) f p = map f p

(* [apply f x] runs [f x], turning a synchronous exception into a rejected
   promise (as {!Lwt.apply}). *)
let apply f x = try f x with e -> fail e

let wrap1 f = fun x -> (try return (f x) with e -> fail e)
let wrap2 f = fun x y -> (try return (f x y) with e -> fail e)
let wrap3 f = fun x y z -> (try return (f x y z) with e -> fail e)
let wrap4 f = fun a b c d -> (try return (f a b c d) with e -> fail e)
let wrap5 f = fun a b c d e -> (try return (f a b c d e) with ex -> fail ex)
let wrap6 f = fun a b c d e g -> (try return (f a b c d e g) with ex -> fail ex)
let wrap7 f = fun a b c d e g h -> (try return (f a b c d e g h) with ex -> fail ex)

external reraise : exn -> 'a = "%reraise"

(* Lwt's [async] is fire-and-forget ([(unit -> unit t) -> unit]) and runs [f ()]
   {e immediately} on the caller's stack (its callbacks register before the
   caller's next action — tests rely on this); a synchronous raise or a rejection
   goes to [async_exception_hook]. Shadows the effect core's fiber-spawning,
   promise-returning [async]. *)
let async (f : unit -> unit t) : unit =
  let p = try f () with e when Exception_filter.run e -> fail e in
  on_failure p (fun e -> !async_exception_hook e)

(* Same immediate-run semantics for [dont_wait], with a user handler. *)
let dont_wait (f : unit -> unit t) (handler : exn -> unit) : unit =
  let p = try f () with e when Exception_filter.run e -> fail e in
  on_failure p handler

(* The tracing/backtrace variants take location metadata (name, line, an
   exception-rewriting function) and otherwise delegate to the plain combinators:
   the effect core does not maintain Lwt's backtraces. *)
let backtrace_bind _name _line _add_loc p f = bind p f
let backtrace_catch _name _line _add_loc f h = catch f h
let backtrace_finalize _name _line _add_loc f g = finalize f g
let backtrace_try_bind _name _line _add_loc f g h = try_bind f g h

(* The resolvers, reporting double resolution under Lwt's own function names. *)
let wakeup u v = Lwt_effects.Private.wakeup_named "Lwt.wakeup" u (Ok v)
let wakeup_exn u e = Lwt_effects.Private.wakeup_named "Lwt.wakeup_exn" u (Error e)
let wakeup_later u v =
  Lwt_effects.Private.wakeup_later_named "Lwt.wakeup_later" u (Ok v)

let wakeup_later_exn u e =
  Lwt_effects.Private.wakeup_later_named "Lwt.wakeup_later_exn" u (Error e)

let wakeup_result u r = Lwt_effects.Private.wakeup_named "Lwt.wakeup_result" u r

let wakeup_later_result u r =
  Lwt_effects.Private.wakeup_later_named "Lwt.wakeup_later_result" u r


(* ------------------------------------------------------------------ *)
(* Conformance batch 2: ppx syntax, task/sequence, exception filter,   *)
(* tracing/debug, and Lwt_main pause internals (shims — the effect     *)
(* scheduler drives its own loop).                                     *)
(* ------------------------------------------------------------------ *)

module Let_syntax = struct
  module Let_syntax = struct
    let return = return
    let map p ~f = map f p
    let bind p ~f = bind p f
    let both = both

    module Open_on_rhs = struct end
  end
end

(* Lwt's [Infix] also carries [<&>]/[<?>] and a nested [Let_syntax]. *)
module Infix = struct
  include Infix

  let ( <&> ) a b = join [ a; b ]
  let ( <?> ) a b = choose [ a; b ]

  module Let_syntax = Let_syntax.Let_syntax
end

(* [add_task_r]/[add_task_l]: a task whose resolver is added to an Lwt_sequence,
   removed on cancel (the documented equivalent in lwt.mli). *)
let add_task_r seq =
  let p, r = task () in
  let node = Lwt_sequence.add_r r seq in
  on_cancel p (fun () -> Lwt_sequence.remove node);
  p

let add_task_l seq =
  let p, r = task () in
  let node = Lwt_sequence.add_l r seq in
  on_cancel p (fun () -> Lwt_sequence.remove node);
  p

(* Tracing is a no-op here (the effect core does not emit Lwt's span events). *)
let with_tracing_context _name f = f ()

(* Compares the {e constructor} of the expected state with the promise's. *)
let debug_state_is expected p =
  return
    (match (expected, state p) with
     | Return _, Return _ | Fail _, Fail _ | Sleep, Sleep -> true
     | _ -> false)

(* The pause protocol comes from the effect core (real implementation).
   [abandon_wakeups] bails out of the resolution loop (Lwt's issue #48 hook);
   the paused queue is dropped separately by [abandon_paused], as in Lwt. *)
let abandon_wakeups () = Lwt_effects.Private.abandon_resolution_loop ()

(* Lwt's [Private]: the storage internals (backed by the effect core's own
   fiber-local storage) and the tracing-context key. Shadows the effect core's
   [Private] (scheduler hooks), which the candidate does not re-export. *)
module Private = struct
  type storage = Lwt_effects.Private.storage

  module Sequence_associated_storage = struct
    let get_from_storage = Lwt_effects.Private.get_from_storage
    let modify_storage = Lwt_effects.Private.modify_storage
    let empty_storage = Lwt_effects.Private.empty_storage
    let current_storage = Lwt_effects.Private.current_storage
  end

  let tracing_context : string key = new_key ()
end
