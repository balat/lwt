(* Oracle for B2b conformance: a candidate Lwt backed by the effect core,
   constrained by an UNCHANGED copy of src/core/lwt.mli. The build errors are the
   exact conformance gap (missing names + type/shape mismatches) to close for the
   in-place core swap. Not shipped; a measurement harness. *)

include Lwt_effects
