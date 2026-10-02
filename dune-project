(lang dune 3.18)

(name lwt)

(generate_opam_files true)

(maintainers
 "Raphaël Proust <code@bnwr.net>"
 "Anton Bachin <antonbachin@yahoo.com>")
(authors "Jérôme Vouillon" "Jérémie Dimino")
(license MIT)
(source (github ocsigen/lwt))
(documentation "https://ocsigen.org/lwt")

(package
 (name lwt_retry)
 (version 5.9.0)
 (synopsis "Utilities for retrying Lwt computations")
 (authors "Shon Feder")
 (maintainers
   "Raphaël Proust <code@bnwr.net>"
   "Shon Feder <shon.feder@gmail.com>")
 (maintenance_intent "(latest)")
 (depends
  (ocaml (>= 4.08))
  (lwt (>= 5.3))))

(package
 (name lwt_ppx)
 (version 6.1.0)
 (synopsis "PPX syntax for Lwt, providing something similar to async/await from JavaScript")
 (maintenance_intent "(latest)")
 (depends
  (ocaml (>= 4.08))
  (ppxlib (>= 0.36))
  (lwt (>= 6)))
 (conflicts
  (lwt (or (= 6.0.0~alpha00) (= 6.0.0-beta01)))))

(package
 (name lwt_ppx__ppx_let_tests)
 (synopsis "DO NOT RELEASE! only for testing let_ppx")
 (license NOTFORRELEASE) ;; trap for opam-repo ci
 (allow_empty)
 (maintenance_intent "(latest)")
 (depends
  (ocaml (>= 5.1))
  (ppx_let (and :with-test (>= v0.17.1)))))

(package
 (name lwt_react)
 (version 1.2.0)
 (synopsis "Helpers for using React with Lwt")
 (maintenance_intent "(latest)")
 (depends
  (ocaml (>= 4.08))
  (cppo (and :build (>= 1.1)))
  (lwt (>= 3.0))
  (react (>= 1.0))))

(package
 (name lwt_direct)
 (version 6.0.0)
 (synopsis "Direct-style control-flow and `await` for Lwt")
 (authors "Simon Cruanes")
 (depends
  (ocaml (>= 5.3))
  base-unix
  (lwt (>= 6))))

(package
 (name lwt_uring)
 (version 1.0.0)
 (synopsis "Lwt engine based on io_uring, where Linux provides it")
 (depends
  (ocaml (>= 4.14))
  base-unix
  ; The first lwt with the completion backend of Lwt_unix (set_completion_io,
  ; fd_kind), whose optional arguments let later versions extend it.
  (lwt (>= 6.2.0))
  ; Only where ocaml-uring exists. Elsewhere lwt_uring builds a stub that
  ; reports io_uring as unavailable (see src/uring/dune).
  (uring (and (>= 2.14) (= :os linux)))
  (cstruct (and (>= 6.0.1) (= :os linux)))
  (optint (and (>= 0.1.0) (= :os linux)))))

(package
 (name lwt_multicore)
 (version 6.0.0)
 (synopsis "Communication primitives between Lwt loops on several domains")
 ; A SEPARATE package, and for a precise reason: its Saturn dependency must stay
 ; out of the `lwt` package, so that the core keeps its OCaml 4.14 floor and its
 ; js_of_ocaml support untouched. OCaml 5 only, which multicore is anyway.
 (depends
  (ocaml (>= 5.0))
  base-unix
  ; This library uses Lwt.Private and Lwt_dls, which are internal and under an
  ; alert, so it is LOCKSTEP with lwt and (>= 6) understates that: opam may pick
  ; an lwt whose internals have moved, and the user meets it as a compilation
  ; error rather than a solver message. The constraint that says it is
  ; (lwt (= :version)), which needs the packages in this repository to carry ONE
  ; version; today they carry four different ones (lwt 6.1.1, the satellites
  ; 6.0.0), so writing it would make this package uninstallable. Same for
  ; lwt_direct, which uses Lwt.Private too; lwt_uring does not, it uses only the
  ; public interface of Lwt_unix and Lwt_engine. To settle at release time, with
  ; the versions.
  (lwt (>= 6))
  (saturn (>= 1.0))
  (qcheck-stm (and :with-test (>= 0.3)))))

(package
 (name lwt_runtime_events)
 (version 6.0.0)
 (synopsis "Emit runtime events to trace the execution of Lwt programs")
 (authors "Raphaël Proust")
 (depends
  (ocaml (>= 5.1))
  ))

(package
 (name lwt)
 (version 6.2.0)
 (synopsis "Promises and event-driven I/O")
 (description "A promise is a value that may become determined in the future.

Lwt provides typed, composable promises. Promises that are resolved by I/O are
resolved by Lwt in parallel.

Meanwhile, OCaml code, including code creating and waiting on promises, runs in
a single thread by default. This reduces the need for locks or other
synchronization primitives. Code can be run in parallel on an opt-in basis.
")
 (maintenance_intent "(latest)")
 (depends
  ; the lean core uses no OCaml-5 machinery; 4.14 is the oldest version
  ; exercised by the test suite (older may work, unverified)
  (ocaml (>= 4.14))
  (cppo (and :build (>= 1.1)))
  (ocamlfind (and :dev (>= 1.7.3-1)))
  (odoc (and :with-doc (>= 2.3)))
  ; the differential test of the core, test/differential
  (qcheck-core (and :with-test (>= 0.21)))
  dune-configurator
  ocplib-endian)
 (depopts base-threads base-unix conf-libev lwt_runtime_events))
