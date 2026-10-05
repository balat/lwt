(lang dune 3.18)

(name lwt)

; ONE VERSION for every package of this repository, each requiring the lwt of
; the same version: several of them use Lwt.Private or Lwt_dls, which are
; internal and move between versions (lwt_direct, lwt_multicore, lwt_react,
; and the code lwt_ppx generates), so anything looser lets opam pair an lwt with
; a satellite built for other internals, and the user meets it as a
; compilation error rather than a solver message. As Eio does with its
; packages.
(version 6.2.0)

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
 (synopsis "Utilities for retrying Lwt computations")
 (authors "Shon Feder")
 (maintainers
   "Raphaël Proust <code@bnwr.net>"
   "Shon Feder <shon.feder@gmail.com>")
 (maintenance_intent "(latest)")
 (depends
  (ocaml (>= 4.08))
  (lwt (= :version))))

(package
 (name lwt_ppx)
 (synopsis "PPX syntax for Lwt, providing something similar to async/await from JavaScript")
 (maintenance_intent "(latest)")
 (depends
  (ocaml (>= 4.08))
  (ppxlib (>= 0.36))
  (lwt (= :version))))

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
 (synopsis "Helpers for using React with Lwt")
 (maintenance_intent "(latest)")
 (depends
  (ocaml (>= 4.08))
  (cppo (and :build (>= 1.1)))
  (lwt (= :version))
  (react (>= 1.0))))

(package
 (name lwt_direct)
 (synopsis "Direct-style control-flow and `await` for Lwt")
 (authors "Simon Cruanes")
 (depends
  (ocaml (>= 5.3))
  base-unix
  (lwt (= :version))))

(package
 (name lwt_uring)
 (synopsis "Lwt engine based on io_uring, where Linux provides it")
 (depends
  (ocaml (>= 4.14))
  base-unix
  (lwt (= :version))
  ; Only where ocaml-uring exists. Elsewhere lwt_uring builds a stub that
  ; reports io_uring as unavailable (see src/uring/dune).
  (uring (and (>= 2.14) (= :os linux)))
  (cstruct (and (>= 6.0.1) (= :os linux)))
  (optint (and (>= 0.1.0) (= :os linux)))))

(package
 (name lwt_multicore)
 (synopsis "Communication primitives between Lwt loops on several domains")
 ; A SEPARATE package, and for a precise reason: its Saturn dependency must stay
 ; out of the `lwt` package, so that the core keeps its OCaml 4.14 floor and its
 ; js_of_ocaml support untouched. OCaml 5 only, which multicore is anyway.
 (depends
  (ocaml (>= 5.0))
  base-unix
  (lwt (= :version))
  (saturn (>= 1.0))
  (qcheck-core (and :with-test (>= 0.90)))
  (qcheck-stm (and :with-test (>= 0.7)))
  (qcheck-lin (and :with-test (>= 0.7)))))

(package
 (name lwt_runtime_events)
 (synopsis "Emit runtime events to trace the execution of Lwt programs")
 (authors "Raphaël Proust")
 (depends
  (ocaml (>= 5.1))
  ))

(package
 (name lwt)
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
  ; the differential test of the core, test/differential, and the models of
  ; the containers, test/stm (oneof_weighted is from qcheck-core 0.90)
  (qcheck-core (and :with-test (>= 0.90)))
  (qcheck-stm (and :with-test (>= 0.7)))
  dune-configurator
  ocplib-endian)
 (depopts base-threads base-unix conf-libev lwt_runtime_events)
 ; A satellite of another version was built against other internals: the
 ; lwt_direct already released writes a field of Lwt.Private that this one does
 ; not have. lwt_runtime_events, an optional dependency, is held to the same
 ; version this way.
 (conflicts
  (lwt_direct (<> :version))
  (lwt_multicore (<> :version))
  (lwt_runtime_events (<> :version))))
