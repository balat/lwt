#!/bin/sh
# The js_of_ocaml smoke test of the per-domain layer, as one command.
#
# Why this exists as a script rather than as a dune test: js_of_ocaml is not a
# dependency of lwt, and must not become one, so the test cannot be an ordinary
# rule in the tree.  It is instead a script that CI runs in a switch that has
# js_of_ocaml, and that anyone can run by hand.  The test itself
# (test/core/test_browser_invariants.ml) also runs as a native test everywhere;
# what only this script covers is the BACKEND.
#
# What it protects: the per-domain shim [Lwt_dls] keys on the compiler version
# through cppo, not on the backend.  Under js_of_ocaml the compiler is OCaml 5,
# so the shim takes the Domain.DLS branch, which is correct only because
# js_of_ocaml implements that primitive.  A backend that did not would take the
# wrong branch silently, and this is the only thing that would say so.
#
# Usage, in a switch with js_of_ocaml and with node on the PATH:
#
#   sh test/jsoo/smoke.sh
#
# The build directory is separate from the default _build so that running this
# in a second switch does not fight with the native build.
set -eu

build=${LWT_JSOO_BUILD_DIR:-_build-jsoo}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

dune build --build-dir="$build" src/core/lwt.cma

ocamlc -w -a \
  -I "$build/default/src/core/.lwt.objs/byte" \
  "$build/default/src/core/lwt.cma" \
  test/core/test_browser_invariants.ml \
  -o "$tmp/smoke.bc"

js_of_ocaml "$tmp/smoke.bc" -o "$tmp/smoke.js"
node "$tmp/smoke.js"
