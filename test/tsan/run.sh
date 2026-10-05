#!/bin/sh
# Run the per-domain tests under ThreadSanitizer.
#
# This is not part of the ordinary suite: it needs a switch whose COMPILER is built
# with TSan, since the instrumentation of OCaml code is emitted by ocamlopt itself.
# Building that switch is not trivial on every distribution; test/tsan/README.md has
# the recipe and the two things that go wrong.
#
# Usage, from the root of the tree, in a TSan switch:
#
#   sh test/tsan/run.sh              # the whole list
#   sh test/tsan/run.sh domain_soak  # one test, by name
#
# What the exit code means: 0 if TSan reported nothing on any test, non-zero if it
# reported anything at all. TSan's own exit code for "races found" is 66 here, so a
# test that fails its own assertions and a test that races are distinguishable.
set -eu

build=${LWT_TSAN_BUILD_DIR:-_build-tsan}
here=$(cd "$(dirname "$0")" && pwd)

# One line per test: the dune target. Anything that spawns a domain belongs here,
# except the benchmark that needs an external load. The uring, direct and qcheck
# tests need uring, react, qcheck-lin and qcheck-stm in the TSan switch; see
# README.md.
tests="
test/core/test_ownership.exe
test/core/test_sched_domains.exe
test/core/test_containers_domains.exe
test/core/test_lwt_dls_domains.exe
test/core/test_storage_domains.exe
test/unix/domain_wakeup_stress.exe
test/unix/domain_shared_resolved.exe
test/unix/domain_soak.exe
test/unix/domain_channels.exe
test/unix/domain_emfile.exe
test/unix/domain_engines.exe
test/unix/domain_exit_hooks.exe
test/unix/domain_gc.exe
test/unix/domain_io_affinity.exe
test/unix/domain_notifications.exe
test/unix/domain_reuseport.exe
test/unix/domain_signals.exe
test/unix/domain_std_channels.exe
test/unix/signal_full_buffer.exe
test/unix/domain_timeouts.exe
test/unix/domain_two_loops.exe
test/unix/domain_unix_policy.exe
test/unix/domain_upstream_scenarios.exe
test/multicore/run_on.exe
test/multicore/shared_value.exe
test/multicore/adopt.exe
test/multicore/sync.exe
test/multicore/stream.exe
test/multicore/service.exe
test/multicore/example.exe
test/multicore/exit_hooks.exe
test/multicore/departed_loop.exe
test/multicore/service_fatal.exe
test/multicore/cancel_storm.exe
test/multicore/handover.exe
test/multicore/stream_order.exe
test/multicore/systhread.exe
test/multicore/lin_shared_value.exe
test/multicore/notifications.exe
test/multicore/stm_domain_sync.exe
test/direct/main.exe
test/uring/domain_rings.exe
test/uring/test_lwt_uring.exe
"

if [ $# -gt 0 ]; then
  wanted=$1
  tests=$(printf '%s\n' $tests | grep -- "$wanted" || true)
  [ -n "$tests" ] || { echo "no test matches $wanted"; exit 2; }
fi

# Keep the soak short: TSan slows execution by a large factor, and a soak that
# takes ten minutes under TSan will not be run.
LWT_SOAK_SECONDS=${LWT_SOAK_SECONDS:-3}
export LWT_SOAK_SECONDS

# die_after_fork=0: the uring suite forks a process that has threads, which TSan
# does not support in the child, and by default it kills the child. Letting it run
# keeps the test's own assertions; a report from the child would be suspect.
base_opts="suppressions=$here/suppressions.txt halt_on_error=0 history_size=4 die_after_fork=0"

# A recent kernel's ASLR entropy makes libtsan abort at startup ("unexpected memory
# mapping"). setarch -R turns randomisation off for the process and its children,
# and needs no root. See README.md.
if command -v setarch > /dev/null 2>&1; then
  noaslr="setarch $(uname -m) -R"
else
  noaslr=""
fi

# dune is itself compiled by the instrumented compiler, so the BUILD needs the same
# treatment as the tests. It also means dune runs UNDER TSan: exitcode=0 for this
# step, so that a report about dune's own code cannot fail the build. Reports that
# matter are the ones from the tests below.
# The workspace file instruments Lwt's C stubs too, which dune does not do by
# default on a TSan switch; it says why that matters.
# shellcheck disable=SC2086
TSAN_OPTIONS="$base_opts exitcode=0 ${TSAN_OPTIONS:-}" \
  $noaslr dune build --workspace="$here/dune-workspace.tsan" --build-dir="$build" $tests

status=0
for t in $tests; do
  printf '\n=== %s\n' "$t"
  if TSAN_OPTIONS="$base_opts exitcode=66 ${TSAN_OPTIONS:-}" $noaslr "$build/default/$t"; then
    :
  else
    rc=$?
    if [ "$rc" = 66 ]; then
      echo "!!! TSan reported a data race in $t"
    else
      echo "!!! $t failed with exit $rc (not a TSan report)"
    fi
    status=1
  fi
done

if [ "$status" = 0 ]; then echo "\nTSan: no report on any test"; fi
exit "$status"
