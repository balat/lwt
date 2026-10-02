# Running Lwt's per-domain tests under ThreadSanitizer

TSan is the one tool here that can find a race nobody thought to test for. It
reports on what actually executed, so it is not a model checker and a run that
reports nothing proves nothing about the paths it did not take. What it does catch
is the class of bug this work is most exposed to: two domains touching the same
mutable field with no synchronisation between them.

Instrumentation of OCaml code is emitted by `ocamlopt` itself, so this needs a
switch whose **compiler** was built with TSan. C stubs are NOT instrumented by
default, and `run.sh` has to see to it; see "C stubs" below.

## Building the switch

```
opam switch create lwt-tsan --packages=ocaml-variants.5.4.0+options,ocaml-option-tsan
```

Four things go wrong, and each costs an hour if you meet it without warning.

**1. `conf-unwind` needs the nongnu libunwind, not LLVM's.** OCaml's TSan support
unwinds C stacks with libunwind, so the switch depends on `conf-unwind`, which asks
`pkg-config` for `libunwind`. On Debian and Ubuntu that comes from `libunwind-dev`.

**2. On Ubuntu 24.04, `/usr/lib/x86_64-linux-gnu/libunwind.so` is LLVM's
libunwind**, installed by `libunwind-18-dev`, and it does **not** export the
`_ULx86_64_*` symbols OCaml's `runtime/tsan.c` calls. So the header check can pass
and the link still fail, with undefined references at `caml_tsan_exit_on_raise_c`.
The fix is to point OCaml's configure at the right one through its own precious
variables:

```
export LIBUNWIND_CPPFLAGS=-I/path/to/nongnu/include
export LIBUNWIND_LDFLAGS=-L/path/to/nongnu/lib
```

`configure` appends `-lunwind -lunwind-x86_64` itself, so only the paths are needed.

**3. On a recent kernel, TSan dies before it starts.** The build, and then the
tests, abort with `FATAL: ThreadSanitizer: unexpected memory mapping`. This is not
about Lwt or about OCaml: the libtsan that ships with gcc 13 does not cope with the
address-space randomisation entropy of kernels 6.x and later. Two fixes, one of
which needs no root:

```
setarch "$(uname -m)" -R <command>     # no root: run with ASLR off, inherited by children
sudo sysctl vm.mmap_rnd_bits=28        # root: lower the entropy machine-wide
```

`setarch` has to wrap the whole build, `opam switch create` included, since the
compiler being built is itself run by `make`. `test/tsan/run.sh` applies it by
itself when it is available.

**4. And `setarch` does not reach through opam's build sandbox.** opam runs builds
under `bwrap`, which resets the process personality, so the randomisation you turned
off outside is back on inside, and the build dies exactly as before. Every opam
action in a TSan switch runs instrumented binaries (the compiler it just built, then
dune), so the sandbox has to go. The way that touches nothing of your existing setup
is a **separate opam root**:

```
export OPAMROOT=$HOME/.opam-tsan
opam init --bare -y --disable-sandboxing --no-setup default "$HOME/.opam/repo/default"
setarch "$(uname -m)" -R opam switch create lwt-tsan \
  --packages=ocaml-variants.5.4.0+options,ocaml-option-tsan
```

Pointing the new root at the existing root's unpacked repository makes `init`
instant. The switch costs about 2 GB, and deleting the root is the whole cleanup.

If you would rather use your normal root, `sudo sysctl vm.mmap_rnd_bits=28` avoids
the personality question altogether, since then nothing needs `setarch`.

**Without root**, the dev package can be unpacked into a prefix of your own, which
is how this was first run:

```
apt-get download libunwind-dev
dpkg -x libunwind-dev_*.deb "$PREFIX"
# the .so symlinks in the dev package point at the runtime package's files
ln -s /usr/lib/x86_64-linux-gnu/libunwind.so.8.0.1 \
      "$PREFIX/usr/lib/x86_64-linux-gnu/libunwind.so.8.0.1"
ln -s /usr/lib/x86_64-linux-gnu/libunwind-x86_64.so.8.0.1 \
      "$PREFIX/usr/lib/x86_64-linux-gnu/libunwind-x86_64.so.8.0.1"
sed -i "s|^prefix=/usr|prefix=$PREFIX/usr|" \
      "$PREFIX/usr/lib/x86_64-linux-gnu/pkgconfig/libunwind.pc"
export PKG_CONFIG_PATH=$PREFIX/usr/lib/x86_64-linux-gnu/pkgconfig
export LIBUNWIND_CPPFLAGS=-I$PREFIX/usr/include/x86_64-linux-gnu
export LIBUNWIND_LDFLAGS=-L$PREFIX/usr/lib/x86_64-linux-gnu
```

The runtime libraries themselves come from `libunwind8`, which is normally already
installed, so the built binaries need no `LD_LIBRARY_PATH`.

## Running

From the root of the tree, in that switch:

```
opam install ./lwt.opam ./lwt_multicore.opam --deps-only
sh test/tsan/run.sh              # every test that spawns a domain
sh test/tsan/run.sh domain_soak  # one of them
```

The script uses its own build directory (`_build-tsan`) so it does not fight with
the ordinary build, and it shortens the soak, because TSan is slow and a soak that
takes ten minutes will not be run.

Exit code 0 means TSan reported nothing. A test that races exits 66, which the
script separates from a test that merely failed.

## What is suppressed

`test/tsan/suppressions.txt`, with a reason on every entry. In short: libev and
liburing share memory with the kernel, and TSan cannot see the barriers that order
those accesses. **Nothing in Lwt's own OCaml code is suppressed**, so a report
pointing there is a finding.

## C stubs

dune compiles C stubs with the compiler's `ocamlc_cflags`, and on a TSan switch
those do not carry `-fsanitize=thread`: only `native_cflags` do (`ocamlopt -config`
shows both). So by default Lwt's C code is not instrumented, and that costs three
things, all of which happened here before it was noticed.

- **Its races are invisible.** TSan sees nothing of the notification table, the
  signal table or the job pool beyond the libc calls it intercepts.
- **Its synchronisation is invisible too, which produces false reports.** The
  notification table is ordered by acquire loads and release stores; uninstrumented,
  those are plain moves, TSan sees no happens-before edge, and it reports races on
  the mutex and the descriptor of a channel that is correctly published.
- **TSan crashes.** When an exception is raised from C, the OCaml runtime unwinds
  the C frames and calls `__tsan_func_exit` for each, including frames of
  uninstrumented code that never called `__tsan_func_entry`. TSan's shadow stack
  underflows, and it later dies hashing a corrupt stack
  (`SEGV ... MurMur2Hash64Builder::add`), typically in a test that does a lot of
  non-blocking I/O, since every `EAGAIN` is such a raise.

`run.sh` therefore builds with `--workspace=test/tsan/dune-workspace.tsan`, which
adds the native flags to `c_flags`. To check that it worked, look for `__tsan_`
calls in a stub: `objdump -d <exe> | grep -A40 '<lwt_unix_send_notification>:'`.
