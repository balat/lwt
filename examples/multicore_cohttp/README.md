# A real HTTP stack on N Lwt loops

`cohttp-lwt-unix`, unchanged, served by one Lwt loop per domain, each with its own
listening socket on the same port through `SO_REUSEPORT`. The point is not the
throughput: it is that a stack written years before any of this, knowing nothing
about domains, runs on N loops with the recipe entirely on the application's side.

It is built OUTSIDE the Lwt tree, in a switch where this branch's `lwt` and
`cohttp-lwt-unix` are both installed. In-tree is not possible: dune would see two
libraries named `lwt`, the one built from `src/core` and the installed one that
`cohttp-lwt-unix` links.

```
cp -r examples/multicore_cohttp /tmp/mc && cd /tmp/mc
dune build ./server.exe
./_build/default/server.exe 4 8080
wrk -t4 -c64 -d5s http://127.0.0.1:8080/
```

## One thing in the way, and it is not in Lwt

At the time of writing, this dies on the first completed request with

```
Invalid_argument("Lwt_condition.broadcast: belongs to another domain. ...")
```

and the ownership check is right to say so. `conduit-lwt-unix` keeps a process-wide
connection throttle:

```ocaml
(* File descriptors are a global resource so this has to be a global limit too *)
let maxactive = ref None
let active = ref 0
let cond = Lwt_condition.create ()        (* created at module initialisation *)

let disconnected () =
  decr active;
  Lwt_condition.broadcast cond ()         (* on whichever domain closed a connection *)
```

An `Lwt_condition.t` belongs to the loop that created it, because that loop is what
runs its waiters' callbacks. Broadcasting it from another domain would wake those
waiters from the wrong thread, which is why it raises instead.

Nobody is ever waiting on that condition unless a limit is configured, and
`maxactive` is `None` by default, so one line makes the default configuration work:

```ocaml
let disconnected () =
  decr active;
  if !maxactive <> None then Lwt_condition.broadcast cond ()
```

With that, four loops serve every request. Configuring a limit with several loops
needs more than this (a condition per loop, and `active` should be an `Atomic.t`
rather than a `ref` that several domains increment), which is a conduit design
question rather than a patch.

## And a second thing, rarer and nastier: shared lazies

Once, on four loops, the server died at startup with

```
Fatal error: exception CamlinternalLazy.Undefined
```

reported by two domains at the same instant. `Cohttp_lwt_unix.Server.create` takes
`?ctx`, whose default argument is `Lazy.force Net.default_ctx`, and that lazy forces
`Conduit_lwt_unix.default_ctx`, which forces the TLS authenticator, which reads the
system certificate store. Three process-wide lazies, forced by whichever domain calls
`Server.create` first, and OCaml documents forcing one lazy from several domains at
once as unsafe: the loser gets `Undefined`.

It did not reproduce in the forty-odd startups that followed (six loops, twelve
loops, both), which makes it worse rather than better: a startup crash that appears
once in dozens of runs is exactly what reaches production.

The application can remove the whole class without touching the library, by forcing
the value once before any other domain exists, which is what `server.ml` does:

```ocaml
let shared_ctx = Lazy.force Cohttp_lwt_unix.Net.default_ctx
...
Cohttp_lwt_unix.Server.create ~ctx:shared_ctx ~mode ...
```

**The general rule, worth more than this instance: force the shared lazies of your
libraries on the main domain, before spawning.** A `lazy` at module level is a
process-wide resource pretending to be a local convenience.

## Measured here, on one machine

Six physical cores, the load generator sharing them with the server, so these
numbers understate anything past two loops and are not a property of Lwt:

| Loops | pass 1 | pass 2 |
|---|---|---|
| 1 | 71 002 req/s | 59 778 req/s |
| 2 | 116 698 req/s | 119 886 req/s |
| 4 | 172 568 req/s | 173 836 req/s |
| 6 | 202 468 req/s | 188 258 req/s |

What this establishes is the shape and the absence of errors, not a ceiling: two
loops carry about 1.8 times one loop, and no request was lost or refused at any
point.
