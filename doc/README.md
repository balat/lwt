# How the Lwt documentation is generated

The Lwt documentation published at <https://ocsigen.org/lwt/> is built entirely
with **odoc** and themed with the Ocsigen site chrome by
[**wodoc**](https://github.com/ocsigen/wodoc) (an odoc driver). The same odoc
sources are also what ocaml.org renders.

## Sources

| What | Where | Format |
|---|---|---|
| Manual | [`docs/manual.mld`](../docs/manual.mld) | odoc page |
| Package landing | [`src/core/index.mld`](../src/core/index.mld) | odoc page |
| API | the `.mli` of every `lwt*` package | odoc comments (native `{!…}` refs) |
| Left-column navigation | [`doc/menu.wiki`](menu.wiki) | wikicreole (nav only) |
| Theme / chrome | [`doc/template.html`](template.html), [`doc/leftnav.html`](leftnav.html) | HTML with `{{holes}}` |

Lwt is a modern dune project with several packages (`lwt`, `lwt_ppx`,
`lwt_react`, `lwt_retry`) but no client/server split, so a single
`dune build @doc` (plain odoc) builds the manual **and** the API of every package
in one run. This replaces the former ocamldoc/wikidoc pipeline.

## Build

```
WODOC=$(which wodoc) doc/build.sh <label> [outdir]   # e.g. doc/build.sh dev
```

`doc/build.sh`:

1. `dune build @doc` — odoc HTML for the manual and the API of every `lwt*`
   package, in one run, into `_build/default/_doc/_html/`.
2. `wodoc assemble` — wraps every page in the Ocsigen chrome (header, menu,
   drawer, version `<select>`, left navigation from `doc/menu.wiki`).
3. `doc/resolve-siblings.py` — `dune build @doc` only resolves references along
   dependency edges, so references from the base `lwt` package to its sibling
   packages (`lwt_ppx`, `lwt_react`, `lwt_retry`) are turned into relative links
   to their subtree in the same output.

Output goes to `<outdir>/<label>/` (default `_doc-site/<label>/`), laid out to
match `https://ocsigen.org/lwt/<label>/`. Internal links are version-relative
(the `{{base}}` token); only the version `<select>` is absolute (`{{pub}}` =
`/lwt`). The themed stylesheet is served centrally at `/css/ocsigen-odoc.css` by
ocsigen.org.

## Deployment (CI)

[`.github/workflows/doc.yml`](../.github/workflows/doc.yml) builds and publishes
to the project's **`gh-pages`** branch (served at `ocsigen.org/lwt/`):

- **push to `master`** → rebuilds and deploys the **`dev`** docs (`dev/`).
- **manual run** (Actions → *Documentation* → *Run workflow*) → builds any
  version. For a release: set *label* to the version (e.g. `6.0.0`), *ref* to the
  tag, and tick *set_latest* to repoint `latest`. A tag cut **before** this
  migration has no `doc/` infra, so the workflow overlays the (version-independent)
  doc sources from `master`.

Each run replaces only its own `<label>/` directory; the other version
directories already on `gh-pages` are preserved.
