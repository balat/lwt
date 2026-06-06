#!/bin/bash
# Build the themed Lwt documentation (manual + API) for ocsigen.org, from the
# CURRENT checkout. Produces one version directory ready to publish on the
# project's gh-pages (served at https://ocsigen.org/lwt/).
#
# Lwt is a modern dune project with SEVERAL packages (lwt, lwt_ppx, lwt_react,
# lwt_retry) but NO client/server split, so `dune build @doc` (plain odoc) builds
# them all into one tree — no odoc-driver needed. The manual is ALREADY .mld
# (docs/manual.mld) and lives in the lwt package (docs/dune:
# `(documentation (package lwt))`); the package landing is src/core/index.mld.
# The API already uses native odoc refs ({!…}); the manual's cross-package
# references ({!Ppx_lwt}, …) are linked by resolve-siblings.py since odoc only
# resolves along dependency edges.
#
# Pipeline (see doc/README.md):
#   1. dune build @doc      -> odoc HTML for the manual AND the API of every lwt*
#                              package, in one run.
#   2. wodoc assemble       -> wrap every page in the Ocsigen site chrome
#                              (header/menu/drawer, version <select>, left nav).
#   3. resolve-siblings.py  -> make cross-package references (lwt -> lwt_ppx/
#                              lwt_react/lwt_retry) clickable.
#
# Links are version-relative via the {{base}} token; only the version <select> is
# absolute, via {{pub}} = /lwt. The themed CSS is served centrally at
# /css/ocsigen-odoc.css by ocsigen.org.
#
# Usage: build.sh <label> [outdir]
#   label   version label / output subdir (e.g. dev, 6.0.0); NEVER "latest"
#   outdir  where to write <label>/ (default: _doc-site, gitignored)
#
#   WODOC   path to the wodoc binary (default: wodoc from PATH/opam)
set -e

LABEL="$1"
[ -n "$LABEL" ] || { echo "usage: build.sh <label> [outdir]" >&2; exit 2; }
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
OUTDIR="${2:-$ROOT/_doc-site}"
OUT="$OUTDIR/$LABEL"
WODOC="${WODOC:-wodoc}"
PUB="${PUB:-/lwt}"

cd "$ROOT"
dune build @doc
SRC="$ROOT/_build/default/_doc/_html"
[ -d "$SRC/lwt" ] || { echo "no lwt package output in $SRC" >&2; exit 1; }

rm -rf "$OUT"; mkdir -p "$OUT"

# Manual left-column nav, from the canonical menu kept here.
NAV_MANUAL="$(mktemp)"
python3 "$HERE/gen-manual-nav.py" "$HERE/menu.wiki" "{{base}}" >"$NAV_MANUAL"

# Version <select> options: every sibling version directory already published.
VERSIONS="$(mktemp)"
{
  echo "              <option value=\"latest\">latest</option>"
  for d in "$OUTDIR"/*/; do
    v="$(basename "$d")"
    [ "$v" = latest ] && continue
    echo "              <option value=\"$v\">$v</option>"
  done
  echo "              <option value=\"$LABEL\">$LABEL</option>"
} 2>/dev/null | awk '!seen[$0]++' >"$VERSIONS"

TMPL="$(mktemp)"
sed -e "/{{leftnav}}/r $HERE/leftnav.html" -e "/{{leftnav}}/d" "$HERE/template.html" \
  | sed -e "s#{{pub}}#$PUB#g" \
        -e "/{{versions}}/r $VERSIONS" -e "/{{versions}}/d" \
        -e "/{{manual_nav}}/r $NAV_MANUAL" -e "/{{manual_nav}}/d" \
  >"$TMPL"

# Assemble every page across ALL package subtrees, mirroring odoc's layout. The
# current project is always lwt. Skip odoc's support dir and the top-level
# package-list index (replaced by a redirect below).
(cd "$SRC" && find . -name '*.html' -not -path './odoc.support/*') | while read -r page; do
  rel="${page#./}"
  [ "$rel" = "index.html" ] && continue
  slashes="${rel//[!\/]/}"; depth=${#slashes}
  if [ "$depth" -eq 0 ]; then base="."; else
    base=""; for _ in $(seq 1 "$depth"); do base="../$base"; done; base="${base%/}"
  fi
  mkdir -p "$OUT/$(dirname "$rel")"
  "$WODOC" assemble --template "$TMPL" --current "lwt" --base "$base" \
    "$SRC/$rel" >"$OUT/$rel"
  python3 "$HERE/resolve-siblings.py" "$base" "$OUT/$rel"
done

rm -f "$TMPL" "$NAV_MANUAL" "$VERSIONS"

# Manual assets, if the manual ever references {{image:files/…}} (none today).
if [ -d "$ROOT/docs/files" ]; then
  mkdir -p "$OUT/lwt/files"
  cp -RL "$ROOT/docs/files/." "$OUT/lwt/files/" 2>/dev/null || \
    cp -R "$ROOT/docs/files/." "$OUT/lwt/files/"
fi

# Version root -> package home (the lwt landing page).
cat >"$OUT/index.html" <<EOF
<!DOCTYPE html>
<html><head><meta charset="utf-8"/>
<meta http-equiv="refresh" content="0; url=lwt/index.html"/>
<link rel="canonical" href="lwt/index.html"/>
<title>Lwt documentation</title></head>
<body><p>Redirecting to the <a href="lwt/index.html">Lwt documentation</a>.</p></body>
</html>
EOF

SF="$(mktemp -d)"; odoc support-files -o "$SF" >/dev/null 2>&1 \
  && cp "$SF/highlight.pack.js" "$OUT/highlight.pack.js"; rm -rf "$SF"
cp "$HERE/lwt-highlight.js" "$OUT/lwt-highlight.js"

echo "built lwt $LABEL: $(find "$OUT" -name '*.html' | wc -l) pages -> $OUT"
