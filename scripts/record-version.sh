#!/usr/bin/env bash
#
# Record the tested target's version into results/<target>.version, so the
# results table can show which build each run was against. Best-effort: writes
# "-" when nothing is resolvable. Called by each conformance CI job.
#
#   scripts/record-version.sh <target>
#
# Per-target inputs:
#   EXTENDDB_REF   extenddb — the release tag/ref that was built
#   TARGET_IMAGE   container targets — the image ref, used to resolve a digest
#
# Sources, by target:
#   dynamodb              live AWS service (no version)
#   dynoxide / dynalite   npm published version (`npm view`)
#   localstack            its /_localstack/info version endpoint
#   extenddb              the built release tag
#   container `:latest`   the resolved image digest (pins what latest was)
set -uo pipefail

target="${1:?usage: record-version.sh <target>}"
mkdir -p results

ver=""
case "$target" in
  dynamodb)   ver="live (AWS)" ;;
  extenddb)   ver="${EXTENDDB_REF:-}" ;;
  dynoxide)   ver=$(npm view dynoxide version 2>/dev/null) ;;
  dynalite)   ver=$(npm view dynalite version 2>/dev/null) ;;
  localstack)
    ver=$(curl -fsS http://localhost:4566/_localstack/info 2>/dev/null | jq -r '.version // empty')
    ver=${ver%%:*} # drop any :git-hash suffix
    ;;
esac

# Container targets (and a fallback for any image-backed target): the resolved
# image digest pins what `:latest` actually was at run time.
if [ -z "$ver" ] && [ -n "${TARGET_IMAGE:-}" ]; then
  digest=$(docker inspect --format '{{ if .RepoDigests }}{{ index .RepoDigests 0 }}{{ end }}' "$TARGET_IMAGE" 2>/dev/null)
  if [ -n "$digest" ]; then
    ver="${digest##*sha256:}"
    ver="${ver:0:12}"
  fi
fi

[ -z "$ver" ] && ver="-"
printf '%s\n' "$ver" >"results/${target}.version"
echo "recorded ${target} version: ${ver}"

# Region the target was exercised in. Meaningful chiefly for the ground-truth
# dynamodb run (emulator targets sign for a placeholder region). Written to a
# separate sibling file so summarise.mjs stays byte-stable, and so the results
# table and paritysuite.org can render "observed in <region>" from data rather
# than hand-authoring it.
region="${AWS_REGION:-}"
[ -z "$region" ] && region="-"
printf '%s\n' "$region" >"results/${target}.region"
echo "recorded ${target} region: ${region}"
