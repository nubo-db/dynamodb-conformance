#!/usr/bin/env bash
#
# Build the Dynoxide WebAssembly engine and stand it up on an HTTP port.
#
# The wasm build is the only target the suite cannot reach with a container
# image or an npm binary. It has no server of its own: it answers a postMessage
# RPC inside a browser. Dynoxide ships a shim that speaks the DynamoDB HTTP API
# and forwards each request into a headless Chromium running the built dist/,
# so the suite sees a port like every other target. The shim is not published,
# which is why this needs a Dynoxide checkout rather than an install.
#
# Used by the Dynoxide (wasm) CI job and runnable locally.
#
# Required:
#   DYNOXIDE_DIR                path to a Dynoxide checkout
#
# Optional (defaults shown):
#   DYNOXIDE_WASM_PORT=8003     the DynamoDB endpoint the suite drives
#   DYNOXIDE_WASM_ASSET_PORT=8004  the shim's internal static server
#   BUILD=1                     set 0 to reuse an existing dist/
#
# On success it emits the suite env (DYNAMODB_ENDPOINT and placeholder AWS
# credentials). In CI ($GITHUB_ENV set) it appends them there; otherwise it
# prints `export` lines suitable for
# `eval "$(DYNOXIDE_DIR=... ./scripts/run-dynoxide-wasm.sh)"`.
set -euo pipefail

: "${DYNOXIDE_DIR:?set DYNOXIDE_DIR to a Dynoxide checkout}"
PORT=${DYNOXIDE_WASM_PORT:-8003}
ASSET_PORT=${DYNOXIDE_WASM_ASSET_PORT:-8004}
LOG=${DYNOXIDE_WASM_LOG:-/tmp/dynoxide-wasm-bridge.log}

cd "$DYNOXIDE_DIR"

if [ "${BUILD:-1}" = "1" ]; then
  echo "==> npm ci (dynoxide)" >&2
  npm ci >&2

  # The shim drives Chromium through Playwright and will install it on first
  # run, but doing it here keeps a browser-download failure attributable to its
  # own step, and --with-deps pulls the system libraries a bare CI image lacks.
  echo "==> install chromium" >&2
  npx playwright install --with-deps chromium >&2

  echo "==> build the wasm bundle" >&2
  scripts/build-wasm.sh >&2
fi

echo "==> start the wasm engine on :${PORT}" >&2
# Detached, because the conformance run is a later step and the runner would
# otherwise reap the shim with this shell.
nohup npm run wasm:serve -- --port "$PORT" --asset-port "$ASSET_PORT" >"$LOG" 2>&1 &

# Ready means answering the DynamoDB API, not merely holding the port: the shim
# binds only after Chromium has loaded the bundle and the engine has reported
# its contract version. An unauthenticated ListTables is rejected rather than
# served, and that rejection is proof enough that the whole chain is up.
ready=0
for _ in $(seq 1 120); do
  code=$(curl -s -o /dev/null -w '%{http_code}' \
    -X POST "http://127.0.0.1:${PORT}/" \
    -H 'X-Amz-Target: DynamoDB_20120810.ListTables' \
    -H 'Content-Type: application/x-amz-json-1.0' \
    -d '{}' 2>/dev/null || true)
  if [ -n "$code" ] && [ "$code" != "000" ]; then ready=1; break; fi
  sleep 2
done
if [ "$ready" -ne 1 ]; then
  echo "ERROR: the wasm engine did not answer on :${PORT}" >&2
  echo "--- bridge log ---" >&2
  cat "$LOG" >&2 || true
  exit 1
fi

emit() { # name value
  if [ -n "${GITHUB_ENV:-}" ]; then
    printf '%s=%s\n' "$1" "$2" >>"$GITHUB_ENV"
  else
    printf "export %s='%s'\n" "$1" "$2"
  fi
}
emit DYNAMODB_ENDPOINT "http://127.0.0.1:${PORT}"
emit AWS_ACCESS_KEY_ID "fakeAccessKeyId"
emit AWS_SECRET_ACCESS_KEY "fakeSecretAccessKey"
emit AWS_REGION "us-east-1"

echo "==> Dynoxide wasm ready at http://127.0.0.1:${PORT}" >&2
