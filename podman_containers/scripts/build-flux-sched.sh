#!/bin/bash
set -euo pipefail

export SRC_ROOT=/workspace
export INSTALL_ROOT="$SRC_ROOT/container-installs"
export FLUX_PREFIX="$INSTALL_ROOT/flux-core"

git config --global --add safe.directory "$SRC_ROOT/flux-sched" >/dev/null 2>&1 || true
source /usr/local/bin/flux-dev-env.sh

if [ -z "${FLUX_SCHED_VERSION:-}" ]; then
  FLUX_SCHED_GIT_VERSION="$(cd "$SRC_ROOT/flux-sched" && git describe --tags --always 2>/dev/null || true)"
  case "$FLUX_SCHED_GIT_VERSION" in
    ''|*[!0-9.]*)
      export FLUX_SCHED_VERSION=0.0.0
      ;;
    *)
      export FLUX_SCHED_VERSION="${FLUX_SCHED_GIT_VERSION#v}"
      ;;
  esac
fi

cd "$SRC_ROOT/flux-sched"
mkdir -p build
CC=gcc-12 CXX=g++-12 cmake -S . -B build \
  -DCMAKE_INSTALL_PREFIX="$FLUX_PREFIX" \
  -DCMAKE_PREFIX_PATH="$FLUX_PREFIX" \
  -DENABLE_DOCS=Off
cmake --build build -j"$(nproc)"
cmake --install build

echo
echo "flux-sched installed to: $FLUX_PREFIX"
