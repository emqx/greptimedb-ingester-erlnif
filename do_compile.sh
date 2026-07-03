#!/usr/bin/env bash

set -euo pipefail

BUILD_SCRIPT="crates/greptimedb-ingester-nif/build.rs"
NIF_PATH="priv/libgreptimedb_nif.so"

if ./do_prebuilt.sh; then
  exit 0
else
  echo "No prebuilt artifacts, building from source"
fi

# touch the build.rs to force cargo to rerun build script and generate libpath file
touch "${BUILD_SCRIPT}"

# `aws-lc-sys` (pulled in transitively via the gRPC/TLS stack) refuses to build
# with the GCC 9 shipped on Ubuntu 20.04 (focal), guarding against
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95189. Build with clang there
# instead: it is unaffected by the bug and still links against glibc 2.31, so
# the produced NIF stays compatible with Ubuntu 20.04. Only applied on focal,
# only when the caller has not already chosen a compiler, and only if clang is
# available.
if [ -z "${CC:-}" ] && grep -qi focal /etc/os-release 2>/dev/null && command -v clang >/dev/null 2>&1; then
  export CC=clang CXX=clang++
fi

cargo build --release

# Should always be `.so`, OTP on macos won't load `.dylib` files.
cp $(cat ./libpath) "${NIF_PATH}"

if [ "${BUILD_RELEASE:-}" = 1 ]; then
  PKGNAME="$(./pkgname.sh)"
  if [ -z "$PKGNAME" ]; then
    echo "unable_to_resolve_release_package_name"
    exit 1
  fi
  mkdir -p _packages
  TARGET="_packages/${PKGNAME}"
  gzip -c "${NIF_PATH}" > "$TARGET"
  # use openssl but not sha256sum command because in some macos env it does not exist
  if command -v openssl; then
    openssl dgst -sha256 "${TARGET}" | cut -d ' ' -f 2  > "${TARGET}.sha256"
  else
    sha256sum "${TARGET}"  | cut -d ' ' -f 1 > "${TARGET}.sha256"
  fi
fi
