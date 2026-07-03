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

# The Ubuntu 20.04 (focal) builder image needs two things newer images already
# provide. This block is scoped to focal only, so all other targets (ubuntu22/24,
# debian, el, amzn) are untouched.
#   1. protoc: greptime-proto's build script needs it, but the focal image does
#      not ship protobuf-compiler (ubuntu22/24 images do).
#   2. A compiler aws-lc-sys accepts: the gRPC/TLS stack pulls in aws-lc-sys,
#      which refuses to build with the GCC 9 focal ships, guarding against
#      https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95189. GCC 10 fixes that bug
#      and still links against glibc 2.31, keeping the NIF compatible with focal.
#      The image has neither clang nor gcc-10, so install gcc-10. Honour a
#      caller-set CC.
if grep -qi focal /etc/os-release 2>/dev/null; then
  need_install=""
  command -v protoc >/dev/null 2>&1 || need_install="protobuf-compiler"
  if [ -z "${CC:-}" ] && ! command -v gcc-10 >/dev/null 2>&1; then
    need_install="${need_install} gcc-10 g++-10"
  fi
  if [ -n "${need_install}" ]; then
    apt-get update
    apt-get install -y --no-install-recommends ${need_install}
  fi
  if [ -z "${CC:-}" ]; then
    export CC=gcc-10 CXX=g++-10
  fi
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
