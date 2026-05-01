#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="build"
BUILD_TYPE="RelWithDebInfo"
RUN_TEST=0
CLEAN=0
INSTALL_DEPS=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --clean         Remove the build directory before configuring
  --test          Run parquet linux_engine_smoke after building
  --install-deps  Install build dependencies using apt-get on Linux or brew on macOS
  -h, --help      Show this help message

Examples:
  $0 --clean
  $0 --clean --test
  JOBS=1 $0 --clean --test
  $0 --install-deps --clean --test
USAGE
}

for arg in "$@"; do
  case "$arg" in
    --clean)
      CLEAN=1
      ;;
    --test)
      RUN_TEST=1
      ;;
    --install-deps)
      INSTALL_DEPS=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg"
      usage
      exit 1
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OS_NAME="$(uname -s)"

if [[ "$OS_NAME" == "Linux" ]]; then
  CPU_COUNT="$(nproc)"
elif [[ "$OS_NAME" == "Darwin" ]]; then
  CPU_COUNT="$(sysctl -n hw.ncpu)"
else
  echo "Unsupported OS: $OS_NAME"
  exit 1
fi

# DuckDB compilation is memory-heavy.
# Default to max 2 jobs to avoid cc1plus getting killed.
# Override manually with:
#   JOBS=1 ./scripts/build_parquet.sh --clean --test
#   JOBS=4 ./scripts/build_parquet.sh --clean --test
if [[ -z "${JOBS:-}" ]]; then
  if [[ "$CPU_COUNT" -gt 2 ]]; then
    JOBS=2
  else
    JOBS="$CPU_COUNT"
  fi
fi

export CMAKE_BUILD_PARALLEL_LEVEL="$JOBS"
export MAKEFLAGS="-j$JOBS"

echo "==> Repo root: $REPO_ROOT"
echo "==> OS: $OS_NAME"
echo "==> Build dir: $BUILD_DIR"
echo "==> CPU count: $CPU_COUNT"
echo "==> Build jobs: $JOBS"

if [[ "$INSTALL_DEPS" -eq 1 ]]; then
  if [[ "$OS_NAME" == "Linux" ]]; then
    echo "==> Installing Linux dependencies"
    sudo apt-get update
    sudo apt-get install -y \
      build-essential \
      bison \
      cmake \
      flex \
      libaio-dev \
      libcurl4-openssl-dev \
      libncurses5-dev \
      libpcre2-dev \
      libperl-dev \
      libsnappy-dev \
      libssl-dev \
      libsystemd-dev \
      ninja-build \
      perl \
      pkg-config \
      zlib1g-dev
  elif [[ "$OS_NAME" == "Darwin" ]]; then
    echo "==> Installing macOS dependencies"

    if ! command -v brew >/dev/null 2>&1; then
      echo "Homebrew is required for --install-deps on macOS."
      exit 1
    fi

    brew install \
      bison \
      cmake \
      curl \
      flex \
      ninja \
      openssl \
      pcre2 \
      pkg-config \
      snappy \
      zlib
  fi
fi

echo "==> Updating submodules"
git submodule update --init --recursive

if [[ "$CLEAN" -eq 1 ]]; then
  echo "==> Removing old build directory"
  rm -rf "$BUILD_DIR"
fi

if command -v ninja >/dev/null 2>&1; then
  GENERATOR_ARGS=(-G Ninja)
else
  GENERATOR_ARGS=()
fi

if [[ "$OS_NAME" == "Darwin" ]]; then
  # Homebrew libraries often live outside default compiler search paths.
  if [[ -d "/opt/homebrew" ]]; then
    export CMAKE_PREFIX_PATH="/opt/homebrew/opt/openssl:/opt/homebrew/opt/curl:/opt/homebrew/opt/zlib:/opt/homebrew/opt/pcre2:${CMAKE_PREFIX_PATH:-}"
  elif [[ -d "/usr/local" ]]; then
    export CMAKE_PREFIX_PATH="/usr/local/opt/openssl:/usr/local/opt/curl:/usr/local/opt/zlib:/usr/local/opt/pcre2:${CMAKE_PREFIX_PATH:-}"
  fi
fi

echo "==> Configuring MariaDB with Parquet as a dynamic plugin"

cmake -S . -B "$BUILD_DIR" "${GENERATOR_ARGS[@]}" \
  -DCMAKE_CXX_STANDARD=17 \
  -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
  -DWITH_UNIT_TESTS=OFF \
  -DPLUGIN_PARQUET=DYNAMIC \
  -DPLUGIN_MROONGA=NO \
  -DPLUGIN_ROCKSDB=NO \
  -DPLUGIN_COLUMNSTORE=NO \
  -DPLUGIN_CONNECT=NO \
  -DPLUGIN_SPIDER=NO \
  -DPLUGIN_S3=NO \f
  -DPLUGIN_OQGRAPH=NO \
  -DPLUGIN_TOKUDB=NO \
  -DPLUGIN_ARCHIVE=NO \
  -DPLUGIN_BLACKHOLE=NO \
  -DPLUGIN_EXAMPLE=NO \
  -DPLUGIN_FEDERATED=NO \
  -DPLUGIN_FEEDBACK=NO

echo "==> Building MariaDB, Parquet plugin, DuckDB dependency, and MTR infrastructure"
cmake --build "$BUILD_DIR" --parallel "$JOBS"

echo "==> Verifying expected build artifacts"

if [[ ! -f "$BUILD_DIR/sql/mariadbd" ]]; then
  echo "Missing server binary: $BUILD_DIR/sql/mariadbd"
  exit 1
fi

if [[ ! -f "$BUILD_DIR/mysql-test/lib/My/SafeProcess/my_safe_process" ]]; then
  echo "Missing MTR helper: $BUILD_DIR/mysql-test/lib/My/SafeProcess/my_safe_process"
  exit 1
fi

PARQUET_PLUGIN="$(find "$BUILD_DIR/storage/parquet" -maxdepth 2 -type f \( -name 'ha_parquet.so' -o -name 'ha_parquet.dylib' \) | head -n 1 || true)"

if [[ -z "$PARQUET_PLUGIN" ]]; then
  echo "Missing Parquet plugin under $BUILD_DIR/storage/parquet"
  exit 1
fi

echo "==> Build succeeded"
echo "    Server:  $BUILD_DIR/sql/mariadbd"
echo "    Plugin:  $PARQUET_PLUGIN"
echo "    Jobs:    $JOBS"

if [[ "$RUN_TEST" -eq 1 ]]; then
  echo "==> Running Parquet smoke test"

  if [[ -f "$BUILD_DIR/mysql-test/mysql-test-run.pl" ]]; then
    MTR="$BUILD_DIR/mysql-test/mysql-test-run.pl"
  elif [[ -f "$BUILD_DIR/mysql-test/mariadb-test-run.pl" ]]; then
    MTR="$BUILD_DIR/mysql-test/mariadb-test-run.pl"
  else
    echo "Could not find mysql-test-run.pl or mariadb-test-run.pl"
    exit 1
  fi

  perl "$MTR" --suite=parquet linux_engine_smoke

  echo "==> Smoke test passed"
fi

