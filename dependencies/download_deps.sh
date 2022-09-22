#!/usr/bin/env bash
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
LIB_DIR="$SCRIPT_DIR/tarballs"
ONLINE_DIR="$SCRIPT_DIR/online"

sudo apt-get install -y libcgroup-dev libssl-dev libboost-all-dev pkg-config

mkdir -p tarballs
pushd tarballs

GTEST_TAR='googletest-1.10.0.tar.gz'
CXXOPTS_TAR='cxxopts-2.2.1.tar.gz'
LIBEVENT_TAR='libevent-2.1.12.tar.gz'
GRPC_TAR='grpc-1.38.1.tar.gz'
BOOST_TAR='boost-1.76.0.tar.gz'
FMT_TAR='fmt-8.0.1.tar.gz'
SPDLOG_TAR='spdlog-1.8.5.tar.gz'

function check_and_download() {
  local FILE=$1
  local LINK=$2
  if [ ! -f $FILE ]; then
    wget --retry-connrefused --waitretry=1 -O $FILE $LINK
  fi
}

if [ ! -f .extracted ]; then
  check_and_download $GTEST_TAR https://github.com/google/googletest/archive/refs/tags/release-1.11.0.tar.gz
  check_and_download $CXXOPTS_TAR https://github.com/jarro2783/cxxopts/archive/refs/tags/v2.2.1.tar.gz
  check_and_download $LIBEVENT_TAR https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz
  check_and_download $FMT_TAR https://github.com/fmtlib/fmt/archive/refs/tags/8.0.1.tar.gz
  check_and_download $SPDLOG_TAR https://github.com/gabime/spdlog/archive/refs/tags/v1.8.5.tar.gz

  tar xzf $GTEST_TAR -C $LIB_DIR
  tar xzf $CXXOPTS_TAR -C $LIB_DIR
  tar xzf $LIBEVENT_TAR -C $LIB_DIR
  tar xzf $FMT_TAR -C $LIB_DIR
  tar xzf $SPDLOG_TAR -C $LIB_DIR

  touch $LIB_DIR/.extracted
fi

# Download grpc and its dependencies.
git clone -b v1.38.1 https://github.com/grpc/grpc grpc-1.38.1
cd grpc-1.38.1
git submodule update --init

popd

CMAKE_DEFS=("-DCMAKE_CXX_STANDARD=17" "-G" "Ninja")

# GTest
pushd $LIB_DIR/googletest-release-1.11.0 &&
  mkdir -p build && cd build &&
  cmake -DCMAKE_INSTALL_PREFIX="$ONLINE_DIR/google-test" "${CMAKE_DEFS[@]}" .. && ninja install &&
  popd

# cxxopts
pushd $LIB_DIR/cxxopts-2.2.1 &&
  mkdir -p build && cd build &&
  cmake -DCMAKE_INSTALL_PREFIX=$ONLINE_DIR/cxxopts "${CMAKE_DEFS[@]}" .. && ninja install &&
  popd

# LibEvent
pushd $LIB_DIR/libevent-2.1.12-stable &&
  mkdir -p build && cd build &&
  cmake -DCMAKE_INSTALL_PREFIX=$ONLINE_DIR/libevent \
    -DEVENT__DISABLE_TESTS=ON \
    -DEVENT__DISABLE_BENCHMARK=ON \
    "${CMAKE_DEFS[@]}" \
    .. && ninja install &&
  popd

# GRPC
pushd $LIB_DIR/grpc-1.38.1 &&
  mkdir -p cmake/build && pushd cmake/build &&
  cmake -DCMAKE_INSTALL_PREFIX=$ONLINE_DIR/grpc "${CMAKE_DEFS[@]}" \
    ../.. &&
  ninja install &&
  popd &&

  # Install Abseil
  mkdir -p third_party/abseil-cpp/cmake/build &&
  pushd third_party/abseil-cpp/cmake/build &&
  cmake -DCMAKE_INSTALL_PREFIX=$ONLINE_DIR/abseil \
    -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE \
    "${CMAKE_DEFS[@]}" \
    ../.. &&
  ninja install &&
  popd &&
  popd

# fmt
pushd $LIB_DIR/fmt-8.0.1 &&
  mkdir -p build && cd build &&
  cmake -DCMAKE_INSTALL_PREFIX=$ONLINE_DIR/fmt \
    "${CMAKE_DEFS[@]}" .. && ninja install &&
  popd

# spdlog
pushd $LIB_DIR/spdlog-1.8.5 &&
  mkdir -p build && cd build &&
  cmake -DCMAKE_INSTALL_PREFIX=$ONLINE_DIR/spdlog -DCMAKE_POSITION_INDEPENDENT_CODE=ON "${CMAKE_DEFS[@]}" .. &&
  ninja install &&
  popd

# libuv
# cmake -DCMAKE_CXX_STANDARD=17 -G Ninja -DCMAKE_INSTALL_PREFIX=../../../online/libuv -DBUILD_TESTING=OFF ..