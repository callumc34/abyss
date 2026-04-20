# syntax=docker/dockerfile:1

ARG GCC_IMAGE=gcc:14

FROM ${GCC_IMAGE} AS toolchain

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      cmake ninja-build git curl zip unzip tar pkg-config python3 && \
    rm -rf /var/lib/apt/lists/*

ENV VCPKG_ROOT=/opt/vcpkg

FROM toolchain AS vcpkg-bootstrap
ARG VCPKG_BASELINE=d13fa75214c258099923cf25a5e6311e58c07f3b
RUN set -eu; \
    git init "${VCPKG_ROOT}"; \
    git -C "${VCPKG_ROOT}" remote add origin https://github.com/microsoft/vcpkg.git; \
    git -C "${VCPKG_ROOT}" fetch --depth 1 origin "${VCPKG_BASELINE}"; \
    git -C "${VCPKG_ROOT}" checkout FETCH_HEAD; \
    "${VCPKG_ROOT}/bootstrap-vcpkg.sh" -disableMetrics

FROM vcpkg-bootstrap AS deps
WORKDIR /src

COPY vcpkg.json ./
COPY cmake/triplets/ cmake/triplets/

ARG CI_REQUIRE_BINARY_CACHE=0
RUN --mount=type=secret,id=actions_cache_url \
    --mount=type=secret,id=actions_results_url \
    --mount=type=secret,id=actions_runtime_token \
    --mount=type=cache,target=/var/cache/vcpkg,sharing=locked \
    set -eu; \
    if [ -s /run/secrets/actions_runtime_token ]; then \
      export ACTIONS_CACHE_URL="$(cat /run/secrets/actions_cache_url 2>/dev/null || true)"; \
      export ACTIONS_RESULTS_URL="$(cat /run/secrets/actions_results_url 2>/dev/null || true)"; \
      export ACTIONS_RUNTIME_TOKEN="$(cat /run/secrets/actions_runtime_token)"; \
      export VCPKG_BINARY_SOURCES="clear;files,/var/cache/vcpkg,readwrite;x-gha,readwrite"; \
      echo "vcpkg binary cache: x-gha + files (cache_url=${ACTIONS_CACHE_URL:+set} results_url=${ACTIONS_RESULTS_URL:+set} token_len=${#ACTIONS_RUNTIME_TOKEN})"; \
    else \
      if [ "${CI_REQUIRE_BINARY_CACHE}" = "1" ]; then \
        echo "ERROR: CI_REQUIRE_BINARY_CACHE=1 but actions_runtime_token secret is empty; refusing to build dependencies from source." >&2; \
        exit 1; \
      fi; \
      export VCPKG_BINARY_SOURCES="clear;files,/var/cache/vcpkg,readwrite"; \
      echo "vcpkg binary cache: files-only (no actions_runtime_token secret provided)"; \
    fi; \
    "${VCPKG_ROOT}/vcpkg" install \
      --x-manifest-root=. \
      --x-install-root=/opt/vcpkg_installed \
      --triplet=x64-linux-static-release \
      --host-triplet=x64-linux-static-release \
      --overlay-triplets=cmake/triplets \
      --x-feature=builtin-cold \
      --clean-after-build

FROM deps AS build
WORKDIR /src

COPY CMakeLists.txt CMakePresets.json ./
COPY cmake/ cmake/
COPY include/ include/
COPY src/ src/
COPY apps/ apps/

RUN set -eu; \
    cmake -S . -B build/container -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DABYSS_BUILD_TESTS=OFF \
      -DABYSS_WERROR=ON \
      -DABYSS_USE_CCACHE=OFF \
      -DCMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" \
      -DVCPKG_TARGET_TRIPLET=x64-linux-static-release \
      -DVCPKG_HOST_TRIPLET=x64-linux-static-release \
      -DVCPKG_OVERLAY_TRIPLETS=/src/cmake/triplets \
      -DVCPKG_MANIFEST_INSTALL=OFF \
      -DVCPKG_INSTALLED_DIR=/opt/vcpkg_installed \
      -DCMAKE_EXE_LINKER_FLAGS="-static-libstdc++ -static-libgcc" && \
    cmake --build build/container --target abyss-server

RUN ldd build/container/apps/abyss-server/abyss-server || true
RUN timeout 5 build/container/apps/abyss-server/abyss-server || [ $? -eq 124 ]

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /src/build/container/apps/abyss-server/abyss-server /usr/local/bin/abyss-server

EXPOSE 6379 9090 8080

ENTRYPOINT ["abyss-server"]
