# syntax=docker/dockerfile:1

ARG GCC_IMAGE=gcc:14

FROM ${GCC_IMAGE} AS toolchain

SHELL ["/bin/bash", "-eo", "pipefail", "-c"]

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      cmake ninja-build git curl zip unzip tar pkg-config python3 \
      mono-complete ca-certificates-mono && \
    rm -rf /var/lib/apt/lists/*

ENV VCPKG_ROOT=/opt/vcpkg

FROM toolchain AS vcpkg-bootstrap
ARG VCPKG_BASELINE=d13fa75214c258099923cf25a5e6311e58c07f3b
RUN git init "${VCPKG_ROOT}" && \
    git -C "${VCPKG_ROOT}" remote add origin https://github.com/microsoft/vcpkg.git && \
    git -C "${VCPKG_ROOT}" fetch --depth 1 origin "${VCPKG_BASELINE}" && \
    git -C "${VCPKG_ROOT}" checkout FETCH_HEAD && \
    "${VCPKG_ROOT}/bootstrap-vcpkg.sh" -disableMetrics

FROM vcpkg-bootstrap AS deps
WORKDIR /src

COPY vcpkg.json ./
COPY cmake/triplets/ cmake/triplets/

ARG CI_REQUIRE_BINARY_CACHE=0
ARG NUGET_FEED_OWNER
RUN --mount=type=secret,id=github_token \
    if [[ -s /run/secrets/github_token && -n "${NUGET_FEED_OWNER:-}" ]]; then \
      GITHUB_TOKEN="$(cat /run/secrets/github_token)"; \
      FEED="https://nuget.pkg.github.com/${NUGET_FEED_OWNER}/index.json"; \
      NUGET="$("${VCPKG_ROOT}/vcpkg" fetch nuget | tail -n1)"; \
      mono "${NUGET}" sources add \
        -Source "${FEED}" \
        -StorePasswordInClearText \
        -Name GitHub \
        -Username "${NUGET_FEED_OWNER}" \
        -Password "${GITHUB_TOKEN}"; \
      mono "${NUGET}" setapikey "${GITHUB_TOKEN}" -Source "${FEED}"; \
      export VCPKG_BINARY_SOURCES="clear;nuget,GitHub,readwrite"; \
      echo "vcpkg binary cache: NuGet feed at ${FEED}"; \
    else \
      if [[ "${CI_REQUIRE_BINARY_CACHE}" = "1" ]]; then \
        echo "ERROR: CI_REQUIRE_BINARY_CACHE=1 but github_token secret or NUGET_FEED_OWNER is missing." >&2; \
        exit 1; \
      fi; \
      export VCPKG_BINARY_SOURCES="clear;default,readwrite"; \
      echo "vcpkg binary cache: local default (no NuGet credentials provided)"; \
    fi; \
    "${VCPKG_ROOT}/vcpkg" install \
      --x-manifest-root=. \
      --x-install-root=/opt/vcpkg_installed \
      --triplet=x64-linux-static-release \
      --host-triplet=x64-linux-static-release \
      --overlay-triplets=cmake/triplets \
      --x-feature=builtin-cold \
      --clean-after-build 2>&1 | tee /tmp/vcpkg-install.log; \
    if [[ "${CI_REQUIRE_BINARY_CACHE}" = "1" ]]; then \
      RESTORED=$(grep -oE 'Restored [0-9]+ package\(s\) from' /tmp/vcpkg-install.log | grep -oE '[0-9]+' | head -n1 || true); \
      if [[ "${RESTORED:-0}" -gt 0 ]] && grep -qE '^Building (rocksdb|prometheus-cpp|civetweb)\b' /tmp/vcpkg-install.log; then \
        echo "ERROR: heavy package was rebuilt from source despite a warm NuGet cache (Restored=${RESTORED}). Cache key/triplet drift?" >&2; \
        grep -E '^Building [a-z0-9-]+:' /tmp/vcpkg-install.log >&2 || true; \
        exit 1; \
      fi; \
    fi

FROM deps AS build
WORKDIR /src

COPY CMakeLists.txt CMakePresets.json ./
COPY cmake/ cmake/
COPY include/ include/
COPY src/ src/
COPY apps/ apps/

RUN cmake -S . -B build/container -G Ninja \
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
RUN timeout 5 build/container/apps/abyss-server/abyss-server || [[ $? -eq 124 ]]

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /src/build/container/apps/abyss-server/abyss-server /usr/local/bin/abyss-server

EXPOSE 6379 9090 8080

ENTRYPOINT ["abyss-server"]
