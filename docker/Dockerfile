# syntax=docker/dockerfile:1

ARG GCC_IMAGE=gcc:14-bookworm@sha256:5e927c284bf55a7dc796262e311a0703344f62f41f5621eb56843111b1d37e15

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

# tini: a tiny init that owns PID-1 responsibilities (signal forwarding +
# zombie reaping) so abyss-server runs as a normal child with standard SIGTERM
# disposition (G7). distroless does not ship tini, so fetch and checksum-pin it
# here (digest discipline matches the vcpkg/base-image pinning) and COPY into
# the final stage.
ARG TINI_VERSION=v0.19.0
ARG TINI_SHA256=c5b0666b4cb676901f90dfcb37106783c5fe2077b04590973b885950611b30ee
RUN curl -fsSL "https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static-amd64" \
      -o /tini && \
    echo "${TINI_SHA256}  /tini" | sha256sum -c - && \
    chmod +x /tini

ARG RUNTIME_GLIBC_MAX=2.36
RUN bin=build/container/apps/abyss-server/abyss-server; \
    max=$(objdump -T "${bin}" | grep -oE 'GLIBC_[0-9]+\.[0-9]+' \
          | sed 's/GLIBC_//' | sort -V | tail -n1); \
    echo "max glibc symbol required: ${max:-none}; runtime provides: ${RUNTIME_GLIBC_MAX}"; \
    if [[ -n "${max}" && "$(printf '%s\n%s\n' "${RUNTIME_GLIBC_MAX}" "${max}" | sort -V | tail -n1)" != "${RUNTIME_GLIBC_MAX}" ]]; then \
      echo "ERROR: binary requires GLIBC_${max} but runtime provides only GLIBC_${RUNTIME_GLIBC_MAX}." >&2; \
      echo "Build and runtime base images are out of sync." >&2; \
      exit 1; \
    fi

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:b0ae8e989418b458e0f25489bc3be523718938a2b70864cc0f6a00af1ddbd985

COPY --from=build /tini /usr/bin/tini
COPY --from=build /src/build/container/apps/abyss-server/abyss-server /usr/local/bin/abyss-server

EXPOSE 6379 9090 8080

# tini (PID 1) forwards signals to abyss-server and reaps zombies; the exec-form
# wrapping preserves abyss-server's own arg handling. Together with the
# signal-first bootstrap in main.cpp, a SIGTERM at any instant is honored (G7).
ENTRYPOINT ["/usr/bin/tini", "--", "abyss-server"]
