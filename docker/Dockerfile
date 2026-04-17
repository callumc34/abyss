# syntax=docker/dockerfile:1

FROM gcc:14 AS build

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      cmake ninja-build git curl zip unzip tar pkg-config && \
    rm -rf /var/lib/apt/lists/*

ENV VCPKG_ROOT=/opt/vcpkg

WORKDIR /src

COPY vcpkg.json ./
RUN BASELINE=$(python3 -c "import json; print(json.load(open('vcpkg.json'))['builtin-baseline'])") && \
    git init ${VCPKG_ROOT} && \
    cd ${VCPKG_ROOT} && \
    git remote add origin https://github.com/microsoft/vcpkg.git && \
    git fetch --depth 1 origin "${BASELINE}" && \
    git checkout FETCH_HEAD && \
    ./bootstrap-vcpkg.sh -disableMetrics

COPY CMakeLists.txt CMakePresets.json ./
COPY cmake/ cmake/
COPY include/ include/
COPY src/ src/
COPY apps/ apps/

RUN --mount=type=cache,target=/root/.cache/vcpkg/archives \
    cmake --preset container -G Ninja && \
    cmake --build build/container --target abyss-server

RUN ldd build/container/apps/abyss-server/abyss-server || true
RUN timeout 5 build/container/apps/abyss-server/abyss-server || [ $? -eq 124 ]

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /src/build/container/apps/abyss-server/abyss-server /usr/local/bin/abyss-server

EXPOSE 6379 9090 8080

ENTRYPOINT ["abyss-server"]
