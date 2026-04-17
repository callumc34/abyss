# syntax=docker/dockerfile:1

FROM gcc:14 AS build

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      cmake ninja-build git curl zip unzip tar pkg-config && \
    rm -rf /var/lib/apt/lists/*

ENV VCPKG_ROOT=/opt/vcpkg
# Must match builtin-baseline in vcpkg.json
ARG VCPKG_COMMIT=d13fa75214c258099923cf25a5e6311e58c07f3b
RUN git init ${VCPKG_ROOT} && \
    cd ${VCPKG_ROOT} && \
    git remote add origin https://github.com/microsoft/vcpkg.git && \
    git fetch --depth 1 origin ${VCPKG_COMMIT} && \
    git checkout FETCH_HEAD && \
    ./bootstrap-vcpkg.sh -disableMetrics

WORKDIR /src

COPY vcpkg.json CMakeLists.txt CMakePresets.json ./
COPY cmake/ cmake/
COPY include/ include/
COPY src/ src/
COPY apps/ apps/

RUN cmake --preset container -G Ninja && \
    cmake --build build/container --target abyss-server

RUN ldd build/container/apps/abyss-server/abyss-server || true
RUN build/container/apps/abyss-server/abyss-server

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /src/build/container/apps/abyss-server/abyss-server /usr/local/bin/abyss-server

EXPOSE 6379 9090 8080

ENTRYPOINT ["abyss-server"]
