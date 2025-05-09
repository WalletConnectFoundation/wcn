################################################################################
#
# Build args
#
################################################################################
ARG                 BASE="rust:1.79-buster"
ARG                 RUNTIME="debian:buster-slim"
ARG                 VERSION="unknown"
ARG                 SHA="unknown"
ARG                 MAINTAINER="WalletConnect"
ARG                 PROFILE="release"
ARG                 LOG_LEVEL="debug"
ARG                 WORK_DIR="/app"

################################################################################
#
# Parameters for release builds
#
################################################################################
FROM                ${BASE} as build-release
ENV                 BUILD_SHARED_ARGS="--profile release-debug"
ENV                 BUILD_PROFILE_DIR="release-debug"

################################################################################
#
# Parameters for debug builds
#
################################################################################
FROM                ${BASE} as build-debug
ENV                 BUILD_SHARED_ARGS=""
ENV                 BUILD_PROFILE_DIR="debug"

################################################################################
#
# Build the binary
#
################################################################################
FROM                build-${PROFILE} AS build

ARG                 LOG_LEVEL
ARG                 WORK_DIR

RUN                 apt-get update && apt-get install -y --no-install-recommends clang

WORKDIR             ${WORK_DIR}

# Build the local binary
COPY                . .
RUN                 cargo build ${BUILD_SHARED_ARGS}

# Put the artifacts to a known path so we don't have to pass an extra arg to the runtime image
RUN                 ln -s ${WORK_DIR}/target/${BUILD_PROFILE_DIR} ${WORK_DIR}/target/out

################################################################################
#
# Runtime image
#
################################################################################
FROM                ${RUNTIME} AS runtime

ARG                 VERSION
ARG                 SHA
ARG                 MAINTAINER
ARG                 WORK_DIR
ARG                 LOG_LEVEL

LABEL               version=${VERSION}
LABEL               sha=${SHA}
LABEL               maintainer=${MAINTAINER}

RUN                 apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates libssl-dev procps linux-perf \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR             ${WORK_DIR}

COPY --from=build   ${WORK_DIR}/target/out/wcn_node /usr/local/bin/wcn_node

# Preset the `LOG_LEVEL` env var based on the global log level.
ENV                 LOG_LEVEL="info,wcn_node=${LOG_LEVEL}"

RUN                 mkdir /wcn && chown 1001:1001 /wcn

USER                1001:1001
ENTRYPOINT          ["/usr/local/bin/wcn_node"]
