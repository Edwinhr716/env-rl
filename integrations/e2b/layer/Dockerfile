# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A runtime layer with envd, the daemon the E2B SDK talks to inside a sandbox
# (github.com/e2b-dev/runtime, packages/envd, Apache-2.0). Mounted read-only
# into any task image
# (ate-env manifest template --layer envd=<this image@sha256:...>=/opt/e2b),
# it appears as /opt/e2b/envd, next to its licence. The binary is static, thus
# it runs on glibc and musl images alike.
#
# The binary is upstream's release build, from the public bucket that
# packages/envd/Makefile names (E2B_ARTIFACT_BINARIES_URL): objects
# v<version>/envd (amd64) and v<version>/arm64/envd, each with an envd.sha256
# next to it. The checksums below are those files' contents. Upstream tags no
# git release for this version (the last envd tag is envd-v0.6.13), and its
# Makefile lets a rebuild replace the object of a version: a changed object
# fails the build at the checksum.
#
# Another envd version: change ENVD_VERSION and the two checksums, and
# ENVD_LICENSE_COMMIT to the commit that set the version in
# packages/envd/pkg/version.go.
#
# Build and push:  docker buildx build --platform <platform of the worker nodes> --push -t <repo>:0.10.2 layer
# Pin by digest:   docker buildx imagetools inspect --format '{{.Manifest.Digest}}' <repo>:0.10.2

ARG ENVD_VERSION=0.10.2

# One stage per architecture, because each has its own checksum; the last
# stage picks the one of the target. No stage runs a command: the image holds
# only the binary and its licence, as a mount, not a container that runs.
FROM scratch AS envd-amd64
ARG ENVD_VERSION
ADD --checksum=sha256:2bf41e6c44df89f5bea59c6d7eaf47aacc0e59b9e6957f95d3a8c6b8c9f1e7a2 --chmod=0755 \
    https://storage.googleapis.com/e2b-artifact-binaries/envd/v${ENVD_VERSION}/envd /envd

FROM scratch AS envd-arm64
ARG ENVD_VERSION
ADD --checksum=sha256:c4bc9e463b7a89d28ae79e1740f7af8d2802ae06c164b25d3ad49e68e9c2a8d6 --chmod=0755 \
    https://storage.googleapis.com/e2b-artifact-binaries/envd/v${ENVD_VERSION}/arm64/envd /envd

FROM envd-${TARGETARCH}
# The licence of the repository at the commit that released the version.
ARG ENVD_LICENSE_COMMIT=acfbd6e93f371e047080411f72bed452365ceb00
ADD --checksum=sha256:b4ef1bf811cb4095229fb86b574199e467c78f0ac4078cf8be62189e1fbd0818 --chmod=0644 \
    https://raw.githubusercontent.com/e2b-dev/runtime/${ENVD_LICENSE_COMMIT}/LICENSE /LICENSE
