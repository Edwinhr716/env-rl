#!/usr/bin/env bash

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

# Prints the ActorTemplate of the E2B base template: a rootfs image, the guest,
# and the envd runtime layer (../layer), with envd as a sidecar that the
# guest's /readyz waits for, and the five capabilities envd needs. Pipe the
# output to `kubectl-ate create actor-template -f -`. See ../README.md.
#
#   base-template.sh <envd-layer-image@sha256:...> --rootfs-image <image@sha256:...> [ate-env manifest template flags]
#
# The flags go to `ate-env manifest template` after this script's own: at
# --rootfs-image (required) and --guest-image, both digest pinned. --template
# renames the template (default e2b-base); a repeatable flag, such as --layer,
# --env or --add-capability, adds to this script's.
#
# The rootfs image must run as root: its USER is root or not set. Substrate
# gives a non-root USER no effective capability, and envd then starts no
# process.
#
# ATE_ENV names the ate-env binary (default: ate-env on PATH).
set -o errexit -o nounset -o pipefail

usage() {
  echo "usage: $0 <envd-layer-image@sha256:...> --rootfs-image <image@sha256:...> [ate-env manifest template flags]" >&2
  exit 2
}

# A flag in the place of the layer image would go to ate-env as an image.
LAYER_IMAGE="${1:-}"
case "${LAYER_IMAGE}" in
  "" | -*) usage ;;
esac
shift

# Without a rootfs image the template would be the guest image alone, with
# nothing for envd to run commands in.
rootfs=""
for arg in ${@+"$@"}; do
  case "${arg}" in
    --rootfs-image | --rootfs-image=?*) rootfs="${arg}" ;;
  esac
done
if [[ -z "${rootfs}" ]]; then
  echo "$0: --rootfs-image is required" >&2
  usage
fi

ENVD_PORT=49983

# The capabilities of the caller's own --add-capability flags. The script
# leaves out each of its five that the caller names too, thus a repeated name
# reaches ate-env once.
given=" "
previous=""
for arg in ${@+"$@"}; do
  case "${arg}" in
    --add-capability=*) given+="${arg#*=} " ;;
  esac
  if [[ "${previous}" == "--add-capability" ]]; then
    given+="${arg} "
  fi
  previous="${arg}"
done

capabilities=()
for name in SETUID SETGID CHOWN DAC_OVERRIDE FOWNER; do
  if [[ "${given}" != *" ${name} "* ]]; then
    capabilities+=(--add-capability "${name}")
  fi
done

"${ATE_ENV:-ate-env}" manifest template \
  --template e2b-base \
  --layer "envd=${LAYER_IMAGE}=/opt/e2b" \
  --sidecar "/opt/e2b/envd -isnotfc -no-cgroups -port ${ENVD_PORT}" \
  --sidecar-readyz "http://127.0.0.1:${ENVD_PORT}/health" \
  ${capabilities[@]+"${capabilities[@]}"} \
  ${@+"$@"}
