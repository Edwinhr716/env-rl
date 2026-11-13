# E2B runtime layer

[envd](https://github.com/e2b-dev/runtime/tree/main/packages/envd) is the daemon the
[E2B](https://github.com/e2b-dev/E2B) SDK talks to inside a sandbox. This directory runs it in an
ate-env environment as a [runtime layer](../../docs/rootfs-images/RUNTIMES.md): the task image is
not changed.

| Path                                                 | What it is                                                                        |
| ---------------------------------------------------- | --------------------------------------------------------------------------------- |
| [`layer/Dockerfile`](./layer/Dockerfile)             | An image with upstream's static `envd` 0.10.2 release binary and its licence.   |
| [`hack/base-template.sh`](./hack/base-template.sh)   | Prints the base template: a rootfs image, the guest, the layer and the sidecar. |
| [`tests/test_envd_e2e.py`](./tests/test_envd_e2e.py) | Live checks of the layer in an environment, through the port proxy.             |

## Register the base template

No release publishes the layer image: build it and push it to your own registry. The build
downloads upstream's release binary for the platform and checks its sha256. Then register the
template with digest-pinned images:

```bash
docker buildx build --platform <platform of the worker nodes> --push -t <registry>/ate-env-e2b-envd:0.10.2 integrations/e2b/layer
LAYER=<registry>/ate-env-e2b-envd@$(docker buildx imagetools inspect --format '{{.Manifest.Digest}}' <registry>/ate-env-e2b-envd:0.10.2)

integrations/e2b/hack/base-template.sh "$LAYER" \
  --rootfs-image <image>@sha256:<digest> \
  --guest-image  <registry>/ate-env-guest@sha256:<digest> |
  kubectl-ate create actor-template -f -
```

The template is `e2b-base`. The guest starts `/opt/e2b/envd -isnotfc -no-cgroups -port 49983` as a
sidecar and reports ready only after envd's `/health` answers. An environment created with
`rootfs_image` on this base keeps the layer, the sidecar and the capabilities.

## Capabilities

An actor's container has `AUDIT_WRITE`, `KILL` and `NET_BIND_SERVICE` by default. The script adds
five more with `--add-capability`:

| Capability               | Why envd needs it                                                                                                                                     |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SETUID`, `SETGID`       | envd sets the user, the group and the groups of each process it starts, also for root. Without them each start fails with `operation not permitted`. |
| `CHOWN`                  | envd gives a file it writes to the user of the call.                                                                                                  |
| `DAC_OVERRIDE`, `FOWNER` | Root in the environment reads and re-modes the files of the other users, as root does in an E2B sandbox.                                              |

The guest and each command in the environment hold them too.

## Reach envd

envd listens on port 49983 inside the environment. A caller reaches it through the
[port proxy](../../docs/rootfs-images/RUNTIMES.md#who-can-reach-the-second-runtime) of `ate-env-api`,
with the API's caller token when the API has one.

## What the image needs

- It runs as root: its `USER` is root or not set. Substrate runs the container as the image's
  `USER`, and gives a non-root user no effective capability, thus `--add-capability` does not help
  it. In such an image envd answers `/health`, and the environment is created, but each process start
  fails with `fork/exec /bin/sh: operation not permitted`.
- `/bin/sh`: envd starts each process through it. envd itself is static and starts in a musl image
  (Alpine, for example).

## Tests

`tests/test_envd_e2e.py` checks the layer on a live cluster, and the Kind E2E job runs it: envd
serves in a glibc image and, through a derived template, in a musl image; envd and a process it
started survive a suspend; a file written for another user belongs to that user, which fails on a
template without `CHOWN`, `DAC_OVERRIDE` or `FOWNER`; a request without the API token gets `401`
from the port proxy. One case records what envd does in an image with a non-root `USER`.

## Limits

- An image with a non-root `USER` is not supported (see [What the image needs](#what-the-image-needs)).
- The image holds envd's own licence. It does not hold the notices of the Go modules that the
  static binary contains. Add them to the image you build when your use needs them.
- The pins are the sha256 of upstream's binary for each architecture. Upstream tags no git release
  for this envd version, and its build can replace the bucket object of a version; a change fails
  the build at the checksum.
