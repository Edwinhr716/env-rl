# env-lifecycle

A Go program that walks through the `EnvironmentService` lifecycle with the generated gRPC stubs
alone: create an environment, get it, suspend it, delete it. It shows the wire API without the Go
client; [`examples/mcp`](../mcp/README.md) shows a program that constructs a client.

Needs a port-forward to `ate-env-api`:

```bash
kubectl port-forward -n ate-env svc/ate-env-api 7777:7777
go run ./examples/env-lifecycle -addr localhost:7777
```

The program sends no bearer token, and `ate-env manifest` deploys `ate-env-api` with a required
token by default. Deploy a test instance with `ate-env manifest --insecure-no-auth`; against the
default deployment each call fails with `Unauthenticated`.

Run `go run ./examples/env-lifecycle -h` for the flags (address, atespace, template).
