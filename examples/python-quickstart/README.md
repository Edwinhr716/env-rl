# Python client quickstart

[`quickstart.py`](./quickstart.py) uses the Python client (`ate_env_rl.Client`) end to end against
a running `ate-env-api`: create an environment, run a shell command, write and read a file, follow
a process's output, and delete the environment. The client's guide is
[`clients/python/README.md`](../../clients/python/README.md).

## Run

```sh
pip install -e clients/python
kubectl port-forward -n ate-env svc/ate-env-api 7777:7777
python examples/python-quickstart/quickstart.py
```

The script sends no bearer token, and `ate-env manifest` deploys `ate-env-api` with a required token
by default. Deploy a test instance with `ate-env manifest --insecure-no-auth`, or pass
`auth_token=` to `Client` in the script; against the default deployment each call fails with
`Unauthenticated`.

The example is provided as is ([docs/support.md](../../docs/support.md)).
