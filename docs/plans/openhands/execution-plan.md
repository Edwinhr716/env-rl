# Execution plan: OpenHands on sandboxenv-rl

Status: describes `main` at `40d7ef88` (2026-11-12). Written 2026-10-26 as a proposal; workstreams
W0 to W10 are done, and each plan and follow-up pull request is merged. W11 (OH-12, OH-13) and W12
(OH-14) are open; see [State on `main`](#state-on-main) and [Open work](#open-work).

Goal: each in-scope criterion in [acceptance-criteria.md](./acceptance-criteria.md) is met with
evidence, the reviewer has no open blocking finding, and the pull requests are approved and merged.
Design and scope: [README.md](./README.md). Failures found on the way: [findings.md](./findings.md).
Decisions: [decisions.md](./decisions.md).

## Roles

| Role         | Does                                                                                                                  | Never                                                  |
| ------------ | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Orchestrator | Owns the plan and the decisions; starts the other roles; assigns findings; reports to the owner; escalates            | Approves or merges a pull request                      |
| Infra        | Prepares the test cluster (D-1) inside the blast radius; deploys branch images; registers templates; pushes the layer | Touches anything outside the blast radius              |
| Developer    | Changes code on a branch, runs the hermetic suites, opens or updates the pull request, answers review                 | Pushes to `main`; force-pushes another author's branch |
| Reviewer     | Reviews each pull request against the criteria and the design                                                         | Changes code                                           |
| Tester       | Runs the criteria on the test cluster from branch images; records evidence; files failures as findings                | Changes code or the cluster setup                      |

Blast radius: one test cluster; the namespaces `ate-system`, `ate-env` and `oh-*`; one test registry
path. Never node pools, IAM, buckets or other namespaces without the owner's word.

## Workstreams

| WS  | Content                                                                                                                    | Depends on   | Criteria     | State                                   |
| --- | -------------------------------------------------------------------------------------------------------------------------- | ------------ | ------------ | --------------------------------------- |
| W0  | Bring #14 up to `main` (Substrate v0.6.0, router mTLS, the client `Observer`, tracing); keep CI green                      | —            | OH-1         | done                                    |
| W1  | Test cluster: Substrate v0.6.0, ate-env with `-auth-token-file`, the router allow-list entry, the layer, the base template | D-1, D-3     | prerequisite | done                                    |
| W2  | Upstream example 17 live, pause and resume                                                                                 | W0, W1       | OH-2         | done                                    |
| W3  | agent-sandbox-rl rollout live, imports changed only                                                                        | W0, W1       | OH-3         | done                                    |
| W4  | R2E-Gym episode live: rewards for the gold patch and the empty patch, clean grading                                        | W0, W1       | OH-4         | done                                    |
| W5  | Publish the layer (multi-arch, signed) from `release.yaml`                                                                 | D-3          | OH-5         | shipped (#128), removed by D-5 (#295)   |
| W6  | A credential for the event WebSocket that the proxy checks and strips; `AteEnvWorkspace` sends it                          | W0           | OH-6         | done (#129, #130)                       |
| W7  | Performance next to agent-sandbox: warm acquire, cold create, contended creates                                            | W1, W3       | OH-7         | done                                    |
| W8  | Docs and threat model for what merged                                                                                      | W2 to W6     | OH-8         | done; drift of 2026-11-09 fixed by #345 |
| W9  | Merge the plan pull requests                                                                                               | all above    | OH-9         | done                                    |
| W10 | Tunix on a fork branch; upstream OpenHands as a draft                                                                      | D-4          | OH-10, OH-11 | stretch, not started                    |
| W11 | Live regression of merged `main`, then the same on Substrate `274605f1`                                                    | test cluster | OH-12, OH-13 | open                                    |
| W12 | Session key not readable through `/proc` (`PR_SET_DUMPABLE` or a separate uid)                                             | owner's call | OH-14        | follow-up                               |

## The loop

1. The tester runs each criterion that is not yet met and records the result (pass, or fail with the
   transcript).
2. The orchestrator turns each failure into a finding with an owner and records it in
   [findings.md](./findings.md).
3. The developer fixes it on a branch, runs the hermetic suites and pushes. Infra redeploys the
   branch images when the fix touches `ate-env-api` or the guest.
4. The reviewer reviews the change; blocking findings go back to step 3.
5. The tester runs the failed criterion again. Repeat until all in-scope criteria pass.

Escalate to the owner when a pull request needs an approval or a merge (the `main` ruleset needs one
approval from someone other than the author), when a fix needs a change outside the blast radius, a
credential or an external repository, when one criterion fails three times in a row for one cause,
or when a criterion turns out to be wrong (record the change in [decisions.md](./decisions.md)).

The live runs combined the open branch heads into "combined states" (for example `f06f1dc`,
`bcabb35`, `00e697e`) and deployed images built from them, so each criterion ran before the merges.

## State on `main`

What changed on `main` since the plan was written, and the effect on the plan:

| Change on `main`                                                                                                                                      | Effect on this plan                                                                                                                                                                                                                                                                                  |
| ----------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| All 15 plan pull requests merged (#14, #128 to #131, #134 to #143), then #265 (allow-list docs), #267 (layer re-lock), #268 (`--sidecar-env`), #295   | OH-1 to OH-4, OH-6, OH-7 and OH-9 stay met; F-AC8-1 is fixed except the `/proc` read (OH-14)                                                                                                                                                                                                         |
| D-5: no published OpenHands image (#295)                                                                                                              | OH-5 is out of scope; users build the layer from `integrations/openhands/layer` into their own registry                                                                                                                                                                                              |
| #232: the Python client is `ate-env-rl` (`ate_env_rl`)                                                                                                | The integration imports `ate_env_rl`; OH-3's rule (only the upstream file's imports change) holds, because that file imports `ate_env_openhands.agent_sandbox_rl`                                                                                                                                    |
| #314: the R2E-Gym adapter is `ate_env_rl.adapters.r2egym`                                                                                             | `ate_env_openhands.r2egym` re-exports it, thus OH-4's program runs unchanged; new code imports from `ate_env_rl.adapters.r2egym`                                                                                                                                                                     |
| #254: Substrate `274605f1`; `--snapshots-bucket` optional (supersedes #266)                                                                           | Templates name no snapshot location; the test cluster still runs `475f49b0`, thus OH-13 covers `274605f1`'s runtime changes                                                                                                                                                                          |
| Repo-owned criteria: ate-env-api AE-1 to AE-5 and WF-1 to WF-8 (#222, #275); port proxy PP-1 and PP-2 (#245, #277, #316)                              | They own the properties behind F-AC7-1, F-AC7-2, F-AC7-4 and F-C4B-1 for guest calls and the proxy path; this plan keeps the OpenHands-visible checks (OH-12)                                                                                                                                        |
| #209 `GetImageStatus` and `ate-env image-status`; #235 `WarmImage` and `ListPlacementDomains`                                                         | Optional for RL fan-out on new images (the F-AC7-1 burst): warm the images before a burst; not a criterion                                                                                                                                                                                           |
| v0.1.0 (2026-11-07) published a private layer image                                                                                                   | Predates #267; no updates (D-5); an org admin can delete the package                                                                                                                                                                                                                                 |
| #345 (`40d7ef88`): `integrations/openhands/README.md` and `docs/security/threat-model.md` match `main` after v0.1.0 and the Substrate `274605f1` bump | OH-8 is met again: the R2E-Gym live episode, the `delete_on_exit` rules of a failed constructor (a failed create with `delete_on_exit=False`, a failure after an attach with `delete_on_exit=True`), the v0.1.0 release and the image `USER` change are stated as the code has them. Was open work 8 |
| #346 (`7a73ded7`): `SandboxFleet.run` in `ate_env_openhands.agent_sandbox_rl` tears down once, after the passes, also when a pass fails               | A failed `preflight()` on the last pass no longer skips `teardown()` (`run_sliding` and `run_pipelined` call `preflight()` before their own teardown scope); `test_a_failed_preflight_on_the_last_pass_tears_down` checks it. Was open work 9                                                        |

## Open work

| #   | Item                                                                                                                                                                                                                                                                                                                     | Owner                        | Criterion    |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------- | ------------ |
| 1   | Live regression of merged `main` on the test cluster. Check 2's delete has two blockers to record: Substrate refuses `DeleteActor` on a `CRASHED` actor (`FailedPrecondition` on `DeleteVolumes`), and the port proxy's 2 s state cache (`stateCacheTTL`) can answer 410 `CRASHED` to a request within 2 s of the delete | owner runs it                | OH-12        |
| 2   | Move the test cluster to Substrate `274605f1` and rerun OH-2, OH-3, OH-4 and OH-12 (tmpfs `/tmp` and `/dev/shm`, Substrate #3533; `capabilities.add` restriction and nodev mounts, Substrate #3580)                                                                                                                      | owner and infra              | OH-13        |
| 3   | Session key not readable by same-user code (`PR_SET_DUMPABLE=0` in the guest, or a separate uid for the agent-server)                                                                                                                                                                                                    | developer, on the owner's go | OH-14        |
| 4   | Layer Dependabot alerts stay open on GitHub although `main` has the fixed versions (F-L1): check the dependency graph for a parse error on `layer/requirements.txt`; if GitHub cannot read it, dismiss the 62 alerts as fixed by #267                                                                                    | repository admin             | —            |
| 5   | filelock alerts (13): wait for a stable litellm that allows filelock 4, then re-lock                                                                                                                                                                                                                                     | developer                    | —            |
| 6   | Release: the v0.1.0 run's first `ko build` of each image failed with `writing sbom: DENIED`; only `ko-publish.sh`'s retry saved it (F-R1)                                                                                                                                                                                | release owner                | —            |
| 7   | Stretch: OH-10 (Tunix) and OH-11 (upstream OpenHands; with D-5 it documents building the layer, no image)                                                                                                                                                                                                                | after 1 to 3                 | OH-10, OH-11 |

## Risks

| Risk                                                       | Effect                                                      | Mitigation                                                                        |
| ---------------------------------------------------------- | ----------------------------------------------------------- | --------------------------------------------------------------------------------- |
| The router allow-list misses ate-env-api's SPIFFE ID       | Guest calls look like a sleeping environment                | Check the allow-list first (#265); the startup log names the rejected certificate |
| The layer needs glibc 2.31 or newer                        | Old task images never get ready                             | Pick task images on Debian 11, Ubuntu 20.04 or later                              |
| R2E-Gym installs from its checkout only                    | OH-4 needs a local install                                  | `pip install -e path/to/R2E-Gym` in the tester's venv                             |
| Some R2E-Gym instances install packages before the tests   | Sandboxes have no egress; those instances fail to grade     | Use instances that grade offline (F-AC4-3)                                        |
| Substrate `274605f1` runtime changes (tmpfs, capabilities) | The agent-server or R2E-Gym can break on the pinned version | OH-13                                                                             |
