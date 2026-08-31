# Deployment IAM contract

All names below are placeholders or fixed public component names. Apply the
bindings only through the human-owned IAM and WIF phases after reviewing
`deploy.sh --plan`.

## Role matrix

| Identity | Scope | Allowed role or permission |
|---|---|---|
| `pmax-runtime` | Project | `bigquery.jobUser`, `bigquery.readSessionUser` |
| `pmax-runtime` | raw, marts, ops, verify datasets only | `bigquery.dataEditor` |
| `pmax-runtime` | one shared MCC credential secret | `secretmanager.secretAccessor` |
| `pmax-runtime` | report bucket | `storage.objectUser` |
| `pmax-runtime` | config bucket | `storage.objectViewer` |
| `pmax-invoker` | `pmax-pack-daily` only | `run.invoker` |
| Trusted workflow WIF principal set | Project | `bigquery.jobUser` |
| Trusted workflow WIF principal set | CI scratch pair only | `bigquery.dataEditor` |
| `pmax-build` | Project | `artifactregistry.writer`, `logging.logWriter` |
| Deployer | runtime SA | `iam.serviceAccountUser` |
| Deployer | one secret | `secretmanager.secretVersionAdder` |
| Deployer | Project custom role | `bigquery.tables.deleteSnapshot` only |
| Named operator | runtime SA | `iam.serviceAccountTokenCreator` |
| Named operator | one secret | `secretmanager.secretAccessor` |

Runtime has no writer role on parity scratch, CI scratch, or snapshots. The
trusted workflow principal set has no credential, bucket, raw, mart, ops,
parity, snapshot, verification, or Secret Manager role. It does not
impersonate a service account. Deployer never receives token creator on
runtime.

## Credential model

The secret contains the shared MCC token from the operator's existing
credential file and own Google login. The credential uses the existing top
manager as `login_customer_id`; the runtime extracts only the accounts named in
the deployed config. Neither the deployer nor the runtime mints a user, token,
manager account, or credential file.

The pack is recorded in the shared-token consumer inventory by its pinned
Secret Manager version. Rotation probes the operator's replacement file, adds
a version, updates the Job to the new numeric version, verifies the pack and
the remaining consumers, and revokes the previous token last. A compromise
revokes first and redistributes to every consumer, including the pack. This
accepted design couples pack rotation to the shared token and gives a leaked
pack secret the blast radius of the whole MCC.

## WIF boundary

The provider maps `sub`, `repository_id`, `repository_owner_id`, `ref`, and
`workflow_ref`. Its attribute condition requires all of these facts:

- repository id equals the recorded numeric id for `Ninety2UA/pmax-pack`;
- owner id equals the recorded numeric organization id;
- ref is exactly `refs/heads/main`;
- workflow ref is exactly
  `Ninety2UA/pmax-pack/.github/workflows/trusted.yml@refs/heads/main`.

The environment `trusted-parity` requires a reviewer. Forks and pull requests
receive no cloud identity. Outside-collaborator Actions require approval.
Third-party actions use full commit SHAs. Never use `pull_request_target`.
The auth action exchanges GitHub OIDC directly for this federated principal.
There is no `service_account` input and no `roles/iam.workloadIdentityUser`
bridge.

## Mandatory negative probes

Run these in U8 and retain the denied result:

1. Deployer cannot mint a runtime access token or access the credential secret.
2. The trusted workflow principal cannot access the secret, report bucket,
   config bucket, parity scratch, raw, marts, ops, snapshots, or verification
   dataset.
3. Runtime cannot write parity scratch, CI scratch, or snapshots.
4. Invoker cannot invoke another Job or read any data.
5. A token from another repository, owner, ref, or workflow cannot federate.
6. An over-cap query by any identity (the cap is per user, project-wide; Google does not support a per-principal override on this metric) is rejected and the quota alert fires.

## Audit and alert contract

Data Access audit logs are enabled for BigQuery and Secret Manager. The log
metric `pmax_unexpected_secret_access` matches `AccessSecretVersion` by every
principal except `pmax-runtime` and the named operator. Bind that metric to an
enabled alert policy routed to the operator notification channel before the
first live run. The failed-job policy in `alert-policy.json` uses
`run.googleapis.com/job/completed_execution_count` with `result=failed`; it
does not use log severity. Prove it once with Scheduler paused and prove an
AE13 SKIPPED execution stays silent.

Set the BigQuery daily query quota from the measured fixture-suite allowance,
not a guess, then prove rejection immediately above the cap. The quota alert
and rejection proof are required even when the current fixture suite remains
inside the free tier.

## Review record

Record the effective policies, custom-role definition, WIF provider condition,
quota override, alert policy names, negative-probe evidence, operator identity,
and UTC review time under the excluded deployment record. Never record tokens,
secret payloads, client names, or account ids.
