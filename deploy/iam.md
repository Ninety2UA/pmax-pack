# Deployment IAM contract

<img src="../docs/diagrams/section-iam.png" width="72" height="72" alt="A keyhole granting access to one data layer">

All names below are placeholders or fixed public component names. Apply the
bindings only through the human-run IAM and Workload Identity Federation
(WIF) phases after reviewing
`deploy.sh --plan`.

Use the [Looker Studio guide](../docs/looker.md) to connect reporting sources,
the [operations guide](../docs/operations.md#credential-flip-detector) to check
their active reader identity, and the
[migration guide](../docs/migrations/v2.1.0.md) for the order in which grants,
publication and probes become valid. A role readback and a successful chart
refresh prove different things, so keep both before hand-over.

## Bootstrap inputs

Replace every placeholder below with the reviewed deployment value before
running the ladder preview or a live pass. Keep member identities private.
These inputs are consumed by phases 30, 40 and 45:

```bash
export PMAX_DEPLOYER_MEMBER='user:deployer@example.invalid'
export PMAX_OPERATOR_MEMBER='user:operator@example.invalid'
export PMAX_GITHUB_REPOSITORY_ID='<github-repository-numeric-id>'
export PMAX_GITHUB_OWNER_ID='<github-owner-numeric-id>'
export PMAX_OAUTH_PUBLISHING_STATUS='<observed-publishing-status>'
```

| Input | Meaning |
|---|---|
| `PMAX_DEPLOYER_MEMBER` | IAM member for deployment permissions in phase 40, including runtime `actAs` and secret version addition. |
| `PMAX_OPERATOR_MEMBER` | Named operator IAM member for runtime token minting, secret access and the time-bound Looker probe grant in phase 40. |
| `PMAX_GITHUB_REPOSITORY_ID` | Numeric GitHub repository ID used by phase 45's WIF repository condition and principal-set binding. |
| `PMAX_GITHUB_OWNER_ID` | Numeric GitHub repository-owner ID used by phase 45's WIF owner condition; this is not the Google Cloud organization ID. |
| `PMAX_OAUTH_PUBLISHING_STATUS` | Observed publishing status of the existing OAuth client, such as `production`; phase 30 requires a value and refuses `Testing`. |

Phase 40 falls back to the CLI operator identity when either member input is
unset; set both explicitly to make the reviewed role split clear. Phase 45
still requires the two GitHub IDs for its retained WIF resources even though
public CI has no bound workflow. The measured daily query allowance is
documented under [query quota](#measure-and-prove-the-query-quota).
`PMAX_NOTIFICATION_CHANNEL`, required by phases 40, 45 and 90 and by phase 00
on the signed pass, is the alert channel's resource name,
`projects/<project>/notificationChannels/<id>`; its readback is under
[audit alerts](#audit-and-alert-contract). The Looker probe expiry is
under [reader credentials](#looker-service-account-credentials-and-probe-window).

## Role matrix

| Identity | Scope | Allowed role or permission |
|---|---|---|
| `pmax-runtime` | Project | `bigquery.jobUser`, `bigquery.readSessionUser` |
| `pmax-runtime` | raw, marts, ops, marts verify, reporting, reporting verify datasets only | `bigquery.dataEditor` |
| `pmax-runtime` | one shared MCC credential secret | `secretmanager.secretAccessor` |
| `pmax-runtime` | report bucket | `storage.objectUser` |
| `pmax-runtime` | config bucket | `storage.objectViewer` |
| `pmax-looker` | reporting dataset only | `bigquery.dataViewer` (READER access entry) |
| `pmax-looker` | Project | `bigquery.jobUser` (job creation, no dataset access) |
| Each configured Looker Studio service agent | Looker SA only | `iam.serviceAccountTokenCreator` |
| Each configured report editor | Looker SA only | `iam.serviceAccountUser` (`actAs`) |
| Named operator during the probe window | Looker SA only | `iam.serviceAccountTokenCreator` with an expiring condition |
| `pmax-invoker` | `pmax-pack-daily` only | `run.invoker` |
| WIF principal set (retained, no workflow bound since v2.0.1) | Project | `bigquery.jobUser` |
| WIF principal set (retained, no workflow bound since v2.0.1) | CI scratch pair only | `bigquery.dataEditor` |
| `pmax-build` | Project | `artifactregistry.writer`, `logging.logWriter` |
| `pmax-build` | Cloud Build staging bucket `<project>_cloudbuild` only | `storage.objectViewer` |
| Deployer | runtime SA | `iam.serviceAccountUser` |
| Deployer | one secret | `secretmanager.secretVersionAdder` |
| Deployer | Project custom role | `bigquery.tables.deleteSnapshot` only |
| Deployer (phase 50, default Cloud Build route) | Project | `cloudbuild.builds.create` (Owner or Editor already has it) |
| Deployer (phase 50, default Cloud Build route) | `pmax-build` SA | `iam.serviceAccounts.actAs` (Owner or Editor already has it) |
| Named operator | runtime SA | `iam.serviceAccountTokenCreator` |
| Named operator | one secret | `secretmanager.secretAccessor` |

Runtime has no writer role on parity scratch, CI scratch, or snapshots. The
WIF principal set has no credential, bucket, raw, mart, ops, parity,
snapshot, verification, or Secret Manager role, does not impersonate a
service account, and since v2.0.1 has no GitHub workflow bound to it (public
CI is fixture-only and never federates). Deployer never receives token
creator on runtime.

## Credential model

The secret contains the shared manager-account (MCC) token from the operator's
existing credential file and own Google login. The credential uses the existing top
manager as `login_customer_id`; the runtime extracts only the accounts named in
the deployed config. Neither the deployer nor the runtime mints a user, token,
manager account, or credential file.

The pack is recorded in the shared-token consumer inventory by its pinned
Secret Manager version. The operator keeps that inventory privately. It lists
every consumer of the shared token with its storage location, last verified
UTC date, one-row probe result, and natural scheduled run result, so a
rotation can reach every copy. Rotation probes the operator's replacement file, adds
a version, updates the Job to the new numeric version, verifies the pack and
the remaining consumers, and retires the previous copies without revoking,
because revocation invalidates every token the same login granted to the
project, including the replacement. The
[operations guide](../docs/operations.md#identities-credentials-and-data-protection)
names the ladder pass that performs the rotation and the revocation rule. A compromise
revokes first, grants again, and redistributes to every consumer, including the pack. This
accepted design couples pack rotation to the shared token and gives a leaked
pack secret the blast radius of the whole MCC.

## Looker service-account credentials and probe window

Phase 00 requires a parent organization, including projects below folders. That
requirement stands in for managed identities at deployment time; it does not
prove an editor's membership. Each editor must be a Google Workspace or Cloud
Identity user in an organization whose Looker Studio service agent is listed
in `looker_service_agents`. Copy each service-agent principal from Google's setup
page as a user of that organization; do not derive or guess it. Put editors in
`editors` as `user:` principals and agents as `serviceAccount:` principals.
Both lists must be nonempty before the human-run phase 40 can apply IAM.

The source uses `pmax-looker` credentials. There is no personal-credentials
mode. Give each editor `actAs` before they edit a source: an editor without it
can cause the source to use personal credentials. Share reports with named users or the client's domain, never link sharing. Google's
[service-account setup guide](https://docs.cloud.google.com/data-studio/set-up-a-google-cloud-service-account)
and [data credentials guide](https://docs.cloud.google.com/data-studio/data-credentials-article)
describe the managed-identity, service-agent, and editor requirements.

Set `PMAX_LOOKER_PROBE_EXPIRES_AT` to a reviewed future UTC timestamp in
`YYYY-MM-DDTHH:MM:SSZ` form covering both ladder passes. Phase 40 refuses a
missing, malformed, or expired live window before making any IAM changes.
When the variable is unset, `--plan` prints an illustrative next-day expiry,
which is not an authorization. Reuse the same explicit expiry for retries. An
expired window must be renewed through the human-run phase 40, never by phase
75 or 88. The runtime account's existing standing operator grant is unchanged.

`bq` reads the invocation-scoped
`CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT` property through its gcloud config
integration. No key, saved token, or persistent config change is needed. See
[BigQuery authentication](https://docs.cloud.google.com/bigquery/docs/authentication)
and [gcloud properties](https://docs.cloud.google.com/sdk/docs/properties).
The project Job User grant lets Looker run queries billed to this project;
the existing project-wide per-user daily query quota described below also
bounds how much this reader can query under on-demand pricing only.

The quota limits bytes queried; the reporting dataset entry bounds which data
the reader can access. A custom query quota is an approximate safeguard and
does not guarantee a spend ceiling. Review the two controls separately: adding
project Job User grants no access to raw or marts, and granting a dataset
reader does not cap query spend. The reference deployment, which is the
maintainers' own deployment whose evidence shapes the migration guide
reproduces, uses one reporting reader for all its sources, so those sources
share that user's daily quota.
[BigQuery cost controls](https://docs.cloud.google.com/bigquery/docs/custom-quotas)
describes query billing and custom quotas.

Phase 40 suppresses IAM command output, including failure arguments. It keeps
only editor and service-agent counts, the input config hash, the config object
generation on upgrades, and the probe expiry in the durable record
`deployments/<project>/looker-iam-<config-sha256-prefix>.json`. The prefix is
the first 12 characters of the config SHA-256, also retained in full in the
record. On first deployment, the generation is pending the phase-65
upload; the Go/No-Go pairs these counts with that upload's generation. Full
policies are temporary private inputs outside the evidence directory and are
removed after the audit configuration update. `--plan` prints configured
principals, including editor addresses; never commit it. Use its output
only for operator review.

## Human-run hand-over and revocation

Phase 40 prints the count of existing conditional operator Token Creator
bindings before it adds the reviewed window. Reusing an expiry is idempotent;
a different expiry adds another condition. Expired bindings remain in the
policy until removed. At hand-over and revocation, inspect that policy and
remove every obsolete operator window using its exact original expiry and
title, including expired windows:

```bash
gcloud iam service-accounts remove-iam-policy-binding \
  "pmax-looker@${PROJECT}.iam.gserviceaccount.com" \
  --project="$PROJECT" --member="$OPERATOR_MEMBER" \
  --role=roles/iam.serviceAccountTokenCreator \
  --condition="expression=request.time < timestamp(\"$EXPIRED_PROBE_EXPIRES_AT\"),title=pmax-looker-probe-window" \
  --format=none --quiet
```

Set `EXPIRED_PROBE_EXPIRES_AT` from the binding being removed, not the renewed
window. Also remove all service-agent and editor bindings and the reporting
READER entry when revoking the account. Read its last seven days of reads and
notify or repoint consumers before disabling it, prove a token mint and chart
refresh fail, and delete the account after the 30-day hold. Remove revoked
editors from report sharing separately. These are human-run actions; the
ladder never removes bindings or renews a probe window automatically.

## WIF boundary

The provider maps `sub`, `repository_id`, `repository_owner_id`, `ref`, and
`workflow_ref`. Its attribute condition requires all of these facts:

- repository id equals the recorded numeric id for `Ninety2UA/pmax-pack`;
- owner id equals the recorded numeric organization id;
- ref is exactly `refs/heads/main`;
- workflow ref is exactly
  `Ninety2UA/pmax-pack/.github/workflows/trusted.yml@refs/heads/main`.

Since v2.0.1 no workflow in the public repository matches this condition:
the trusted parity workflow was retired, so the provider grants nothing until
a workflow with exactly that ref is published again. Forks and pull requests
receive no cloud identity. Outside-collaborator Actions require approval.
Third-party actions use full commit SHAs. Never use `pull_request_target`.
If the workflow returns, the auth action exchanges GitHub OIDC directly for
this federated principal with no `service_account` input and no
`roles/iam.workloadIdentityUser` bridge; otherwise use the [pool removal procedure](#remove-the-unused-wif-pool).

### Remove the unused WIF pool

As the operator, verify no workflow depends on the retained pool before
removal. In Google Cloud console > IAM & Admin > Workload Identity Federation,
select `PROJECT`, open `pmax-pack-github`, inspect its provider
`pmax-pack-main`, and disable the provider's status. Record the change and
confirm no intended workload lost access before deleting it. In that pool's
Providers pane, choose the provider's Delete action and confirm. Then return
to the pools list, edit `pmax-pack-github`, choose Delete pool and confirm.
Read the page back with Show deleted pools and providers enabled and record
the deleted states and UTC time. See [pool and provider management](https://docs.cloud.google.com/iam/docs/manage-workload-identity-pools-providers).

Remove the retired principal set's project `bigquery.jobUser` binding in
IAM & Admin > IAM and its `bigquery.dataEditor` entries in the two CI scratch
datasets' Sharing > Permissions dialogs. Match the exact principal set from
the reviewed WIF record in private scratch, remove only those bindings, and
retain count/role summaries. Provider/pool deletion and IAM cleanup are
separate steps; do not copy policy addresses into the evidence record.

## Mandatory negative probes

Run these during the IAM/WIF and reader-probe ladder phases and retain the denied result.
Use the [operator-owned denial recipes](#operator-owned-denial-probes) for 1, 3 and 4;
the remaining items name their own phase or quota procedure.

1. Deployer cannot mint a runtime access token or access the credential secret.
2. Retired in v2.0.1 (no workflow federates): the WIF principal set cannot
   access the secret, report bucket, config bucket, parity scratch, raw,
   marts, ops, snapshots, or verification dataset. Re-run only if a workflow
   is bound again.
3. Runtime cannot write parity scratch, CI scratch, or snapshots.
4. Invoker cannot invoke another Job or read any data.
5. Retired in v2.0.1 with probe 2: a token from another repository, owner,
   ref, or workflow cannot federate. Re-run only if a workflow is bound again.
6. An over-cap query by any identity (the cap is per user, project-wide; Google does not support a per-principal override on this metric; see [custom quotas](https://docs.cloud.google.com/bigquery/docs/custom-quotas)) is rejected and the quota alert fires.

7. As the last step of phase 75, the unrecorded pre-check lists up to 20 objects in the
   configured reporting dataset as the operator with `bq ls --max_results=20`.
   It selects the first object whose type is `TABLE`, skipping views and
   snapshots. It refuses an empty page or a page without a `TABLE`.
   It reads that resolved table as `pmax-looker` using `bq head --max_rows=1`.
   Phase 88 repeats this positive control, retrying failed reads for up to
   five minutes for IAM propagation. Zero or one returned row proves the
   permission; malformed successful responses fail. The evidence records
   the resolved name, operator route, and row count, never the row contents.
   Neither this `tables.getData` probe nor list and describe calls creates
   a query job. Operator resolution explicitly clears
   `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT` for that invocation.
8. Phase 88 uses `bq ls --datasets --all --max_results=2` as `pmax-looker` and
   requires exactly the configured reporting dataset. `datasets.list` names
   the API operation; BigQuery gates dataset visibility with `datasets.get`.
   For every other dataset exported from phase 00's config, the operator
   resolves one table with `bq ls --max_results=1`. As the reader,
   `bq head --max_rows=1` on that table and `bq show --dataset` on the dataset
   must return resource access denials. If there is no table, the fixed name
   `pmax_probe_missing` is recorded, never created, and never read: BigQuery
   answers Not Found for a table that does not exist to every caller, before
   any permission check, so a synthetic name cannot prove a denial. On that
   route the phase records `tables.getData` and `tables.getData.query` as
   `NOT_PROVABLE_EMPTY_DATASET` and the dataset describe is the provable
   object. The evidence identifies the existing or synthetic route. A
   success, 404 / Not Found, token error, or transport error on a probed
   object fails the phase. The [query troubleshooting guide](https://docs.cloud.google.com/bigquery/docs/troubleshoot-queries)
   explains the access-denied response's ambiguous existence suffix; the
   [error reference](https://docs.cloud.google.com/bigquery/docs/error-messages)
   distinguishes access denial from Not Found. No probe fixtures are created.
   What this route cannot prove: on an empty dataset the describe denial
   proves only that `bigquery.datasets.get` is denied, so a custom role that
   carries `bigquery.tables.getData` without `bigquery.datasets.get` goes
   undetected until the dataset gains a table. Rerun the phase 88 probes after
   an empty dataset gains its first table.
9. Phase 88 separately submits a `SELECT` on that same name in each denied
   dataset and a `CREATE TABLE` statement in reporting, with
   `--maximum_bytes_billed=1048576`. Each must fail on data access or table
   creation. A `bigquery.jobs.create` failure does not prove these denials
   and fails the phase. The CREATE target has a unique machine suffix and
   one-hour expiry to bound an unexpected privilege inversion; if it
   succeeds, the phase stops and the operator removes it after correcting
   IAM. This deliberately attempted denial is the only probe DDL.

The probe runner writes one attempt record under
`deployments/<project>/looker-probes-<machine-suffix>.json`, including principal,
image, UTC window, request reason, resource names, and per-permission results.
It never records table rows, tokens, editor addresses, or raw command errors.
A failed attempt has `status: FAILED`; the pre-check creates no record.
Before the Go/No-Go, the operator runs the exact command stored at
`audit_log_corroboration.command` after log propagation. It uses `gcloud logging
read` with an explicit project, the reader principal, and the recorded UTC probe
window; phases 75 and 88 never run it.
The command includes `--freshness=30d`, matching the default 30-day Data Access
log retention assumption. Verify the configured retention and actual coverage
with the [log-retention readback](../docs/operations.md#log-retention-readback).
Keep the timestamp bounds when running it later:
they select the original probe window. The
[`gcloud logging read` reference](https://docs.cloud.google.com/sdk/gcloud/reference/logging/read)
states that `--freshness` works only with descending order and filters without
a timestamp; the recorded command uses ascending order and explicit timestamps. The explicit 30-day
value also documents the retention assumption if an operator adjusts that
query. A bucket's configured retention still determines which logs survive.
The filter has no request-reason or resource clause. BigQuery Data Access
entries do not carry the `bq --request_reason` value, and a denied query's entry
names its job, not the dataset (observed 2026-10-03). Match each slot by method
and status message instead:

| Slot | Filled from |
|---|---|
| `tables.getData.query` on each denied dataset | The `google.cloud.bigquery.v2.JobService.InsertJob` entry with `protoPayload.status.code` 7 whose message begins `Access Denied: Table <project>:<dataset>.<table>` |
| `tables.create` on reporting | The InsertJob entry with code 7 whose message begins `Access Denied: Dataset <project>:<reporting>: Permission bigquery.tables.create denied` |
| `tables.getData` and `datasets.get` | Pre-filled `NOT_LOGGED_BY_BIGQUERY`. BigQuery wrote no Data Access entry for a denied direct read or dataset describe, so the PERMISSION_DENIED results in the probe record are the evidence for these two. |
| `tables.getData` and `tables.getData.query` on the synthetic route | Pre-filled `NOT_PROVABLE_EMPTY_DATASET`. |

Fill each open slot with the entry's log name, insert ID, UTC timestamp,
method, and status code. Keep raw logs private and retain no editor, agent, or
delegated operator addresses in the attempt. Missing entries stay pending; a
successful probe run alone does not corroborate them. Set
`audit_log_corroboration.status` to `CORROBORATED` only after every open slot
is filled and independently reviewed. Do not substitute a failed query's
missing TableDataRead event for this proof. The first successful Looker-side
read on reporting, with the reader principal in its Data Access log,
independently proves the service-agent route. See [BigQuery audit logging](https://docs.cloud.google.com/bigquery/docs/reference/auditlogs),
[dataset visibility](https://docs.cloud.google.com/bigquery/docs/listing-datasets),
and [tabledata.list permission](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list).

Phase 80 adds `resource_labels` to the parity record: every configured dataset,
both buckets, and the job, with observation time. It checks `app=pmax` and
`env` against config and retains only `app`, `env`, and `other_label_count`.
The evidence excludes foreign label names and values. Dataset and bucket updates
merge these two labels on every ladder run; the job deploy carries both too.

### Operator-owned denial probes

Probes 1, 3 and 4 above are manual operator duties after phases 40 and 55;
no ladder phase writes their denial records. Use the actual deployer login
for probe 1 and invocation-scoped runtime/invoker credentials for probes 3
and 4. The operator has a standing runtime Token Creator grant. The shipped
IAM does not grant the operator Token Creator on the invoker: an already
reviewed administrative credential route is required for that probe. If it
is unavailable, mark the invoker checks BLOCKED; do not add a grant or count
a credential-mint failure as a resource denial.

Resolve the deployed `SECRET_NAME` and numeric `SECRET_VERSION`, configured
service-account identities, all dataset IDs and bucket names from the private
config/deployment record. Keep identities and raw failures in private scratch.
For every check, retain UTC interval, caller role, resource role, permission,
exit status and verified denial category in `EVIDENCE/iam-negative-probes.md`.
An unexpected success stops the checks. A Not Found, token-mint, network or
quota error is not the required resource denial; leave that check unresolved.

For probe 1, refresh the actual deployer login and clear CLI/environment
impersonation using the migration's [identity preparation](../docs/migrations/v2.1.0.md#guard-resource-scope).
Set `DEPLOYER_ACCOUNT` to that logged-in account and run each command
separately. Both must fail; discard access-token or secret output even if
permissions are unexpectedly broad:

```bash
gcloud auth print-access-token --account="$DEPLOYER_ACCOUNT" \
  --impersonate-service-account="$RUNTIME_SA" --lifetime=300 --quiet \
  > /dev/null 2> "$EVIDENCE_SCRATCH/deployer-mint.err"
gcloud secrets versions access "$SECRET_VERSION" --secret "$SECRET_NAME" \
  --project="$PROJECT" --account="$DEPLOYER_ACCOUNT" --quiet \
  > /dev/null 2> "$EVIDENCE_SCRATCH/deployer-secret.err"
```

The first refusal must identify `iam.serviceAccounts.getAccessToken` on the
runtime account; the second must identify `secretmanager.versions.access` on
the credential secret. Inspect failures privately, compare the targets with
the config and record only role/match and denial results. Run each expected
failure in a separate shell invocation, so the migration shell's `set -e`
does not skip a later probe. See [token minting](https://docs.cloud.google.com/sdk/gcloud/reference/auth/print-access-token)
and [secret access](https://docs.cloud.google.com/sdk/gcloud/reference/secrets/versions/access).

For probe 3, restore the operator login after probe 1: repeat the migration
[identity preparation](../docs/migrations/v2.1.0.md#guard-resource-scope), including
the CLI and ADC refresh and the in-memory principal match check. Confirm
privately that the matched account is the configured operator; stop on a
mismatch or unavailable identity. Then prove runtime token mint succeeds
through the existing operator grant. Use the phase-88 count-only route, with `set -o pipefail` so a
failed mint cannot be hidden by `wc`; no token is saved:

```bash
set -o pipefail
TOKEN_BYTES=$(gcloud auth print-access-token \
  --impersonate-service-account="$RUNTIME_SA" --lifetime=300 --quiet \
  2> "$EVIDENCE_SCRATCH/runtime-mint.err" | wc -c)
[[ "$TOKEN_BYTES" -gt 0 ]] || exit 1
```

Then set
`DENIED_DATASET` in turn to each configured parity scratch, CI scratch and
snapshot dataset. Submit this bounded table-creation attempt using a fresh
`PROBE_TABLE` identifier reviewed to be absent, such as a UTC-dated probe name:

```bash
CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT="$RUNTIME_SA" \
bq --project_id="$PROJECT" --location=EU --format=json \
  --use_gcloud_config=true --use_gcloud_config_cache=false --quiet query \
  --use_legacy_sql=false --maximum_bytes_billed=1048576 \
  "CREATE TABLE \`$PROJECT.$DENIED_DATASET.$PROBE_TABLE\` (probe INT64) OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))" \
  > "$EVIDENCE_SCRATCH/runtime-write.out" 2> "$EVIDENCE_SCRATCH/runtime-write.err"
```

Require `bigquery.tables.create` denial on each target, not failure to create
a query job. This is the same expiring-denial-target pattern as the phase-88
Looker CREATE probe. On unexpected success, stop, correct IAM and remove only
the exact probe table as the operator through BigQuery Explorer > table menu >
Delete; the one-hour expiry bounds any cleanup delay. See
[BigQuery CREATE TABLE](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_statement).

For probe 4, while still using the verified operator login, confirm that
`OTHER_JOB` is an existing disposable Job distinct from `pmax-pack-daily`,
with no data/secret mounts or external effects. Use its actual `OTHER_REGION`
and inspect its specification as the operator before the denial probe:

```bash
gcloud run jobs describe "$OTHER_JOB" --project="$PROJECT" --region="$OTHER_REGION" \
  --format=json --quiet > "$EVIDENCE_SCRATCH/other-job.json" \
  2> "$EVIDENCE_SCRATCH/other-job.err"
```

Check its image and workload purpose, environment, volumes and service account
in scratch. If no reviewed harmless Job exists, the invocation check is
BLOCKED. Resolve the dataset tables and object targets for the data checks
below now, while using the operator login.

After the operator inspection, switch to the approved administrator: repeat the migration [identity preparation](../docs/migrations/v2.1.0.md#guard-resource-scope),
including CLI/ADC refresh and the principal match check, for that approved
administrator account. Confirm its identity privately against the reviewed
credential route and stop if the route is unavailable. Execute every subsequent invoker mint and denial command using this
verified administrator login. Repeat the operator identity preparation before
any later operator work; opening another shell alone does not isolate the
active gcloud credential configuration. First prove the invoker
credential route with the same count-only mint, using existing authorization:

```bash
set -o pipefail
TOKEN_BYTES=$(gcloud auth print-access-token \
  --impersonate-service-account="$INVOKER_SA" --lifetime=300 --quiet \
  2> "$EVIDENCE_SCRATCH/invoker-mint.err" | wc -c)
[[ "$TOKEN_BYTES" -gt 0 ]] || exit 1
```

Then run the denial through the reviewed invoker route:

```bash
gcloud run jobs execute "$OTHER_JOB" --project="$PROJECT" --region="$OTHER_REGION" \
  --impersonate-service-account="$INVOKER_SA" --async --format='value(metadata.name)' --quiet \
  > "$EVIDENCE_SCRATCH/invoker-job.out" 2> "$EVIDENCE_SCRATCH/invoker-job.err"
```

Require `run.jobs.run` denial on that Job. On unexpected success, stop and
retain the harmless execution reference for operator cleanup. See
[execute a Job](https://docs.cloud.google.com/sdk/gcloud/reference/run/jobs/execute).
For the data boundary, repeat the following direct-table probe for every
configured dataset, using an existing table resolved during the operator
inspection above. If a
dataset is empty, skip the table read (a missing table is Not Found to every
caller, which proves nothing) and rely on the dataset describe; only a resource
permission denial counts, never Not Found:

```bash
CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT="$INVOKER_SA" \
bq --project_id="$PROJECT" --location=EU --format=json \
  --use_gcloud_config=true --use_gcloud_config_cache=false --quiet head \
  --max_rows=1 "$PROJECT:$DENIED_DATASET.$RESOLVED_TABLE" \
  > /dev/null 2> "$EVIDENCE_SCRATCH/invoker-read.err"
```

Require `bigquery.tables.getData` denial. Also test the configured config
object and an existing report/observation object (`DATA_OBJECT_URI`) with
`gcloud storage cat`, and the credential secret with the same secret-access
leaf used above, all through the invoker route:

```bash
gcloud storage cat "$DATA_OBJECT_URI" --project="$PROJECT" \
  --impersonate-service-account="$INVOKER_SA" --quiet \
  > /dev/null 2> "$EVIDENCE_SCRATCH/invoker-object.err"
gcloud secrets versions access "$SECRET_VERSION" --secret "$SECRET_NAME" \
  --project="$PROJECT" --impersonate-service-account="$INVOKER_SA" --quiet \
  > /dev/null 2> "$EVIDENCE_SCRATCH/invoker-secret.err"
```

Require `storage.objects.get` and `secretmanager.versions.access` denials on
the corresponding existing resources. The direct table read avoids masking a
data permission with a missing query-job permission. Match each raw response
to the expected resource and permission in scratch, then delete raw outputs
after recording the sanitized result. Preserve unresolved checks as NO-GO;
these samples do not assert a live result.

## Audit and alert contract

Data Access audit logs are enabled for BigQuery and Secret Manager. The log
metric `pmax_unexpected_secret_access` matches `AccessSecretVersion` by every
principal except `pmax-runtime` and the named operator. The human-run phase
40 creates or updates that log metric and the enabled `pMax pack unexpected
secret access` alert policy, routed to `PMAX_NOTIFICATION_CHANNEL`, before the
first live run. The operator supplies and verifies that channel in the phase
inputs; no separate metric or policy creation is required.
`PMAX_NOTIFICATION_CHANNEL` takes the channel's full resource name in the form
`projects/<project>/notificationChannels/<id>`; read it back with
`gcloud beta monitoring channels list`, which prints each channel's `name` in
that form ([notification channels](https://docs.cloud.google.com/monitoring/alerts/using-channels-api)).
The failed-job policy in `alert-policy.json` uses
`run.googleapis.com/job/completed_execution_count` with `result=failed`; it
does not use log severity. Prove it once with Scheduler paused and prove a
SKIPPED execution (a run refused by the lease) stays silent.

Follow [Measure and prove the query quota](#measure-and-prove-the-query-quota)
to set the allowance from one fixture run and prove an over-cap rejection. The
quota alert and rejection proof are required even when the current fixture
suite remains inside the free tier.

Run the credential-flip detector in the Go/No-Go after-invariants, every Monday
after the scheduled run, and after editor or data-source changes. Require zero unexplained
successful reads of reporting. The public [detector procedure](../docs/operations.md#credential-flip-detector)
defines the policy/sink checks, positive control, query and the
`deployments/<project>/looker-flip-detector-YYYY-MM-DD.md` evidence path. Treat
Looker job labels as supporting evidence only, because a credential flip can
submit a job in another billing project. Read the data project's audit log to
cover that case. Automatic partition expiry produces no `TableDataChange` entry in the
per-principal trail, so verify retention from partition metadata. Whole-table
expiry has a separate system event. See [audit-log limits](https://docs.cloud.google.com/bigquery/docs/reference/auditlogs#data_access_data_access)
and [table-expiry events](https://docs.cloud.google.com/bigquery/docs/reference/auditlogs#system_event_system_event).

### Measure and prove the query quota

This is an operator-only live measurement using synthetic fixtures. Public CI
runs offline and supplies no BigQuery usage measurement. Refresh and verify
the operator ADC identity using the [migration identity check](../docs/migrations/v2.1.0.md#guard-resource-scope).
Approve the isolated `pmax_ci_scratch` and `pmax_ci_scratch_bq` datasets and
settle all other fixture users first: this function replaces fixture tables,
adjusts their scratch dataset expiry, and cleans up the tables it created.
Do not use live datasets or run another fixture invocation concurrently.

Run one complete fixture suite from the product root with the configured
`PROJECT` and private `EVIDENCE` directory exported. This invokes the shipped
`run_fixture_parity_bq`; it uses the committed synthetic inputs and ADC:

```bash
export PROJECT EVIDENCE
uv run python - <<'PY_FIXTURE_QUOTA'
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from google.cloud import bigquery
from pmax_pack.parity import run_fixture_parity_bq

started = datetime.now(timezone.utc).isoformat()
passed = False
try:
    with bigquery.Client(project=os.environ['PROJECT'], location='EU') as client:
        passed = run_fixture_parity_bq(
            bq_client=client,
            project=os.environ['PROJECT'],
            input_dataset='pmax_ci_scratch',
            output_dataset='pmax_ci_scratch_bq',
        ).passed
finally:
    record = {'start': started, 'end': datetime.now(timezone.utc).isoformat(),
              'principal_role': 'operator', 'passed': passed}
    (Path(os.environ['EVIDENCE']) / 'fixture-quota-window.json').write_text(
        json.dumps(record, indent=2) + '\n')
raise SystemExit(0 if passed else 1)
PY_FIXTURE_QUOTA
```

A failed suite is not an allowance baseline. Inspect failures privately and
keep raw errors out of the durable record. For a successful suite, use its
exact UTC interval and the verified ADC principal in the query below. Do not
run other queries as that principal during the interval. Enter these three
query parameters privately in the BigQuery console, use `EU`, set a reviewed
maximum bytes billed, and apply the migration runner's audit labels. Wait for
job metadata to settle and account for every parent QUERY job before accepting
the sum; the later measurement query is outside the recorded interval.

```sql
SELECT
  COUNT(*) AS query_submissions,
  SUM(total_bytes_processed) AS processed_bytes,
  SUM(total_bytes_billed) AS billed_bytes,
  CAST(CEIL(SUM(total_bytes_billed) / 1048576.0) AS INT64) AS measured_mib
FROM `PROJECT.region-eu.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= @fixture_start
  AND creation_time <= @fixture_end
  AND user_email = @fixture_principal
  AND job_type = 'QUERY'
  AND parent_job_id IS NULL;
```

Size `PMAX_CI_DAILY_QUERY_QUOTA_MIB` from billed bytes; processed bytes are
diagnostic only. For higher-rate operations, reconcile the rate-normalized
usage in Quotas & System Limits before choosing the allowance. Google's
[query-usage accounting](https://docs.cloud.google.com/bigquery/docs/custom-quotas)
normalizes those operations against regular on-demand pricing, so the
`measured_mib` billed-byte total can be lower than quota usage. Inspect the
same interval's query-usage metric in IAM & Admin > Quotas & System Limits,
filtered to BigQuery and `QueryUsagePerUserPerDay`, and resolve any difference
using the jobs' operation types before setting the daily allowance. For this
regular-query fixture, retain both `processed_bytes` and `billed_bytes` and
use the billed-byte `measured_mib` as the baseline for the approved daily
fixture cadence, with any headroom explicitly reviewed and
recorded. Phase 45 accepts a nonnegative integer no larger than 10485760 and
applies it project-wide per user; its unit is MiB, not bytes. Read back the
active `QueryUsagePerUserPerDay` override in Quotas & System Limits before
probing. This quota applies only to on-demand query pricing and cannot target
an individual principal. It is approximate, so it is not a strict spend cap.

For the over-cap proof, use the BigQuery console as the approved probe
identity. Disable cached results and select a reviewed synthetic table query
whose estimated bytes exceed that user's remaining daily allowance. Keep the
query's maximum bytes billed above its estimate so that a bytes-cap rejection
cannot masquerade as a daily-quota rejection. Submit once and require the
per-user daily query-quota denial, then verify the quota-exceeded alert reaches
the operator's channel. Record the UTC interval, quota value, byte estimate,
principal role, rejection category and alert match, never the address or raw
error. An unexpected success, another denial reason, or a missing alert is
NO-GO. Do not exhaust the quota with repeated submissions. See
[custom query quotas](https://docs.cloud.google.com/bigquery/docs/custom-quotas),
[query cost controls](https://docs.cloud.google.com/bigquery/docs/best-practices-costs),
and the [JOBS schema](https://docs.cloud.google.com/bigquery/docs/information-schema-jobs).

## Review record

Record redacted policy summaries, custom-role definition, WIF provider condition,
quota override, alert policy names, negative-probe evidence, operator identity,
and UTC review time under the excluded deployment record. Never record tokens,
secret payloads, client names, account ids, editor addresses, or service-agent addresses.
