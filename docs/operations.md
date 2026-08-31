# Operations

The pack runs as one private, digest-pinned Cloud Run Job in
`europe-west1`. A paused-first Cloud Scheduler trigger starts the daily run in
the configured account timezone. Configuration comes from a private GCS YAML
object. Google Ads credentials come from one pinned Secret Manager version
mounted as a file. No credential is baked into the image.

That secret holds the shared MCC token from the operator's existing Google Ads
credential file and own Google login. The credential's top-manager
`login_customer_id` provides access, but the deployed config's exact account
list is the only extraction bound. The pack is therefore a shared-token
consumer: rotation must add and pin its replacement Secret Manager version,
and compromise of the pack secret has the blast radius of the whole MCC.

Use `deploy/deploy.sh --plan` to inspect the ordered deployment. The plan
performs only read-only target checks and prints every later phase with an
`agent-safe` or `human-run` owner marker. API enablement, IAM, WIF, invoker
binding, sign-off, resume, and all destructive recovery actions remain
operator-owned. The alert phase is agent-safe but still refuses without both
operator confirmations.

The runtime identity can write only the raw, mart, ops, and verification
datasets. Trusted CI uses direct WIF, without service-account impersonation,
and can write only the fixed CI scratch pair while carrying no Google Ads
credential. A concurrency group serializes that pair. Pull-request CI is
fixture-only and credential-free. Both CI paths lint SQL template dataset
references under `src/pmax_pack/sql`; Python-embedded SQL is outside this
lint's scope. Real-data parity is run locally by the named operator and
verified through its ledger row.

Reports are written to `reports/<deployment>/<run_id>.md` with a `latest.md`
pointer. SKIPPED executions never advance the pointer. Observation history is
append-only and backed up as Avro under `observations/`; the report-bucket
lifecycle rules do not expire that prefix.

Every execution mints its own time-sortable, label-safe run ID. `PMAX_RUN_ID`
does not replace that ID; when set, its normalized value is appended as a
correlation suffix and truncated only as needed to keep the complete ID within
BigQuery's 63-character label limit. Operators can therefore tag a repair
without allowing an older same-day observation to remain the selected winner.

When `start_date` is omitted, the execution window still begins 90 days before
that run's date, but the checkpoint hash uses the stable `default-90d` token.
An explicit `start_date` contributes its ISO date to the hash. Advancing the
calendar alone therefore does not invalidate every completed backfill chunk.
With `start_date` omitted, the first run after this upgrade computes a different
checkpoint hash than the previous scheme, so every chunk re-extracts once (the
same four-month re-pull that previously happened daily); subsequent hashes
remain stable.

`rebuild --dry-run` issues no billed reads. It skips the family-D derivation
query and the report collectors, uses the configured maximum cohort plus
restatement margin, records `dry_run_config_fallback`, and reports an
upper-bound cost estimate for the potentially longer rebuild window. The
report states `dry-run: report collectors skipped` so operators can distinguish
the intentionally absent metrics from collection failures.

Before an upgrade, pause Scheduler, record the observation row count, distinct
observed days, and latest observed day, confirm the rollback digest exists,
review internal-table schema changes, and optionally snapshot marts. A rollback
sets `PMAX_IMAGE_REF` to the prior digest so phase 50 verifies and redeploys it
without rebuilding an image, runs data rebuild mode, and
requires the observation triple to stay at or above its prior values.

Shared-token rotation probes the operator's replacement file first, adds a
Secret Manager version, updates the Job to that numeric version, verifies one
pack run and every other consumer, and revokes the previous token last. A
compromise revokes first and redistributes to all consumers, including the
pack.

The private `RUNBOOK.md` carries the complete go/no-go checklist, schema
migration procedure, observation restore, credential rotation, decommission,
offboarding, and five-day maximum unattended-window rules. It is intentionally
excluded from the scrubbed public export.
