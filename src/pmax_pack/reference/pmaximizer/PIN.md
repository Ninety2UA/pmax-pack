# pMaximizer reference pin

## Source

- Repository: `google-marketing-solutions/pmax_best_practices_dashboard`
- Commit: `9790e8a585b6e6f76851efed3e9b42ad87d8d97c`
- Commit author date: 2026-04-01T11:17:29Z
- Copied scope: the ten GAQL inputs and BigQuery chain closure 01 to 05, 07,
  09, and 10 listed in KTD6.
- Integrity hash: SHA-256
  `239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528`
  over the copied files in harness execution order.

The SQL files under this directory are byte-identical copies from that commit.
They are reference inputs, not production transformations.

## Harness API version

The parity harness pins Google Ads API `v25`, the newest version against which
the post-rewrite pinned GAQL chain validates. Offline proto probes on
2026-08-26 returned no errors for both v24 and v25 with
`google-ads==31.4.0`; the newer successful probe controls the harness pin. This includes
`campaign.asset_automation_settings`, from which the upstream virtual
`url_expansion_opt_out` alias is derived. No client-library upgrade is needed
to run this harness pin.

The v25 sunset is August 2027, sourced from the plan's Google Ads API Sources
entry (`https://developers.google.com/google-ads/api/docs/release-notes`). F4
must still verify the date against Google's official sunset schedule before
using it as the operational deadline.

The production API remains v25. Live characterization on 2026-08-26 proved
that v25 rejects `campaign.url_expansion_opt_out`, so the production score
keeps the nullable value from its schema and uses the parity-neutral rule in
RULES.md when it is null.

## Runtime compatibility rewrites

The reference files remain unchanged. Before execution, the harness applies
the two enumerated compatibility rewrites documented in RULES.md:

1. the gaarf-JS backticked `some(...)` expression in
   `google_ads_queries/campaign_settings.sql` becomes the underlying repeated
   `campaign.asset_automation_settings` field for Python gaarf, after which the
   harness derives the same boolean;
2. `${format(today(),'yyyyMMdd')}` in `bq_queries/09-bpscore.sql` becomes the
   requested parity date in `YYYYMMDD` form.

`audit_reference_rewrites()` fails if a re-sync introduces another backticked
GAQL expression or `${...}` BigQuery expression. Tests cover both truth values
of the URL expression and the exact rewrite inventory.

## F4 bump procedure

1. Fetch the upstream repository and check out the candidate commit.
2. Diff every copied reference file and record all upstream rule changes.
3. Run `audit_reference_rewrites()` and classify every new runtime expression.
4. Validate every pinned GAQL input against the newest supported API version
   offline. Do not use a credential for this gate.
5. Verify that API version's sunset date from Google's official sunset table.
6. Re-run fixture coverage, the seeded red case, fixture parity, and live
   parity before changing this pin or hash.
7. Update RULES.md mappings and fixtures for every changed branch.
