# pMax Performance Pack

Google Ads Performance Max data engine: gaarf extraction to BigQuery marts
for best-practice scores, asset performance, and cohort CPA and ROAS, with
a validation report.

## Who it is for

Performance Max advertisers with multi-day conversion lag (e-commerce and
app-purchase accounts). Operators deploy the pack to their own GCP project
and point it at allowlisted accounts.

## Stack

- Python 3.12, `uv` lockfile
- gaarf (`google-ads-api-report-fetcher`) against Google Ads API v25
- BigQuery marts, Cloud Run Job runtime
- Fixture-only pull-request CI (no credentials)

## Run

Boot (no dependencies):

```
python3 src/main.py
```

CLI (after `uv sync --locked`):

```
uv run pmax-pack --help
```
