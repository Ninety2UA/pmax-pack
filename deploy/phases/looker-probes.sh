#!/usr/bin/env bash
# Shared by the unrecorded pass-1 pre-check and phase 88's recorded proof.
# No IAM changes. bq uses the invocation-scoped gcloud impersonation property.
looker_probes() {
  uv run python - "$1" "$PLAN" "$PROJECT" "$DATASET_REPORTING" \
    "$DATASETS_CSV" "$ROOT" "${IMAGE_REF:-}" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
import uuid
import unicodedata

mode, plan_text, project, reporting, dataset_csv, root, image = sys.argv[1:]
plan = plan_text == "1"
if mode not in {"precheck", "full"}:
    raise SystemExit("Looker probe: invalid mode")
datasets = list(dict.fromkeys(dataset_csv.split(",")))
if reporting not in datasets or any(not re.fullmatch(r"[A-Za-z0-9_]{1,1024}", d) for d in datasets):
    raise SystemExit("Looker probe: invalid exported dataset list")
principal = f"pmax-looker@{project}.iam.gserviceaccount.com"
started_at = datetime.now(timezone.utc).isoformat()
request_reason = f"pmax-looker-{mode}-{uuid.uuid4().hex}"
base = ["bq", f"--project_id={project}", "--location=EU", "--format=json",
        "--use_gcloud_config=true", "--use_gcloud_config_cache=false", "--quiet",
        f"--request_reason={request_reason}"]
evidence: dict = {"principal": principal, "image_ref": image,
                  "started_at": started_at, "request_reason": request_reason,
                  "status": "RUNNING", "probes": []}
record: Path | None = None
if mode == "full" and not plan:
    directory = Path(root) / "deployments" / project
    directory.mkdir(parents=True, exist_ok=True)
    record = directory / f"looker-probes-{request_reason.rsplit('-', 1)[1]}.json"


def save() -> None:
    """Persist only outcomes and resource references, never table rows or errors."""
    if record is not None:
        temporary = record.with_suffix(".tmp")
        temporary.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        temporary.replace(record)


def fail(message: str) -> None:
    """Fail closed and leave an explicit failed attempt instead of stale proof."""
    evidence["status"] = "FAILED"
    evidence["failure"] = message
    save()
    raise SystemExit(f"Looker probe: {message}")


def invoke(args: list[str], *, impersonate: bool = True) -> subprocess.CompletedProcess:
    """Use a fresh invocation-scoped identity without changing local CLI config."""
    env = dict(os.environ)
    env["CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT"] = principal if impersonate else ""
    # Force non-interactive bq's internal config-helper, including auth failures.
    env["CLOUDSDK_CORE_DISABLE_PROMPTS"] = "1"
    return subprocess.run(base + args, env=env, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, check=False)


def preview(args: list[str], *, impersonate: bool = True) -> None:
    """Print the exact leaf in plan mode without making cloud calls."""
    identity = principal if impersonate else ""
    prefix = ["env", f"CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT={identity}"]
    print("PLAN  " + shlex.join(prefix + base + args))


def note(permission: str, dataset: str, outcome: str, **details: object) -> None:
    evidence["probes"].append({"permission": permission, "dataset": dataset,
                               "outcome": outcome, **details})
    save()
    print(f"Looker probe: {permission} {dataset} {outcome}")


def resource_error(response: subprocess.CompletedProcess) -> tuple[bool, bool]:
    """Read structured error fields or the CLI error header, not resource text."""
    message = " ".join((response.stdout + "\n" + response.stderr).split())
    structured = False
    for stream in (response.stdout, response.stderr):
        try:
            data = json.loads(stream)
        except ValueError:
            continue
        if not isinstance(data, dict) or not isinstance(data.get("error"), dict):
            continue
        structured = True
        error = data["error"]
        errors = error.get("errors", [])
        if not isinstance(errors, list) or any(not isinstance(e, dict) for e in errors):
            return False, False
        reasons = {e.get("reason") for e in errors}
        if error.get("code", 403) != 403 or (reasons and reasons != {"accessDenied"}):
            return False, False
        if not reasons and error.get("status") != "PERMISSION_DENIED":
            return False, False
        message = " ".join(str(error.get("message", "")).split())
        if not message:
            message = " ".join(str(e.get("message", "")) for e in errors)
        break
    if not structured:
        # bq_error.py prefixes the API message with operation and job references.
        message = re.sub(r"^BigQuery error in \w+ operation:\s*", "", message)
        message = re.sub(r"^Error processing job '[^']+':\s*", "", message)
        http = re.match(r"^(\d{3}) (?:GET|POST) https?://\S+:\s*", message)
        if http:
            if http[1] != "403":
                return False, False
            message = message[http.end():]
        if not re.match(r"^Access Denied:\s*", message, re.I):
            return False, False
    job_denied = bool(re.search(r"\bbigquery\.jobs\.create\b", message))
    resource = bool(re.search(
        r"\bbigquery\.(?:tables\.(?:getData|get|create)|datasets\.get)\b", message
    ) or re.match(r"^Access Denied: (?:Table|Dataset) ", message, re.I))
    return resource and not job_denied, job_denied


def denied(args: list[str], permission: str, dataset: str, **details: object) -> None:
    if plan:
        preview(args)
        return
    response = invoke(args)
    if response.returncode == 0:
        fail(f"{permission} {dataset}: expected denial, received success")
    resource_denied, job_denied = resource_error(response)
    if job_denied:
        fail(f"{permission} {dataset}: bigquery.jobs.create denied, data permission unproven")
    if not resource_denied:
        fail(f"{permission} {dataset}: expected resource access denial, received another error")
    note(permission, dataset, "PERMISSION_DENIED", **details)


def resolve_table(dataset: str, *, positive: bool = False) -> tuple[str, str]:
    """Resolve a bounded object as the operator, never create probe fixtures."""
    page_size = 20 if positive else 1
    resolution = ["ls", f"--max_results={page_size}", f"{project}:{dataset}"]
    if plan:
        preview(resolution, impersonate=False)
        return ("RESOLVED_REPORTING_TABLE" if positive else "RESOLVED_TABLE_OR_pmax_probe_missing"), "plan"
    response = invoke(resolution, impersonate=False)
    if response.returncode:
        fail(f"operator table resolution failed for {dataset}")
    try:
        tables = json.loads(response.stdout)
        if not isinstance(tables, list) or len(tables) > page_size:
            raise ValueError
        if positive:
            if any(not isinstance(item, dict)
                   or not isinstance(item.get("type"), str)
                   or not isinstance(item.get("tableReference"), dict) for item in tables):
                raise ValueError
            tables = [item for item in tables if item["type"] == "TABLE"]
        if not tables:
            if positive:
                fail(f"no table in reporting dataset {dataset} within the first {page_size} objects")
            return "pmax_probe_missing", "synthetic"
        table = tables[0]["tableReference"]["tableId"]
        # BigQuery table naming: L, M, N, Pc, Pd, Zs; at most 1,024 UTF-8 bytes.
        # These categories exclude SQL quote, backslash and path separators.
        if (not isinstance(table, str) or not 1 <= len(table.encode("utf-8")) <= 1024
                or any(unicodedata.category(c)[0] not in {"L", "M", "N"}
                       and unicodedata.category(c) not in {"Pc", "Pd", "Zs"} for c in table)):
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError):
        fail(f"operator table resolution returned invalid data for {dataset}")
    return table, "existing"


def sql_name(dataset: str, table: str) -> str:
    """Quote the full identifier; invoke passes every command as an argv list."""
    name = f"{project}.{dataset}.{table}".replace("\\", "\\\\").replace("`", "\\`")
    return f"`{name}`"


save()
positive_table, positive_route = resolve_table(reporting, positive=True)
positive = ["head", "--max_rows=1", f"{project}:{reporting}.{positive_table}"]
if plan:
    preview(positive)
else:
    try:
        retry_seconds = int(os.environ.get("PMAX_LOOKER_RETRY_SECONDS", "300"))
    except ValueError:
        fail("PMAX_LOOKER_RETRY_SECONDS must be an integer from 0 to 300")
    if not 0 <= retry_seconds <= 300:
        fail("PMAX_LOOKER_RETRY_SECONDS must be an integer from 0 to 300")
    deadline = time.monotonic() + retry_seconds
    while True:
        response = invoke(positive)
        if response.returncode == 0:
            try:
                rows = json.loads(response.stdout)
            except json.JSONDecodeError:
                fail("tables.getData positive control returned invalid JSON")
            if not isinstance(rows, list) or len(rows) > 1 or any(not isinstance(row, dict) for row in rows):
                fail("tables.getData positive control must return zero or one row")
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            fail("tables.getData positive control failed within the five-minute retry window")
        time.sleep(min(10, remaining))
    note("tables.getData", reporting, "ALLOWED", table=positive_table, route=positive_route,
         resolved_by="operator", row_count=len(rows))

if mode == "precheck":
    sys.exit(0)

# Two results suffice to disprove 'exactly one'; --all includes hidden datasets.
listing = ["ls", "--datasets", "--all", "--max_results=2", f"{project}:"]
if plan:
    preview(listing)
else:
    response = invoke(listing)
    if response.returncode:
        fail("datasets.list failed")
    try:
        visible = [row["datasetReference"]["datasetId"] for row in json.loads(response.stdout)]
    except (ValueError, TypeError, KeyError):
        fail("datasets.list returned invalid data")
    if visible != [reporting]:
        fail("datasets.list must return exactly the reporting dataset")
    # datasets.list is the API operation; visibility is gated by datasets.get.
    note("datasets.list", reporting, "ALLOWED_ONLY_REPORTING")

queries: list[tuple[str, str, str]] = []
for dataset in datasets:
    if dataset == reporting:
        continue
    table, route = resolve_table(dataset)
    denied(["head", "--max_rows=1", f"{project}:{dataset}.{table}"],
           "tables.getData", dataset, table=table, route=route, resolved_by="operator")
    denied(["show", "--dataset", f"{project}:{dataset}"], "datasets.get", dataset,
           table=table, route=route, resolved_by="operator")
    queries.append((dataset, table, route))

# Exercise query-shaped permissions separately from the metadata and row APIs.
query_flags = ["query", "--use_legacy_sql=false", "--maximum_bytes_billed=1048576"]
for dataset, table, route in queries:
    sql = f"SELECT 1 FROM {sql_name(dataset, table)} LIMIT 1"
    denied(query_flags + [sql], "tables.getData.query", dataset, table=table, route=route, resolved_by="operator")
create_table = "pmax_probe_forbidden_" + request_reason.rsplit("-", 1)[1]
sql = (f"CREATE TABLE {sql_name(reporting, create_table)} (probe INT64) "
       "OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))")
denied(query_flags + [sql], "tables.create", reporting, table=create_table)
if not plan:
    evidence["status"] = "PASSED"
    evidence["finished_at"] = datetime.now(timezone.utc).isoformat()
    resources = sorted({
        f"projects/{project}/datasets/{probe['dataset']}"
        + (f"/tables/{probe['table']}" if probe.get("table") else "")
        for probe in evidence["probes"]
    } | {f"projects/{project}/datasets/{d}" for d in datasets})
    resource_filter = " OR ".join(f"protoPayload.resourceName={json.dumps(r, ensure_ascii=False)}" for r in resources)
    log_filter = " AND ".join([
        'protoPayload.serviceName="bigquery.googleapis.com"',
        f"protoPayload.authenticationInfo.principalEmail={json.dumps(principal)}",
        f"protoPayload.requestMetadata.requestAttributes.reason={json.dumps(request_reason)}",
        f"timestamp>={json.dumps(started_at)}",
        f"timestamp<={json.dumps(evidence['finished_at'])}",
        "(" + resource_filter + ")",
    ])
    audit_argv = ["gcloud", "logging", "read", log_filter, f"--project={project}",
                  "--freshness=30d", "--order=asc", "--format=json", "--quiet"]
    evidence["audit_log_corroboration"] = {
        "status": "OPERATOR_PENDING", "filter": log_filter, "argv": audit_argv,
        "command": shlex.join(["env", "CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=", *audit_argv]),
    }
    evidence["audit_log"] = {d: {"tables.getData": None, "datasets.get": None}
                             for d in datasets if d != reporting}
    save()
    print(f"Looker probe evidence: {record}")
PY
}
