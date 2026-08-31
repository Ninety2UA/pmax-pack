"""Grant a BigQuery dataset-scoped role through dataset access entries.

`bq add-iam-policy-binding --dataset` is allowlist-gated (live refusal,
2026-08-27), so dataset-scoped least-privilege grants go through the
standard access-entry surface instead. Idempotent: an existing entry is left
untouched. Roles map dataEditor -> WRITER and dataViewer -> READER; service
accounts bind as userByEmail, principalSet members as iamMember.
"""
from __future__ import annotations

import argparse
import sys

ROLE_MAP = {
    "roles/bigquery.dataEditor": "WRITER",
    "roles/bigquery.dataViewer": "READER",
}


def entry_for(member: str, role: str):
    from google.cloud.bigquery import AccessEntry

    dataset_role = ROLE_MAP[role]
    if member.startswith("serviceAccount:") or member.startswith("user:"):
        return AccessEntry(dataset_role, "userByEmail", member.split(":", 1)[1])
    if member.startswith("principalSet://") or member.startswith("principal://"):
        return AccessEntry(dataset_role, "iamMember", member)
    raise SystemExit(f"unsupported member form: {member}")


def grant(client, project: str, dataset: str, member: str, role: str) -> str:
    wanted = entry_for(member, role)
    ds = client.get_dataset(f"{project}.{dataset}")
    for existing in ds.access_entries:
        if (
            existing.role == wanted.role
            and existing.entity_type == wanted.entity_type
            and existing.entity_id == wanted.entity_id
        ):
            return "exists"
    ds.access_entries = list(ds.access_entries) + [wanted]
    client.update_dataset(ds, ["access_entries"])
    return "granted"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--member", required=True)
    parser.add_argument("--role", required=True, choices=sorted(ROLE_MAP))
    args = parser.parse_args(argv)
    from google.cloud import bigquery

    outcome = grant(bigquery.Client(project=args.project), args.project, args.dataset, args.member, args.role)
    print(f"dataset access {outcome}: {args.role} for {args.member} on {args.project}:{args.dataset}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
