"""Dataset access-entry grants: idempotent, role-mapped, member-typed."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

PRODUCT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "grant_dataset_access", PRODUCT / "deploy" / "lib" / "grant_dataset_access.py"
)
mod = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(mod)


class FakeDataset:
    def __init__(self, entries):
        self.access_entries = entries


class FakeClient:
    def __init__(self, entries):
        self.dataset = FakeDataset(entries)
        self.updates = []

    def get_dataset(self, ref):
        return self.dataset

    def update_dataset(self, ds, fields):
        self.updates.append((list(ds.access_entries), fields))


def test_service_account_grant_is_writer_by_email_and_idempotent():
    client = FakeClient([])
    sa = "serviceAccount:pmax-runtime@p.iam.gserviceaccount.com"
    assert mod.grant(client, "p", "pmax_raw", sa, "roles/bigquery.dataEditor") == "granted"
    (entries, fields), = client.updates
    entry = entries[-1]
    assert (entry.role, entry.entity_type, entry.entity_id) == (
        "WRITER", "userByEmail", "pmax-runtime@p.iam.gserviceaccount.com")
    assert fields == ["access_entries"]
    client.dataset.access_entries = entries
    assert mod.grant(client, "p", "pmax_raw", sa, "roles/bigquery.dataEditor") == "exists"
    assert len(client.updates) == 1


def test_principal_set_binds_as_iam_member_reader():
    client = FakeClient([])
    member = "principalSet://iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/attribute.repository_id/2"
    assert mod.grant(client, "p", "pmax_ci_scratch", member, "roles/bigquery.dataViewer") == "granted"
    entry = client.updates[0][0][-1]
    assert (entry.role, entry.entity_type, entry.entity_id) == ("READER", "iamMember", member)
