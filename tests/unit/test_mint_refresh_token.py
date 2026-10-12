"""mint_refresh_token.py tests: argument handling and output discipline, offline.

The OAuth flow is replaced by a fake, or driven through the library with its
browser, local server, and token exchange stubbed, so no browser opens and no
request leaves the machine. Credential key names and fake values are assembled at runtime so
this file never holds a key next to a value (scrub_check scans tests/).
"""
from __future__ import annotations

import importlib.util
import json
import stat
from pathlib import Path

import pytest

PRODUCT = Path(__file__).resolve().parents[2]
SCRIPT = PRODUCT / "scripts" / "mint_refresh_token.py"

KEY_ID = "client" + "_id"
KEY_CS = "client" + "_secret"
KEY_REFRESH = "refresh" + "_token"

PROJECT_NUMBER = "123456789012"
FAKE_ID = PROJECT_NUMBER + "-fakeclient.apps.googleusercontent.com"
FAKE_CS = "fake" + "-client-" + "secret-value"
FAKE_REFRESH = "fake" + "-refresh-" + "token-value"


def _load():
    spec = importlib.util.spec_from_file_location("mint_refresh_token_mod", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCreds:
    def __init__(self, refresh: str | None) -> None:
        setattr(self, KEY_ID, FAKE_ID)
        setattr(self, KEY_CS, FAKE_CS)
        setattr(self, KEY_REFRESH, refresh)


class FakeFlow:
    calls: dict = {}
    refresh: str | None = FAKE_REFRESH

    @classmethod
    def from_client_secrets_file(cls, path, scopes):
        cls.calls["client_json"] = path
        cls.calls["scopes"] = list(scopes)
        return cls()

    def run_local_server(self, **kwargs):
        type(self).calls["run_local_server"] = kwargs
        return FakeCreds(type(self).refresh)


@pytest.fixture
def mint(monkeypatch):
    mod = _load()
    FakeFlow.calls = {}
    FakeFlow.refresh = FAKE_REFRESH
    monkeypatch.setattr(mod, "InstalledAppFlow", FakeFlow)
    return mod


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@pytest.mark.parametrize(
    "argv",
    [[], ["--client-json", "client.json"], ["--out", "token.json"]],
)
def test_both_arguments_are_required(mint, argv):
    with pytest.raises(SystemExit) as excinfo:
        mint.main(argv)
    assert excinfo.value.code == 2
    assert FakeFlow.calls == {}


def test_requests_adwords_scope_offline_with_consent_and_account_chooser(
    mint, tmp_path, monkeypatch
):
    monkeypatch.setenv("HOME", str(tmp_path))
    mint.main(["--client-json", "~/client.json", "--out", str(tmp_path / "t.json")])
    assert FakeFlow.calls["client_json"] == str(tmp_path / "client.json")
    assert FakeFlow.calls["scopes"] == ["https://www.googleapis.com/auth/adwords"]
    call = dict(FakeFlow.calls["run_local_server"])
    prompt = call.pop("authorization_prompt_message")
    assert call == {
        "port": 0,
        "access_type": "offline",
        "prompt": "consent select_account",
        "open_browser": True,
    }
    # The library formats {url} into this message; the URL carries the client ID.
    assert prompt and "{url}" not in prompt


def test_writes_three_keys_with_mode_600(mint, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    mint.main(["--client-json", "client.json", "--out", "~/token.json"])
    out = tmp_path / "token.json"
    assert _mode(out) == 0o600
    assert json.loads(out.read_text(encoding="utf-8")) == {
        KEY_ID: FAKE_ID,
        KEY_CS: FAKE_CS,
        KEY_REFRESH: FAKE_REFRESH,
    }


def test_existing_output_file_is_tightened_to_600(mint, tmp_path):
    out = tmp_path / "token.json"
    # Longer than the new content, so a lost O_TRUNC leaves trailing bytes.
    out.write_text("x" * 4096, encoding="utf-8")
    out.chmod(0o644)
    mint.main(["--client-json", "client.json", "--out", str(out)])
    assert _mode(out) == 0o600
    assert json.loads(out.read_text(encoding="utf-8")) == {
        KEY_ID: FAKE_ID,
        KEY_CS: FAKE_CS,
        KEY_REFRESH: FAKE_REFRESH,
    }


def test_prints_only_the_client_project_number(mint, tmp_path, capsys):
    out = tmp_path / "token.json"
    mint.main(["--client-json", "client.json", "--out", str(out)])
    captured = capsys.readouterr()
    blob = captured.out + captured.err
    assert f"client project number: {PROJECT_NUMBER}" in captured.out
    for value in (FAKE_ID, FAKE_CS, FAKE_REFRESH):
        assert value not in blob


def test_missing_refresh_token_exits_without_writing(mint, tmp_path, capsys):
    FakeFlow.refresh = None
    out = tmp_path / "token.json"
    with pytest.raises(SystemExit) as excinfo:
        mint.main(["--client-json", "client.json", "--out", str(out)])
    assert "no refresh token" in str(excinfo.value.code)
    assert not out.exists()
    captured = capsys.readouterr()
    assert FAKE_CS not in captured.out + captured.err


def test_real_flow_prints_no_client_id_or_credential(tmp_path, monkeypatch, capsys):
    """Drive the library's own run_local_server with only its network edges stubbed."""
    import google_auth_oauthlib.flow as flow_lib

    mod = _load()
    opened: list[str] = []

    class Browser:
        def open(self, url, new=0, autoraise=True):
            opened.append(url)
            return True

    class Server:
        server_port = 8765
        timeout = None

        def __init__(self, app):
            self.app = app

        def handle_request(self):
            self.app.last_request_uri = "http://localhost:8765/?code=fake-code"

        def server_close(self):
            pass

    monkeypatch.setattr(flow_lib.webbrowser, "get", lambda browser=None: Browser())
    monkeypatch.setattr(
        flow_lib.wsgiref.simple_server,
        "make_server",
        lambda host, port, app, **kwargs: Server(app),
    )
    monkeypatch.setattr(flow_lib.InstalledAppFlow, "fetch_token", lambda self, **kwargs: None)
    monkeypatch.setattr(
        flow_lib.InstalledAppFlow, "credentials", property(lambda self: FakeCreds(FAKE_REFRESH))
    )
    client_json = tmp_path / "client.json"
    installed = {
        KEY_ID: FAKE_ID,
        KEY_CS: FAKE_CS,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost"],
    }
    client_json.write_text(json.dumps({"installed": installed}), encoding="utf-8")

    mod.main(["--client-json", str(client_json), "--out", str(tmp_path / "token.json")])

    assert len(opened) == 1
    assert FAKE_ID in opened[0] and "access_type=offline" in opened[0]
    captured = capsys.readouterr()
    blob = captured.out + captured.err
    for value in (FAKE_ID, FAKE_CS, FAKE_REFRESH):
        assert value not in blob
    assert f"client project number: {PROJECT_NUMBER}" in captured.out
