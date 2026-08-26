"""Redaction tests. Proof-first: planted values assembled at runtime."""
from __future__ import annotations

import io
import logging
import logging.config
import subprocess
import sys
from pathlib import Path

from pmax_pack.redact import install_redaction, redact

PRODUCT = Path(__file__).resolve().parents[2]
CANARY_REFRESH = "1/" + "/0canaryCANARY0canaryCANARY0000"
C_VAL2 = "GOC" + "SPX-" + "canaryClientSecret0001"
_REFRESH_KEY = "refresh" + "_token"
_DEVELOPER_KEY = "developer" + "_token"
_CLIENT_SECRET_KEY = "client" + "_secret"


def test_redact_masks_refresh_token_shape():
    text = f"{_REFRESH_KEY}: {CANARY_REFRESH}"
    out = redact(text)
    assert CANARY_REFRESH not in out
    assert "<redacted:refresh_token>" in out


def test_redact_masks_developer_token_and_client_secret():
    text = (
        f"{_DEVELOPER_KEY}: canaryDevTokenValue0001\n"
        f"{_CLIENT_SECRET_KEY}: {C_VAL2}\n"
    )
    out = redact(text)
    assert "canaryDevTokenValue0001" not in out
    assert C_VAL2 not in out
    assert "<redacted:developer_token>" in out
    assert "<redacted:client_secret>" in out


def test_canary_refresh_token_absent_from_captured_logs():
    """Forced client-construction failure must not leak a canary token."""
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setLevel(logging.DEBUG)
    root = logging.getLogger()
    prev_level = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        install_redaction()
        cred_blob = (
            f"{_REFRESH_KEY}: {CANARY_REFRESH}\n"
            f"{_CLIENT_SECRET_KEY}: {C_VAL2}\n"
        )
        log = logging.getLogger("pmax_pack.ads_client")
        try:
            raise RuntimeError(
                "failed to construct Google Ads client from broken "
                f"credential file: {cred_blob}"
            )
        except RuntimeError:
            log.exception("client construction failed")
        handler.flush()
        captured = buf.getvalue()
        assert CANARY_REFRESH not in captured
        assert C_VAL2 not in captured
        assert "1//0canary" not in captured
    finally:
        root.removeHandler(handler)
        root.setLevel(prev_level)


def test_b1_child_logger_added_after_install_emits_redacted_text():
    install_redaction()
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setLevel(logging.DEBUG)
    child = logging.getLogger("pmax_pack.b1_child")
    child.handlers.clear()
    child.filters.clear()
    child.propagate = False
    child.addHandler(handler)
    child.setLevel(logging.DEBUG)
    canary = "1/" + "/0" + "w" * 34
    child.info("token %s", canary)
    child.info("tok " + canary)
    handler.flush()
    captured = buf.getvalue()
    assert canary not in captured
    assert "<redacted:refresh_token>" in captured


def test_b1_dictconfig_after_install_redacts_traceback():
    install_redaction()
    buf = io.StringIO()
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "handlers": {
                "h": {
                    "()": logging.StreamHandler,
                    "stream": buf,
                }
            },
            "root": {"handlers": ["h"], "level": "DEBUG"},
        }
    )
    canary = "1/" + "/0" + "z" * 34
    log = logging.getLogger("google.ads.googleads.client")
    log.error("plain " + canary)
    try:
        raise RuntimeError(f"client failed {canary}")
    except RuntimeError:
        log.exception("failed to load credentials")
    err = buf.getvalue()
    assert canary not in err
    assert "Traceback" in err or "RuntimeError" in err


def test_b2_multiline_pem_block_collapses():
    head = "-----BEGIN RSA " + "PRIVATE KEY-----"
    foot = "-----END RSA " + "PRIVATE KEY-----"
    body1 = "MIIEvSelfTestBodyLineOneAAAABBBBCCCC0000"
    body2 = "c2VsZlRlc3RCb2R5TGluZVR3bw=="
    text = f"before\n{head}\n{body1}\n{body2}\n{foot}\nafter\n"
    out = redact(text)
    assert body1 not in out
    assert body2 not in out
    assert "-----BEGIN" not in out
    assert "<redacted:private_key_block>" in out


def test_b2_truncated_pem_redacts_through_end():
    head = "-----BEGIN " + "PRIVATE KEY-----"
    body = "MIIEvTruncatedBodyCanaryAAAA1111"
    text = f"before\n{head}\n{body}\nstill secret\n"
    out = redact(text)
    assert body not in out
    assert "still secret" not in out
    assert "<redacted:truncated_private_key_block>" in out


def test_b2_escaped_service_account_json():
    head = "-----BEGIN " + "PRIVATE KEY-----"
    foot = "-----END " + "PRIVATE KEY-----"
    body = "MIIEvSaJsonSelfTestBodyOneLine9uZQ"
    pk_key = '"private' + '_key": '
    text = (
        '{"type": "service_account", ' + pk_key
        + f'"{head}\\n{body}\\n{foot}\\n", "client_email": "t@t"}}\n'
    )
    out = redact(text)
    assert body not in out
    assert "-----BEGIN" not in out
    assert "-----END" not in out
    assert "<redacted:" in out
    assert '"client_email": "t@t"' in out


def test_b2_authorization_bearer_token():
    tok = "OpaqueBearerCanary" + "0123456789abcdef"
    text = f"Authorization: Bearer {tok}\n"
    out = redact(text)
    assert tok not in out
    assert "Bearer <redacted:bearer_token>" in out


def test_b2_bare_bearer_token():
    tok = "OpaqueBearerCanary" + "0123456789abcdef"
    text = f"curl -H bearer {tok}\n"
    out = redact(text)
    assert tok not in out
    assert "bearer <redacted:bearer_token>" in out


def test_b2_url_userinfo_redacted():
    pw = "p/assw" + "ordcanary88"
    text = f"https://admin:{pw}@ads.example/api\n"
    out = redact(text)
    assert pw not in out
    assert "admin" not in out
    assert "https://<redacted:url_credentials>@ads.example/api" in out


def test_b2_assignment_equals():
    val = "swordfishcanary0001"
    eq_key = "DATABASE" + "_PASSWORD="
    text = eq_key + val + "\n"
    out = redact(text)
    assert val not in out
    assert eq_key in out
    assert "<redacted:credential_assignment>" in out


def test_b2_assignment_colon():
    val = "dotpasscanary0001"
    colon_key = "db." + "password" + ": "
    text = colon_key + val + "\n"
    out = redact(text)
    assert val not in out
    assert colon_key in out
    assert "<redacted:credential_assignment>" in out


def test_b2_assignment_json_key():
    val = "jsonkeycanary0001"
    json_key = '"api' + '_key": '
    text = json_key + '"' + val + '"\n'
    out = redact(text)
    assert val not in out
    assert json_key in out
    assert "<redacted:credential_assignment>" in out


def test_b2_grpc_developer_token_hyphenated():
    val = "grpcDevTokenCanary0001"
    hyphen_key = "developer" + "-token" + ": "
    text = hyphen_key + val + "\n"
    out = redact(text)
    assert val not in out
    assert "<redacted:developer_token>" in out
    assert hyphen_key in out


def test_b3_excepthook_redacts_unhandled_canary():
    canary = "1/" + "/0" + "q" * 34
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; sys.path.insert(0, 'src'); "
            "from pmax_pack.redact import install_redaction; "
            "install_redaction(); "
            "raise RuntimeError('tok ' + '1/' + '/0' + 'q'*34)",
        ],
        cwd=str(PRODUCT),
        capture_output=True,
        text=True,
        check=False,
    )
    blob = result.stdout + result.stderr
    assert result.returncode != 0
    assert canary not in blob
    assert "<redacted:refresh_token>" in blob
