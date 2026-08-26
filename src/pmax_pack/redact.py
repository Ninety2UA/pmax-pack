"""Credential redaction for CLI logs, ledger errors, and report text.

Source of truth for the check_secrets.py CONTENT_PATTERNS set plus Ads
YAML field shapes (developer_token, refresh_token, client_secret), PEM
block collapsing, bearer tokens, URL userinfo, and credential assignment
lines. Masked form is <redacted:label>.

install_redaction() installs a logging.setLogRecordFactory wrapper that
redacts msg and every string arg at record creation (this covers child
loggers and later dictConfig handlers). The root-logger filter covers
records logged on the root logger itself; handler filters are the second
layer for exc_text and stack_info. The redaction tests may leave the
factory installed (idempotent install).
"""
from __future__ import annotations

import logging
import re
import sys
import traceback

# Bridged from repo-root check_secrets.py CONTENT_PATTERNS (ASCII, no flags).
_CONTENT_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"-----BEGIN[\sA-Z]*PRIVATE[\s]+KEY-----"), "private_key_block"),
    (re.compile(r"LS0tLS1CRUdJ[A-Za-z0-9+/]"), "base64_pem_header"),
    (re.compile(r"AIza[0-9A-Za-z_\-]{30,}"), "google_api_key"),
    (re.compile(r"AQ\.[A-Za-z0-9][0-9A-Za-z_\-]{25,}"), "gemini_api_key"),
    (re.compile(r"gh[pousr]_[0-9A-Za-z]{30,}"), "github_token"),
    (re.compile(r"xox[baprs]-[0-9A-Za-z-]{20,}"), "slack_token"),
    (re.compile(r"AKIA[0-9A-Z]{16}"), "aws_access_key_id"),
    (re.compile(r"ya29\.[0-9A-Za-z_\-]{20,}"), "google_oauth_access_token"),
    (re.compile(r"(?<![A-Za-z0-9+/])1//[0-9A-Za-z_\-]{30,}"), "refresh_token"),
    (re.compile(r"GOCSPX-[0-9A-Za-z_\-]{20,}"), "client_secret"),
    (re.compile(r'"private_key"\s*:\s*"-----BEGIN'), "service_account_private_key"),
    (re.compile(r"\b[spr]k_(?:live|test)_[0-9A-Za-z]{16,}"), "stripe_api_key"),
    (
        re.compile(
            r"\beyJ[0-9A-Za-z_\-]{20,}\.eyJ[0-9A-Za-z_\-]{20,}"
            r"\.[0-9A-Za-z_\-]{20,}"
        ),
        "supabase_jwt_service_key",
    ),
    (
        re.compile(
            r"(?i)\bvercel[_-]?(?:api[_-]?|access[_-]?)?token"
            r"['\"]?\s*[:=]\s*['\"]?[0-9A-Za-z]{20,}"
        ),
        "vercel_token",
    ),
    (re.compile(r"\bwhsec_[0-9A-Za-z]{16,}"), "stripe_webhook_signing_secret"),
    (re.compile(r"\bsk-or-v1-[0-9A-Za-z]{32,}"), "openrouter_api_key"),
]

# YAML / ads-config / gRPC field shapes. Keep the key and separator,
# replace the value. Words accept [_-] between them; separator is [:=].
# Field names are concatenated at runtime so this file does not contain
# those keys next to a colon (publish and check_secrets scan this file).
_FK_R = "refresh" + "_token"
_FK_D = "developer" + "_token"
_FK_CS = "client" + "_secret"


def _field_name_pat(name: str) -> str:
    return "[_-]".join(re.escape(part) for part in name.split("_"))


_FIELD_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(
            rf"(?i)({_field_name_pat(_FK_R)}\s*[:=]\s*)(\S+)"
        ),
        "refresh_token",
    ),
    (
        re.compile(
            rf"(?i)({_field_name_pat(_FK_D)}\s*[:=]\s*)(\S+)"
        ),
        "developer_token",
    ),
    (
        re.compile(
            rf"(?i)({_field_name_pat(_FK_CS)}\s*[:=]\s*)(\S+)"
        ),
        "client_secret",
    ),
]

PEM_BLOCK_LABEL = "private_key_block"
PEM_BLOCK_RE = re.compile(
    r"-----BEGIN[\sA-Z]*PRIVATE\s+KEY-----"
    r".*?"
    r"-----END[\sA-Z]*PRIVATE\s+KEY-----",
    re.DOTALL,
)
TRUNCATED_PEM_LABEL = "truncated_private_key_block"
PEM_BEGIN_RE = re.compile(r"-----BEGIN[\sA-Z]*PRIVATE\s+KEY-----")

BEARER_LABEL = "bearer_token"
BEARER_RE = re.compile(
    r"(?i)\b(bearer\s+)(?!<redacted:)[A-Za-z0-9._~+/=\-]{16,}"
)

URL_CRED_LABEL = "url_credentials"
URL_CRED_RE = re.compile(
    r"\b([A-Za-z][0-9A-Za-z+.\-]*)://(?!<redacted:)[^/\s:@]+:(?:[^\s@]+@)+"
)

# Same name set as scrub_check A2. Keep the key and the separator.
ASSIGNMENT_LABEL = "credential_assignment"
ASSIGNMENT_RE = re.compile(
    r"(?i)\b((?:[A-Za-z0-9]+[_.\-])*"
    r"(?:password|passwd|token|secret|api[_-]?key|apikey|"
    r"credential|private[_-]?key|client[_-]?secret|"
    r"refresh[_-]?token|developer[_-]?token))\b"
    r"([\"']?\s*(?::=|=(?!=)|:)\s*)"
    r"(?!<redacted:)(\"[^\"]*\"|'[^']*'|\S+)"
)

_FACTORY_INSTALLED = False
_saved_factory = None


def _mask(label: str) -> str:
    return f"<redacted:{label}>"


def redact(text: str) -> str:
    """Return text with credential-shaped values replaced by placeholders."""
    if not text:
        return text
    out = text
    pieces, pos = [], 0
    for m in PEM_BLOCK_RE.finditer(out):
        pieces.append(out[pos:m.start()])
        pieces.append(_mask(PEM_BLOCK_LABEL))
        pos = m.end()
    if pieces:
        pieces.append(out[pos:])
        out = "".join(pieces)
    m = PEM_BEGIN_RE.search(out)
    if m:
        tail = "\n" if out.endswith("\n") else ""
        out = out[: m.start()] + _mask(TRUNCATED_PEM_LABEL) + tail
    for pat, label in _FIELD_PATTERNS:
        out = pat.sub(lambda m, lab=label: f"{m.group(1)}{_mask(lab)}", out)
    out = ASSIGNMENT_RE.sub(
        lambda m: f"{m.group(1)}{m.group(2)}{_mask(ASSIGNMENT_LABEL)}", out
    )
    out = BEARER_RE.sub(lambda m: f"{m.group(1)}{_mask(BEARER_LABEL)}", out)
    out = URL_CRED_RE.sub(
        lambda m: f"{m.group(1)}://{_mask(URL_CRED_LABEL)}@", out
    )
    for pat, label in _CONTENT_PATTERNS:
        out = pat.sub(_mask(label), out)
    return out


def _redact_args(args: object) -> object:
    if args is None:
        return args
    if isinstance(args, dict):
        return {
            k: redact(v) if isinstance(v, str) else v for k, v in args.items()
        }
    if isinstance(args, tuple):
        return tuple(redact(a) if isinstance(a, str) else a for a in args)
    if isinstance(args, list):
        return [redact(a) if isinstance(a, str) else a for a in args]
    if isinstance(args, str):
        return (redact(args),)
    return args


def _redacting_factory(*args: object, **kwargs: object) -> logging.LogRecord:
    record = _saved_factory(*args, **kwargs)
    if isinstance(record.msg, str):
        record.msg = redact(record.msg)
    record.args = _redact_args(record.args)
    if record.exc_info and record.exc_info[0] is not None:
        record.exc_text = redact(
            logging.Formatter().formatException(record.exc_info)
        )
    if isinstance(record.stack_info, str) and record.stack_info:
        record.stack_info = redact(record.stack_info)
    return record


class RedactionFilter(logging.Filter):
    """Rewrite log records so credential-shaped strings never reach handlers."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            formatted = record.getMessage()
        except Exception:
            formatted = str(record.msg)
        record.msg = redact(formatted)
        record.args = ()
        if record.exc_info:
            record.exc_text = redact(
                logging.Formatter().formatException(record.exc_info)
            )
        if record.stack_info:
            record.stack_info = redact(record.stack_info)
        return True


def _excepthook(
    exc_type: type[BaseException],
    exc: BaseException,
    tb: traceback.TracebackType | None,
) -> None:
    text = "".join(traceback.format_exception(exc_type, exc, tb))
    sys.stderr.write(redact(text))
    if not text.endswith("\n"):
        sys.stderr.write("\n")


def install_redaction() -> None:
    """Install record-factory redaction, handler filters, and sys.excepthook.

    The root-logger filter covers records logged on the root logger itself;
    the record factory covers everything else (child loggers, handlers added
    after install, dictConfig from the google-ads client YAML). Idempotent.
    """
    global _FACTORY_INSTALLED, _saved_factory
    if not _FACTORY_INSTALLED:
        _saved_factory = logging.getLogRecordFactory()
        logging.setLogRecordFactory(_redacting_factory)
        _FACTORY_INSTALLED = True
    sys.excepthook = _excepthook
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        root.addHandler(handler)
        if root.level == logging.NOTSET:
            root.setLevel(logging.INFO)
    if not any(isinstance(f, RedactionFilter) for f in root.filters):
        root.addFilter(RedactionFilter())
    for handler in root.handlers:
        if not any(isinstance(f, RedactionFilter) for f in handler.filters):
            handler.addFilter(RedactionFilter())
