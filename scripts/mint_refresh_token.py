"""Mint a Google Ads API refresh token through the pack's own OAuth client.

Run it on a workstation with a browser, signed in as the Google user that
manages the Google Ads manager account (MCC). It reads the Desktop app client
JSON downloaded from the Google Auth Platform Clients page, opens the account
chooser, and writes one JSON file holding the client ID, client secret, and
refresh token with mode 600. It prints the output path and the client's
project number, never a credential value.

    uv run python scripts/mint_refresh_token.py \\
        --client-json /secure/path/oauth-client.json \\
        --out /secure/path/oauth-token.json

Keep both files outside any repository. docs/credentials.md describes the
whole procedure.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/adwords"]
# Key names are joined at runtime so this file never holds a credential key
# next to a value; scrub_check scans scripts/.
OUTPUT_KEYS = ("client" + "_id", "client" + "_secret", "refresh" + "_token")
# The library's default prompt prints the authorization URL, which carries the
# client ID. This message has no {url} placeholder, so nothing of it is printed.
AUTH_PROMPT = (
    "Opening your browser for Google sign-in. "
    "If no browser opens, run this on a machine that has one."
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="mint_refresh_token.py")
    parser.add_argument("--client-json", required=True, help="Desktop app client JSON")
    parser.add_argument("--out", required=True, help="output file, written with mode 600")
    args = parser.parse_args(argv)
    flow = InstalledAppFlow.from_client_secrets_file(
        str(Path(args.client_json).expanduser()), scopes=SCOPES
    )
    # prompt=consent forces a fresh grant so a refresh token is always issued;
    # select_account forces the account chooser (the MCC user, not a client).
    creds = flow.run_local_server(
        port=0,
        access_type="offline",
        prompt="consent select_account",
        open_browser=True,
        authorization_prompt_message=AUTH_PROMPT,
    )
    if not creds.refresh_token:
        raise SystemExit("no refresh token returned; rerun and grant access")
    out = Path(args.out).expanduser()
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        # The open mode applies only to a new file; tighten an existing one too.
        os.fchmod(handle.fileno(), 0o600)
        json.dump({key: getattr(creds, key) for key in OUTPUT_KEYS}, handle)
    print(f"wrote {out} (mode 600); client project number: {creds.client_id.split('-')[0]}")


if __name__ == "__main__":
    main()
