"""pmax-pack CLI skeleton.

Subcommands exist with their U1 flags and exit 2 with a clear
"not implemented in U1" message. Redaction is installed first, then
parse, then dispatch through HANDLERS inside a try/except that never
emits raw exception text.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from collections.abc import Callable

from pmax_pack.redact import install_redaction, redact

NOT_IMPLEMENTED = "not implemented in U1"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pmax-pack",
        description="Performance Max data engine (gaarf to BigQuery marts).",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("run", help="run the daily extraction and marts pipeline")

    p_backfill = sub.add_parser("backfill", help="backfill one account")
    p_backfill.add_argument("--account", help="10-digit customer id")

    p_rebuild = sub.add_parser("rebuild", help="rebuild marts as of a date")
    p_rebuild.add_argument("--as-of", help="ISO date to rebuild as of")
    p_rebuild.add_argument("--target-dataset", help="destination dataset")
    p_rebuild.add_argument(
        "--dry-run",
        action="store_true",
        help="plan the rebuild without writing",
    )

    p_parity = sub.add_parser("parity", help="run the parity harness")
    p_parity.add_argument(
        "--source",
        choices=("live", "fixtures"),
        help="parity source",
    )
    p_parity.add_argument("--account", help="10-digit customer id")
    p_parity.add_argument("--date", help="ISO date")

    p_report = sub.add_parser("report", help="write or fetch a validation report")
    p_report.add_argument("--run-id", help="run identifier")

    p_probe = sub.add_parser("probe", help="probe a credential against an account")
    p_probe.add_argument(
        "--credential-file",
        help="path to a Google Ads YAML credential file",
    )
    p_probe.add_argument("--account", help="10-digit customer id")

    return parser


def _not_implemented(args: argparse.Namespace) -> int:
    print(f"{args.command}: {NOT_IMPLEMENTED}", file=sys.stderr)
    return 2


def _probe(args: argparse.Namespace) -> int:
    from pmax_pack.ads_client import probe
    from pmax_pack.config import DEFAULT_API_VERSION

    if not args.credential_file or not args.account:
        print(
            "probe: --credential-file and --account are required",
            file=sys.stderr,
        )
        return 2
    row = probe(args.credential_file, args.account, DEFAULT_API_VERSION)
    print(json.dumps(row, default=str))
    return 0


HANDLERS: dict[str, Callable[[argparse.Namespace], int]] = {
    "run": _not_implemented,
    "backfill": _not_implemented,
    "rebuild": _not_implemented,
    "parity": _not_implemented,
    "report": _not_implemented,
    "probe": _probe,
}


def main(argv: list[str] | None = None) -> int:
    install_redaction()
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 2
    log = logging.getLogger("pmax_pack.cli")
    try:
        handler = HANDLERS[args.command]
        return handler(args)
    except Exception as exc:
        log.error(redact(str(exc)))
        log.debug(redact(traceback.format_exc()))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
