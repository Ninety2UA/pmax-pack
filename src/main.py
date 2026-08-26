"""pMax Performance Pack -- dependency-free boot entry.

`python3 src/main.py` must run and exit 0 on a bare Python 3 install.
With no arguments this file prints the display name and does not import
`pmax_pack`. With arguments it lazily imports `pmax_pack.cli` and delegates.
"""
from __future__ import annotations

import sys


def main() -> int:
    if len(sys.argv) <= 1:
        print("pMax Performance Pack is alive (base profile scaffold).")
        return 0
    from pmax_pack.cli import main as cli_main

    return cli_main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
