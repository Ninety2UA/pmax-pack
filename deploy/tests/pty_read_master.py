"""Single source for the PTY drivers' read_master helper.

Both TTY drivers in test_deploy_review.sh exec this file; it defines
read_master() over their global `master`, `os`, and `errno`.
"""


def read_master() -> bytes:
    # Linux raises EIO on the PTY master once the child has closed the slave;
    # macOS returns an empty read. Both mean end of output for this driver.
    try:
        return os.read(master, 4096)  # noqa: F821 - driver globals
    except OSError as exc:
        if exc.errno == errno.EIO:  # noqa: F821 - driver globals
            return b""
        raise
