import asyncio
import contextlib

from . import cli

if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(cli.main())
