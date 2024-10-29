import asyncio
import contextlib

from . import cli


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(cli.main())


if __name__ == "__main__":
    main()
