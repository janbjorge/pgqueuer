import contextlib

from pgqueuer import cli


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        cli.app(prog_name="pgqueuer")


if __name__ == "__main__":
    main()
