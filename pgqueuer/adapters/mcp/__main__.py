from __future__ import annotations

import argparse

from pgqueuer.adapters.mcp.server import create_mcp_server


def main() -> None:
    parser = argparse.ArgumentParser(description="PgQueuer MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default="stdio",
        help="MCP transport (default: stdio)",
    )
    args = parser.parse_args()
    server = create_mcp_server()
    server.run(transport=args.transport)


if __name__ == "__main__":
    main()
