"""Entry point: python -m pgqueuer.adapters.mcp"""

from __future__ import annotations

from pgqueuer.adapters.mcp.server import create_mcp_server


def main() -> None:
    server = create_mcp_server()
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
