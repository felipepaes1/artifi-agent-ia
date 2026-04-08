from fastmcp import FastMCP

from .observability import configure_logging
from .settings import MCP_NAME, MCP_PORT, MCP_TRANSPORT
from .tools import register_calendar_tools, register_messaging_tools


configure_logging()
mcp = FastMCP(MCP_NAME)
register_messaging_tools(mcp)
register_calendar_tools(mcp)


if __name__ == "__main__":
    mcp.run(transport=MCP_TRANSPORT, host="0.0.0.0", port=MCP_PORT)
