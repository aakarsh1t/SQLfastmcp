import requests
import json

def test_mcp_tool(tool_name, arguments=None):
    """Test MCP tools via JSON-RPC"""
    url = "http://localhost:8000/mcp"
    
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments or {}
        },
        "id": 1
    }
    
    try:
        response = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        return response.json()
    except Exception as e:
        return {"error": f"Request failed: {str(e)}"}

# Test database info
print("🔍 Testing get_database_info...")
result = test_mcp_tool("get_database_info")
print(json.dumps(result, indent=2))

# Test list tables  
print("\n📋 Testing list_tables...")
result = test_mcp_tool("list_tables")
print(json.dumps(result, indent=2))

# Test query
print("\n🔎 Testing query_database...")
result = test_mcp_tool("query_database", {
    "query": "SELECT TOP 3 TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'",
    "limit": 3
})
print(json.dumps(result, indent=2))

print("\n✅ Tests completed!")
