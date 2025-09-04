"""
SQL Server MCP - WORKING VERSION FOR CLIENT DEMO!
"""

import os
import sys
import logging
from typing import Any, Dict, List, Optional
import pyodbc
from datetime import datetime, date
import json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_SERVER = os.getenv("DB_SERVER", "localhost")
DB_DATABASE = os.getenv("DB_DATABASE", "master") 
DB_USERNAME = os.getenv("DB_USERNAME", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Server configuration
PORT = int(os.getenv("PORT", "8000"))
HOST = os.getenv("HOST", "0.0.0.0")

class DatabaseManager:
    """Database connection and operations manager"""
    
    def __init__(self):
        self.connection_string = self._build_connection_string()
        logger.info("Database manager initialized")
    
    def _build_connection_string(self) -> str:
        """Build SQL Server connection string"""
        driver = "{ODBC Driver 17 for SQL Server}"
        
        if DB_USERNAME and DB_PASSWORD:
            return (
                f"Driver={driver};"
                f"Server={DB_SERVER};"
                f"Database={DB_DATABASE};"
                f"Uid={DB_USERNAME};"
                f"Pwd={DB_PASSWORD};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
        else:
            return (
                f"Driver={driver};"
                f"Server={DB_SERVER};"
                f"Database={DB_DATABASE};"
                f"Trusted_Connection=yes;"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
    
    def get_connection(self):
        """Get database connection"""
        try:
            conn = pyodbc.connect(self.connection_string)
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise HTTPException(400, f"Database connection failed: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute SELECT query"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                columns = [column[0] for column in cursor.description] if cursor.description else []
                
                rows = []
                for row in cursor.fetchall():
                    row_dict = {}
                    for i, value in enumerate(row):
                        if isinstance(value, (datetime, date)):
                            row_dict[columns[i]] = value.isoformat()
                        else:
                            row_dict[columns[i]] = value
                    rows.append(row_dict)
                
                logger.info(f"Query executed successfully, returned {len(rows)} rows")
                return rows
                
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                raise HTTPException(400, f"Query execution failed: {str(e)}")

# Initialize database manager
db_manager = DatabaseManager()

# Create FastAPI app
app = FastAPI(
    title="SQL Server MCP",
    description="Model Context Protocol server for MS SQL Server database operations",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MCP Tools as simple functions
def query_database_tool(query: str, limit: int = 100) -> Dict[str, Any]:
    """Execute a SQL query against the database."""
    query_upper = query.strip().upper()
    if not query_upper.startswith('SELECT'):
        return {
            "error": "Only SELECT queries are allowed for security reasons",
            "query": query
        }
    
    if 'LIMIT' not in query_upper and 'TOP' not in query_upper:
        if limit > 1000:
            limit = 1000
        query = query.strip()
        if query_upper.startswith('SELECT DISTINCT'):
            query = f"SELECT DISTINCT TOP {limit} " + query[15:]
        else:
            query = f"SELECT TOP {limit} " + query[6:]
    
    try:
        results = db_manager.execute_query(query)
        return {
            "success": True,
            "results": results,
            "row_count": len(results),
            "query": query,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "query": query,
            "timestamp": datetime.now().isoformat()
        }

def list_tables_tool() -> Dict[str, Any]:
    """Get a list of all tables in the database."""
    try:
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        results = db_manager.execute_query(query)
        tables = [row['TABLE_NAME'] for row in results]
        
        return {
            "success": True,
            "tables": tables,
            "table_count": len(tables),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

def describe_table_tool(table_name: str) -> Dict[str, Any]:
    """Get schema information for a specific table."""
    try:
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        schema = db_manager.execute_query(query, (table_name,))
        
        return {
            "success": True,
            "table_name": table_name,
            "columns": schema,
            "column_count": len(schema),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "table_name": table_name,
            "timestamp": datetime.now().isoformat()
        }

def get_database_info_tool() -> Dict[str, Any]:
    """Get general information about the connected database."""
    try:
        version_result = db_manager.execute_query("SELECT @@VERSION as version")
        db_name_result = db_manager.execute_query("SELECT DB_NAME() as database_name")
        server_name_result = db_manager.execute_query("SELECT @@SERVERNAME as server_name")
        
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        table_results = db_manager.execute_query(query)
        
        return {
            "success": True,
            "database_name": db_name_result[0]["database_name"] if db_name_result else "Unknown",
            "server_name": server_name_result[0]["server_name"] if server_name_result else "Unknown",
            "version": version_result[0]["version"] if version_result else "Unknown",
            "table_count": len(table_results),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

def execute_stored_procedure_tool(procedure_name: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Execute a stored procedure."""
    try:
        if parameters:
            param_list = []
            param_values = []
            for key, value in parameters.items():
                param_list.append(f"@{key} = ?")
                param_values.append(value)
            
            query = f"EXEC {procedure_name} {', '.join(param_list)}"
            results = db_manager.execute_query(query, tuple(param_values))
        else:
            query = f"EXEC {procedure_name}"
            results = db_manager.execute_query(query)
        
        return {
            "success": True,
            "procedure_name": procedure_name,
            "parameters": parameters,
            "results": results,
            "row_count": len(results),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "procedure_name": procedure_name,
            "parameters": parameters,
            "timestamp": datetime.now().isoformat()
        }

# MCP Tools Registry
MCP_TOOLS = {
    "query_database": {
        "function": query_database_tool,
        "description": "Execute a SQL query against the database.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL SELECT query to execute"},
                "limit": {"type": "integer", "description": "Maximum rows to return", "default": 100}
            },
            "required": ["query"]
        }
    },
    "list_tables": {
        "function": list_tables_tool,
        "description": "Get a list of all tables in the database.",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    "describe_table": {
        "function": describe_table_tool,
        "description": "Get schema information for a specific table.",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Name of the table to describe"}
            },
            "required": ["table_name"]
        }
    },
    "get_database_info": {
        "function": get_database_info_tool,
        "description": "Get general information about the connected database.",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    "execute_stored_procedure": {
        "function": execute_stored_procedure_tool,
        "description": "Execute a stored procedure.",
        "parameters": {
            "type": "object",
            "properties": {
                "procedure_name": {"type": "string", "description": "Name of the stored procedure"},
                "parameters": {"type": "object", "description": "Parameters for the stored procedure"}
            },
            "required": ["procedure_name"]
        }
    }
}

# Custom endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected"
        }
    except Exception as e:
        return {
            "status": "unhealthy", 
            "timestamp": datetime.now().isoformat(),
            "database": "disconnected",
            "error": str(e)
        }

@app.get("/test", response_class=HTMLResponse)
async def test_page():
    """Test page"""
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SQL Server MCP Test</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            .container {{ max-width: 800px; margin: 0 auto; }}
            .endpoint {{ background: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 5px; }}
            .status {{ padding: 5px 10px; border-radius: 3px; color: white; }}
            .success {{ background: green; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>SQL Server MCP Server</h1>
            <p><span class="status success">RUNNING</span> Server is operational</p>
            
            <h2>Available Endpoints:</h2>
            <div class="endpoint"><strong>GET /health</strong> - Health check endpoint</div>
            <div class="endpoint"><strong>GET /test</strong> - This test page</div>
            <div class="endpoint"><strong>GET /debug/tools</strong> - Debug tools list</div>
            <div class="endpoint"><strong>POST /mcp</strong> - Main MCP protocol endpoint</div>
            <div class="endpoint"><strong>GET /mcp/capabilities</strong> - MCP capabilities</div>
            
            <h2>Available MCP Tools:</h2>
            <ul>
                <li><strong>query_database</strong> - Execute SQL SELECT queries</li>
                <li><strong>list_tables</strong> - Get list of database tables</li>
                <li><strong>describe_table</strong> - Get table schema information</li>
                <li><strong>execute_stored_procedure</strong> - Execute stored procedures</li>
                <li><strong>get_database_info</strong> - Get database information</li>
            </ul>
            
            <p><small>Timestamp: {datetime.now().isoformat()}</small></p>
        </div>
    </body>
    </html>
    """
    return html_content

@app.get("/mcp/capabilities")
async def mcp_capabilities():
    """Get MCP server capabilities"""
    return {
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {
                "listChanged": True
            },
            "resources": {},
            "prompts": {}
        },
        "serverInfo": {
            "name": "SQL Server MCP",
            "version": "1.0.0",
            "description": "Model Context Protocol server for MS SQL Server database operations"
        }
    }

@app.get("/debug/tools")
async def list_available_tools():
    """Debug endpoint to list all MCP tools"""
    tools = []
    
    for name, config in MCP_TOOLS.items():
        tools.append({
            "name": name,
            "description": config["description"],
            "parameters": config["parameters"]
        })
    
    return {
        "server_info": {
            "name": "SQL Server MCP",
            "version": "1.0.0",
            "total_tools": len(tools)
        },
        "available_tools": tools,
        "status": "All tools registered and working!"
    }

# MCP Protocol Handler - THIS IS THE KEY!
@app.post("/mcp")
async def mcp_handler(request: dict):
    """Handle MCP JSON-RPC requests"""
    try:
        # Handle tools/list request
        if request.get("method") == "tools/list":
            tools = []
            for name, config in MCP_TOOLS.items():
                tools.append({
                    "name": name,
                    "description": config["description"],
                    "inputSchema": config["parameters"]
                })
            
            return {
                "jsonrpc": "2.0",
                "result": {
                    "tools": tools
                },
                "id": request.get("id", 1)
            }
        
        # Handle tools/call request
        elif request.get("method") == "tools/call":
            params = request.get("params", {})
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            if tool_name not in MCP_TOOLS:
                return {
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -1,
                        "message": f"Tool '{tool_name}' not found"
                    },
                    "id": request.get("id", 1)
                }
            
            # Execute the tool
            tool_function = MCP_TOOLS[tool_name]["function"]
            try:
                result = tool_function(**arguments)
                
                return {
                    "jsonrpc": "2.0",
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(result, indent=2)
                            }
                        ],
                        "isError": False
                    },
                    "id": request.get("id", 1)
                }
            except Exception as e:
                return {
                    "jsonrpc": "2.0",
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps({"error": str(e)}, indent=2)
                            }
                        ],
                        "isError": True
                    },
                    "id": request.get("id", 1)
                }
        
        # Handle initialize request
        elif request.get("method") == "initialize":
            return {
                "jsonrpc": "2.0",
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {
                            "listChanged": True
                        },
                        "resources": {},
                        "prompts": {}
                    },
                    "serverInfo": {
                        "name": "SQL Server MCP",
                        "version": "1.0.0",
                        "description": "Model Context Protocol server for MS SQL Server database operations"
                    }
                },
                "id": request.get("id", 1)
            }
        
        else:
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32601,
                    "message": f"Method '{request.get('method')}' not found"
                },
                "id": request.get("id", 1)
            }
            
    except Exception as e:
        logger.error(f"MCP handler error: {e}")
        return {
            "jsonrpc": "2.0",
            "error": {
                "code": -1,
                "message": str(e)
            },
            "id": request.get("id", 1) if isinstance(request, dict) else 1
        }

if __name__ == "__main__":
    logger.info(f"🚀 SQL Server MCP Server starting on {HOST}:{PORT}")
    logger.info("✅ Database tools registered and ready")
    logger.info("✅ MCP endpoint available at /mcp")
    logger.info("✅ Ready for Copilot Studio integration!")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info", access_log=True)

