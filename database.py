"""
Database utility module for SQL Server MCP
Provides enhanced database connection management and utilities
"""

import os
import logging
from typing import Any, Dict, List, Optional, Union, Tuple
import pyodbc
from contextlib import contextmanager
from datetime import datetime, date
import json

logger = logging.getLogger(__name__)

class SQLServerConfig:
    """SQL Server configuration management"""
    
    def __init__(self):
        self.server = os.getenv("DB_SERVER", "localhost")
        self.database = os.getenv("DB_DATABASE", "master")
        self.username = os.getenv("DB_USERNAME", "")
        self.password = os.getenv("DB_PASSWORD", "")
        self.connection_string = os.getenv("DB_CONNECTION_STRING", "")
        self.connection_timeout = int(os.getenv("CONNECTION_TIMEOUT", "30"))
        self.query_timeout = int(os.getenv("QUERY_TIMEOUT", "30"))
        
    def get_connection_string(self) -> str:
        """Build connection string based on available configuration"""
        if self.connection_string:
            return self.connection_string
            
        driver = "{ODBC Driver 17 for SQL Server}"
        
        if self.username and self.password:
            # SQL Server Authentication
            return (
                f"Driver={driver};"
                f"Server=tcp:{self.server},1433;"
                f"Database={self.database};"
                f"Uid={self.username};"
                f"Pwd={self.password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout={self.connection_timeout};"
                f"Command Timeout={self.query_timeout};"
            )
        else:
            # Windows Authentication (local development)
            return (
                f"Driver={driver};"
                f"Server={self.server};"
                f"Database={self.database};"
                f"Trusted_Connection=yes;"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout={self.connection_timeout};"
                f"Command Timeout={self.query_timeout};"
            )

class DatabaseConnection:
    """Enhanced database connection management"""
    
    def __init__(self, config: SQLServerConfig = None):
        self.config = config or SQLServerConfig()
        self._connection = None
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = pyodbc.connect(self.config.get_connection_string())
            conn.timeout = self.config.query_timeout
            logger.debug("Database connection established")
            yield conn
            
        except pyodbc.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected database error: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                    logger.debug("Database connection closed")
                except:
                    pass

class QueryExecutor:
    """Enhanced query execution with safety features"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.max_rows = int(os.getenv("MAX_ROWS_RETURNED", "1000"))
        self.enable_write_ops = os.getenv("ENABLE_WRITE_OPERATIONS", "false").lower() == "true"
        
    def execute_select_query(
        self, 
        query: str, 
        params: Optional[Tuple] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Execute SELECT query with safety checks"""
        
        # Validate query is SELECT only
        query_upper = query.strip().upper()
        if not query_upper.startswith('SELECT'):
            raise ValueError("Only SELECT queries are allowed")
            
        # Apply row limit
        effective_limit = min(limit or self.max_rows, self.max_rows)
        
        # Add TOP clause if not present
        if 'TOP ' not in query_upper and 'LIMIT ' not in query_upper:
            if query_upper.startswith('SELECT DISTINCT'):
                query = f"SELECT DISTINCT TOP {effective_limit} " + query[15:]
            else:
                query = f"SELECT TOP {effective_limit} " + query[6:]
        
        with self.db_connection.get_connection() as conn:
            cursor = conn.cursor()
            
            start_time = datetime.now()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Get column information
            columns = [column[0] for column in cursor.description] if cursor.description else []
            
            # Fetch and serialize results
            results = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, value in enumerate(row):
                    if isinstance(value, (datetime, date)):
                        row_dict[columns[i]] = value.isoformat()
                    elif isinstance(value, bytes):
                        # Handle binary data
                        try:
                            row_dict[columns[i]] = value.decode('utf-8')
                        except UnicodeDecodeError:
                            row_dict[columns[i]] = f"<binary data: {len(value)} bytes>"
                    else:
                        row_dict[columns[i]] = value
                results.append(row_dict)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Query executed in {execution_time:.3f}s, returned {len(results)} rows")
            
            return results
    
    def execute_non_query(
        self, 
        query: str, 
        params: Optional[Tuple] = None
    ) -> int:
        """Execute non-SELECT query (if enabled)"""
        
        if not self.enable_write_ops:
            raise ValueError("Write operations are disabled for security")
            
        query_upper = query.strip().upper()
        allowed_ops = ['INSERT', 'UPDATE', 'DELETE', 'EXEC', 'EXECUTE']
        
        if not any(query_upper.startswith(op) for op in allowed_ops):
            raise ValueError(f"Only {', '.join(allowed_ops)} operations are allowed")
        
        with self.db_connection.get_connection() as conn:
            cursor = conn.cursor()
            
            start_time = datetime.now()
            
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                affected_rows = cursor.rowcount
                conn.commit()
                
                execution_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"Non-query executed in {execution_time:.3f}s, affected {affected_rows} rows")
                
                return affected_rows
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Non-query execution failed: {e}")
                raise

class SchemaInspector:
    """Database schema inspection utilities"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
    
    def get_tables(self, schema: str = 'dbo') -> List[Dict[str, str]]:
        """Get list of tables with metadata"""
        query = """
        SELECT 
            TABLE_SCHEMA,
            TABLE_NAME,
            TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_SCHEMA = ISNULL(?, TABLE_SCHEMA)
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        
        with self.db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (schema,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'schema': row.TABLE_SCHEMA,
                    'name': row.TABLE_NAME,
                    'type': row.TABLE_TYPE
                })
            
            return results
    
    def get_table_columns(self, table_name: str, schema: str = 'dbo') -> List[Dict[str, Any]]:
        """Get detailed column information for a table"""
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
        ORDER BY ORDINAL_POSITION
        """
        
        with self.db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (table_name, schema))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'column_name': row.COLUMN_NAME,
                    'data_type': row.DATA_TYPE,
                    'is_nullable': row.IS_NULLABLE == 'YES',
                    'default_value': row.COLUMN_DEFAULT,
                    'max_length': row.CHARACTER_MAXIMUM_LENGTH,
                    'precision': row.NUMERIC_PRECISION,
                    'scale': row.NUMERIC_SCALE,
                    'position': row.ORDINAL_POSITION
                })
            
            return results
    
    def get_stored_procedures(self, schema: str = 'dbo') -> List[Dict[str, str]]:
        """Get list of stored procedures"""
        query = """
        SELECT 
            ROUTINE_SCHEMA,
            ROUTINE_NAME,
            ROUTINE_TYPE
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'PROCEDURE'
        AND ROUTINE_SCHEMA = ISNULL(?, ROUTINE_SCHEMA)
        ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
        """
        
        with self.db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (schema,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'schema': row.ROUTINE_SCHEMA,
                    'name': row.ROUTINE_NAME,
                    'type': row.ROUTINE_TYPE
                })
            
            return results

# Factory functions for easy usage
def create_database_connection() -> DatabaseConnection:
    """Factory function to create database connection"""
    config = SQLServerConfig()
    return DatabaseConnection(config)

def create_query_executor() -> QueryExecutor:
    """Factory function to create query executor"""
    db_connection = create_database_connection()
    return QueryExecutor(db_connection)

def create_schema_inspector() -> SchemaInspector:
    """Factory function to create schema inspector"""
    db_connection = create_database_connection()
    return SchemaInspector(db_connection)