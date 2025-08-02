import os
import json
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
import sqlglot
from sqlglot import exp

class DBTLineageTracer:
    def __init__(self, compiled_sql_directory: str, internal_db_prefixes: List[str] = None):
        """
        Initialize the DBT lineage tracer with compiled SQL directory
        
        Args:
            compiled_sql_directory: Path to dbt compiled SQL files
            internal_db_prefixes: List of database prefixes that indicate internal tables (e.g., ['ph_'])
        """
        self.sql_dir = Path(compiled_sql_directory)
        self.internal_db_prefixes = internal_db_prefixes or ['ph_']
        self.table_to_file_map = {}  # "table_name" -> Path object
        self.file_cache = {}         # Cache parsed SQL content
        
        # Build the file mapping on initialization
        self.build_file_mapping()
        
    def build_file_mapping(self) -> Dict[str, Path]:
        """
        Walk the compiled SQL directory and build table name -> file path mapping
        """
        print(f"Scanning directory: {self.sql_dir}")
        
        if not self.sql_dir.exists():
            raise FileNotFoundError(f"Directory does not exist: {self.sql_dir}")
        
        file_count = 0
        for sql_file in self.sql_dir.rglob("*.sql"):
            table_name = sql_file.stem
            self.table_to_file_map[table_name] = sql_file
            file_count += 1
            
        print(f"Found {file_count} SQL files")
        return self.table_to_file_map
    
    def extract_table_name_from_full_ref(self, full_table_name: str) -> str:
        """
        Extract the table name from a fully qualified table reference
        
        Args:
            full_table_name: e.g., "database.schema.table_name"
            
        Returns:
            Table name: e.g., "table_name"
        """
        return full_table_name.split('.')[-1]
    
    def is_source_table(self, full_table_name: str) -> Tuple[bool, str]:
        """
        Determine if a table is a source table:
        1. Database name does NOT start with internal prefixes
        2. OR table file doesn't exist in our project
        
        Args:
            full_table_name: Fully qualified table name
            
        Returns:
            Tuple of (is_source, reason)
        """
        # Check for external database (doesn't start with internal prefixes)
        starts_with_internal = any(full_table_name.startswith(prefix) for prefix in self.internal_db_prefixes)
        if not starts_with_internal:
            return True, "external_database"
            
        # Check if file exists in our project
        table_name = self.extract_table_name_from_full_ref(full_table_name)
        if table_name not in self.table_to_file_map:
            return True, "missing_file"
            
        return False, "internal_table"
    
    def load_sql_file(self, table_name: str) -> Optional[str]:
        """
        Load and cache SQL content for a given table
        """
        if table_name in self.file_cache:
            return self.file_cache[table_name]
            
        if table_name not in self.table_to_file_map:
            return None
            
        file_path = self.table_to_file_map[table_name]
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
                self.file_cache[table_name] = sql_content
                return sql_content
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            return None
    
    def trace_column_lineage_across_files(self, presentation_table: str, target_column: str, visited: Optional[Set[str]] = None) -> Dict:
        """
        Main entry point: Trace a specific column from presentation layer back to all source tables
        
        Args:
            presentation_table: Name of the presentation table
            target_column: Name of the column to trace
            visited: Set of tables already visited (to prevent circular references)
            
        Returns:
            Complete lineage information for the target column
        """
        if visited is None:
            visited = set()
            
        if presentation_table in visited:
            return {"error": f"Circular reference detected: {presentation_table}"}
            
        visited.add(presentation_table)
        
        print(f"\n🔍 Tracing column '{target_column}' in table '{presentation_table}'")
        
        # Load and analyze the SQL file
        sql_content = self.load_sql_file(presentation_table)
        if not sql_content:
            # If file doesn't exist, treat as source
            print(f"✅ Found source table: {presentation_table} (missing_file)")
            return {
                "table": presentation_table,
                "column": target_column,
                "type": "source",
                "reason": "missing_file",
                "lineage_chain": []
            }
            
        try:
            # Import your existing trace function
            from column_lineage import trace_column_lineage
            
            # Trace the column within this single file
            single_file_trace = trace_column_lineage(sql_content, target_column)
            
            if "error" in single_file_trace:
                return single_file_trace
                
            print(f"📊 Found {len(single_file_trace.get('next_columns_to_search', []))} dependencies")
            
            # Now recursively trace each dependency
            upstream_lineage = []
            
            for dep in single_file_trace.get('next_columns_to_search', []):
                dep_table = dep['table']
                dep_column = dep['column']
                
                # Check if this dependency is a source table
                is_source, reason = self.is_source_table(dep_table)
                
                if is_source:
                    print(f"   ✅ Found source: {dep_table}.{dep_column} ({reason})")
                    upstream_lineage.append({
                        "dependency": dep,
                        "upstream_trace": {
                            "table": dep_table,
                            "column": dep_column,
                            "type": "source",
                            "reason": reason,
                            "lineage_chain": []
                        }
                    })
                else:
                    # Extract table name and recursively trace
                    dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                    print(f"   ⬆️  Tracing upstream: {dep_table}.{dep_column}")
                    
                    upstream_trace = self.trace_column_lineage_across_files(
                        dep_table_name, 
                        dep_column, 
                        visited.copy()
                    )
                    
                    upstream_lineage.append({
                        "dependency": dep,
                        "upstream_trace": upstream_trace
                    })
            
            return {
                "table": presentation_table,
                "column": target_column,
                "type": "intermediate",
                "current_file_analysis": single_file_trace,
                "upstream_lineage": upstream_lineage,
                "sql_file": str(self.table_to_file_map.get(presentation_table, "Unknown"))
            }
            
        except Exception as e:
            return {"error": f"Error tracing column in {presentation_table}: {str(e)}"}


def print_lineage_results(result: Dict, level: int = 0):
    """
    Pretty print the lineage results in a tree structure
    """
    indent = "  " * level
    
    if result.get("type") == "source":
        print(f"{indent}🔚 SOURCE: {result['table']}.{result['column']} ({result['reason']})")
        return
        
    print(f"{indent}📁 {result['table']}.{result['column']}")
    
    if result.get("upstream_lineage"):
        for upstream in result["upstream_lineage"]:
            dep = upstream["dependency"]
            print(f"{indent}  ├─ {dep.get('context', 'dependency')}")
            print(f"{indent}  │  ({dep['table']}.{dep['column']})")
            
            upstream_trace = upstream["upstream_trace"]
            if "error" not in upstream_trace:
                print_lineage_results(upstream_trace, level + 2)
            else:
                print(f"{indent}    ❌ {upstream_trace['error']}")


def test_available_tables(compiled_sql_directory: str):
    """
    Quick test to see what tables are available for testing
    """
    print("="*60)
    print("AVAILABLE TABLES FOR TESTING")
    print("="*60)
    
    try:
        tracer = DBTLineageTracer(compiled_sql_directory)
        
        tables = list(tracer.table_to_file_map.keys())
        print(f"Found {len(tables)} tables:")
        
        # Show first 20 tables
        for i, table in enumerate(tables[:20]):
            print(f"  - {table}")
        
        if len(tables) > 20:
            print(f"  ... and {len(tables) - 20} more")
        
        print(f"\n💡 To test lineage, pick any table name from above")
        
    except Exception as e:
        print(f"Error: {e}")


def test_complete_column_lineage(compiled_sql_directory: str, presentation_table: str, target_column: str, internal_db_prefixes: List[str] = None):
    """
    Test the complete column lineage tracing from presentation to source
    """
    print("="*80)
    print("TESTING COMPLETE COLUMN LINEAGE TRACING")
    print("="*80)
    
    try:
        tracer = DBTLineageTracer(compiled_sql_directory, internal_db_prefixes)
        
        print(f"🎯 Starting lineage trace:")
        print(f"   Table: {presentation_table}")
        print(f"   Column: {target_column}")
        print(f"   Internal DB prefixes: {tracer.internal_db_prefixes}")
        print(f"   Compiled files available: {len(tracer.table_to_file_map)}")
        
        # Run the complete lineage trace
        lineage_result = tracer.trace_column_lineage_across_files(presentation_table, target_column)
        
        print(f"\n📋 LINEAGE TRACE RESULTS:")
        print("="*50)
        
        if "error" in lineage_result:
            print(f"❌ Error: {lineage_result['error']}")
        else:
            print_lineage_results(lineage_result, level=0)
            
    except FileNotFoundError as e:
        print(f"❌ Directory not found: {e}")
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()


# Example usage
if __name__ == "__main__":
    # UPDATE THESE VALUES FOR YOUR TEST
    compiled_sql_directory = "compiled"  # UPDATE THIS PATH
    presentation_table = "fct_customer_orders"                # UPDATE: Your presentation table name  
    target_column = "customer_id"                     # UPDATE: Column you want to trace
    internal_db_prefixes = ['ph_']                         # UPDATE: Your internal database prefixes
    
    # First check what tables are available
    test_available_tables(compiled_sql_directory)
    print("\n")
    
    # Then run the complete lineage trace  
    test_complete_column_lineage(compiled_sql_directory, presentation_table, target_column, internal_db_prefixes)