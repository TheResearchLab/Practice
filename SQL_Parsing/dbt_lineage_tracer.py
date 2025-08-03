import os
import json
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
import sqlglot
from sqlglot import exp

class DBTLineageTracer:
    def __init__(self, compiled_sql_directory: str, internal_db_prefixes: List[str] = None, source_definitions_file: Optional[str] = None, manifest_path: Optional[str] = None):
        """
        Initialize the DBT lineage tracer with compiled SQL directory
        
        Args:
            compiled_sql_directory: Path to dbt compiled SQL files
            internal_db_prefixes: List of database prefixes that indicate internal tables (e.g., ['ph_'])
            source_definitions_file: Path to JSON file containing source column definitions
            manifest_path: Path to DBT manifest.json file (default: target/manifest.json)
        """
        self.sql_dir = Path(compiled_sql_directory)
        self.internal_db_prefixes = internal_db_prefixes or ['ph_']
        self.table_to_file_map = {}  # "table_name" -> Path object
        self.file_cache = {}         # Cache parsed SQL content
        self.source_definitions = {}  # Source column definitions
        self.manifest_data = {}      # Manifest data storage
        
        # Load source definitions if provided
        if source_definitions_file:
            self.load_source_definitions(source_definitions_file)
        
        # Load manifest - default path or provided path
        manifest_file = manifest_path or "target/manifest.json"
        self._load_manifest(manifest_file)
        
        # Build the file mapping on initialization
        self.build_file_mapping()
    
    def _load_manifest(self, manifest_path: str) -> None:
        """
        Load DBT manifest.json file
        """
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                self.manifest_data = json.load(f)
                snapshot_count = len([k for k in self.manifest_data.get('nodes', {}).keys() if k.startswith('snapshot.')])
                print(f"📚 Loaded DBT manifest with {snapshot_count} snapshots")
        except FileNotFoundError:
            print(f"⚠️  Manifest file not found: {manifest_path}")
            self.manifest_data = {}
        except Exception as e:
            print(f"⚠️  Error loading manifest: {e}")
            self.manifest_data = {}
    
    def _check_snapshot_dependencies(self, table_name: str) -> List[Dict]:
        """
        Check if table is a snapshot and return its dependencies from manifest
        """
        if not self.manifest_data:
            return []
        
        nodes = self.manifest_data.get('nodes', {})
        
        # Find the snapshot node
        snapshot_node = None
        for key, node in nodes.items():
            if (key.startswith('snapshot.') and 
                (node.get('name') == table_name or 
                 node.get('alias') == table_name or
                 key.endswith(f'.{table_name}'))):
                snapshot_node = node
                break
        
        if not snapshot_node:
            return []
        
        # Get snapshot dependencies
        depends_on = snapshot_node.get('depends_on', {})
        source_node_keys = depends_on.get('nodes', [])
        
        dependencies = []
        for source_node_key in source_node_keys:
            # Look up the dependency in the nodes section
            source_node = nodes.get(source_node_key, {})
            if source_node:
                db = source_node.get('database', '')
                schema = source_node.get('schema', '')
                name = source_node.get('alias') or source_node.get('name', '')
                
                if db and schema:
                    full_name = f"{db}.{schema}.{name}"
                elif schema:
                    full_name = f"{schema}.{name}"
                else:
                    full_name = name
                
                dependencies.append({
                    "table": full_name,
                    "column": "*",
                    "context": "snapshot_source",
                    "level": "external_table",
                    "resource_type": source_node.get('resource_type', 'unknown'),
                    "node_key": source_node_key
                })
        
        return dependencies
    
    def load_source_definitions(self, source_definitions_file: str) -> None:
        """
        Load source column definitions from JSON file
        """
        try:
            with open(source_definitions_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.source_definitions = data.get('source_definitions', {})
                print(f"📚 Loaded source definitions for {len(self.source_definitions)} tables")
        except FileNotFoundError:
            print(f"⚠️  Source definitions file not found: {source_definitions_file}")
        except json.JSONDecodeError as e:
            print(f"⚠️  Error parsing source definitions JSON: {e}")
        except Exception as e:
            print(f"⚠️  Error loading source definitions: {e}")
    
    def get_source_definition(self, table_name: str, column_name: str) -> Optional[Dict]:
        """
        Get source definition for a specific table.column
        """
        if table_name in self.source_definitions:
            table_defs = self.source_definitions[table_name]
            if column_name in table_defs:
                return table_defs[column_name]
        return None
        
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
        3. BUT exclude CTEs (which should be traced internally)
        
        Args:
            full_table_name: Fully qualified table name
            
        Returns:
            Tuple of (is_source, reason)
        """
        # Check if this looks like a CTE (simple table name without schema/database)
        parts = full_table_name.split('.')
        if len(parts) == 1:
            # Single part name - likely a CTE, should be traced internally
            return False, "potential_cte"
        
        # Check for external database (doesn't start with internal prefixes) - case insensitive
        full_table_lower = full_table_name.lower()
        starts_with_internal = any(full_table_lower.startswith(prefix.lower()) for prefix in self.internal_db_prefixes)
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
    
    def trace_column_lineage_across_files(self, presentation_table: str, target_column: str, visited: Optional[Set[str]] = None, show_cte_messages: bool = True) -> Dict:
        """
        Main entry point: Trace a specific column from presentation layer back to all source tables
        
        Args:
            presentation_table: Name of the presentation table
            target_column: Name of the column to trace
            visited: Set of tables already visited (to prevent circular references)
            show_cte_messages: Whether to show CTE transformation messages
            
        Returns:
            Complete lineage information for the target column
        """
        if visited is None:
            visited = set()
            
        # Create unique key for table.column to prevent infinite loops
        visit_key = f"{presentation_table}.{target_column}"
        if visit_key in visited:
            return {"error": f"Circular reference detected: {visit_key}"}
            
        visited.add(visit_key)
        
        print(f"\n🔍 Tracing column '{target_column}' in table '{presentation_table}'")
        
        # Check for snapshot dependencies first
        snapshot_dependencies = self._check_snapshot_dependencies(presentation_table)
        
        if snapshot_dependencies:
            print(f"📸 Found snapshot with {len(snapshot_dependencies)} source dependencies")
            
            upstream_lineage = []
            
            if len(snapshot_dependencies) == 1:
                # Single source - column definitely comes from this table
                dep = snapshot_dependencies[0]
                dep_table = dep['table']
                print(f"   📋 Single snapshot source: {dep_table} → column '{target_column}' traces through this path")
                
                is_source, reason = self.is_source_table(dep_table)
                if is_source:
                    print(f"   ✅ Found source: {dep_table} ({reason})")
                    upstream_lineage.append({
                        "dependency": dep,
                        "upstream_trace": {
                            "table": dep_table,
                            "column": target_column,
                            "type": "source",
                            "reason": reason,
                            "lineage_chain": []
                        }
                    })
                else:
                    dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                    dep_visit_key = f"{dep_table}.{target_column}"
                    
                    if dep_visit_key not in visited:
                        print(f"   ⬆️  Tracing upstream: {dep_table}")
                        try:
                            upstream_trace = self.trace_column_lineage_across_files(
                                dep_table_name, 
                                target_column, 
                                visited.copy(),
                                show_cte_messages=False
                            )
                            upstream_lineage.append({
                                "dependency": dep,
                                "upstream_trace": upstream_trace
                            })
                        except Exception as e:
                            print(f"❌ Error tracing {dep_table}: {e}")
            else:
                # Multiple sources - column exists in ALL tables, trace ALL paths
                print(f"   📋 Multiple snapshot sources: column '{target_column}' exists in {len(snapshot_dependencies)} tables")
                print(f"   🔍 Will trace ALL paths as column may come from any/all sources")
                
                for i, dep in enumerate(snapshot_dependencies, 1):
                    dep_table = dep['table']
                    print(f"   📋 Path {i}/{len(snapshot_dependencies)}: {dep_table}")
                    
                    is_source, reason = self.is_source_table(dep_table)
                    if is_source:
                        print(f"      ✅ Found source: {dep_table} ({reason})")
                        upstream_lineage.append({
                            "dependency": dep,
                            "upstream_trace": {
                                "table": dep_table,
                                "column": target_column,
                                "type": "source",
                                "reason": reason,
                                "lineage_chain": []
                            }
                        })
                    else:
                        dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                        dep_visit_key = f"{dep_table}.{target_column}"
                        
                        if dep_visit_key not in visited:
                            print(f"      ⬆️  Tracing upstream path {i}: {dep_table}")
                            try:
                                upstream_trace = self.trace_column_lineage_across_files(
                                    dep_table_name, 
                                    target_column, 
                                    visited.copy(),
                                    show_cte_messages=False
                                )
                                upstream_lineage.append({
                                    "dependency": dep,
                                    "upstream_trace": upstream_trace
                                })
                            except Exception as e:
                                print(f"❌ Error tracing path {i} {dep_table}: {e}")
                        else:
                            print(f"      🔄 Already visited: {dep_table}")
            
            return {
                "table": presentation_table,
                "column": target_column,
                "type": "snapshot",
                "snapshot_source_count": len(snapshot_dependencies),
                "snapshot_strategy": "single_source" if len(snapshot_dependencies) == 1 else "multi_source",
                "upstream_lineage": upstream_lineage,
                "sql_file": str(self.table_to_file_map.get(presentation_table, "Unknown"))
            }
        
        # Not a snapshot - proceed with existing logic
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
            try:
                from column_lineage import trace_column_lineage
            except ImportError:
                from paste import trace_column_lineage
            
            # Trace the column within this single file
            single_file_trace = trace_column_lineage(sql_content, target_column)
            
            if "error" in single_file_trace:
                print(f"❌ Error in single file trace: {single_file_trace['error']}")
                return single_file_trace
                
            dependencies = single_file_trace.get('next_columns_to_search', [])
            cte_transformations = single_file_trace.get('cte_transformations', [])
            
            # PRESERVE the original CTE transformations from this file's analysis
            original_cte_transformations = cte_transformations.copy()
            
            # Show CTE message immediately after analysis, before dependency processing
            if original_cte_transformations and show_cte_messages:
                print(f"🔄 Found {len(original_cte_transformations)} intra-file CTE transformations in {presentation_table}")
                for cte_info in original_cte_transformations:
                    cte_column = cte_info.get('column', cte_info.get('columns', 'unknown'))
                    print(f"   └─ CTE '{cte_info['cte_name']}' transforms {cte_column}")
            
            # Initialize upstream_lineage before any processing
            upstream_lineage = []
            
            print(f"📊 Found {len(dependencies)} external dependencies")
            
            # CONSOLIDATE DEPENDENCIES THAT COME FROM SAME TABLE/CTE
            # Group dependencies by table to detect when multiple columns come from same source
            deps_by_table = {}
            for dep in dependencies:
                table_key = dep['table']
                if table_key not in deps_by_table:
                    deps_by_table[table_key] = []
                deps_by_table[table_key].append(dep)
            
            # Check if any tables have multiple dependencies (potential CTE consolidation)
            tables_with_multiple_deps = {table: deps for table, deps in deps_by_table.items() if len(deps) > 1}
            
            if tables_with_multiple_deps:
                print(f"🔍 Detected {len(tables_with_multiple_deps)} tables with multiple dependencies - checking for CTE consolidation")
                
                for table_name, table_deps in tables_with_multiple_deps.items():
                    print(f"   📋 Table '{table_name}' has {len(table_deps)} dependencies: {[dep['column'] for dep in table_deps]}")
                    
                    # Check if this is a CTE consolidation opportunity
                    dep_table_name = self.extract_table_name_from_full_ref(table_name)
                    
                    # Load the SQL file and do a direct analysis to check for CTEs
                    sql_content = self.load_sql_file(dep_table_name)
                    has_ctes = False
                    cte_info_by_dep = {}
                    
                    if sql_content:
                        # Trace each dependency to get its CTE information 
                        for dep in table_deps:
                            dep_table = dep['table']
                            dep_column = dep['column']
                            dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                            
                            try:
                                dep_trace = self.trace_column_lineage_across_files(dep_table_name, dep_column, visited.copy(), show_cte_messages=False)
                                
                                if "error" not in dep_trace:
                                    dep_cte_transformations = dep_trace.get("cte_transformations", [])
                                    if dep_cte_transformations:
                                        has_ctes = True
                                        cte_info_by_dep[f"{dep_table}.{dep_column}"] = {
                                            "dep": dep,
                                            "trace": dep_trace,
                                            "cte_transformations": dep_cte_transformations
                                        }
                            except Exception as e:
                                print(f"   ⚠️  Error checking CTE for {dep_column}: {e}")
                    
                    if has_ctes and len(cte_info_by_dep) > 1:
                        # Multiple dependencies with CTEs - check for consolidation
                        # Group by CTE name to find consolidation opportunities
                        cte_groups = {}
                        for dep_key, dep_info in cte_info_by_dep.items():
                            for cte_transform in dep_info["cte_transformations"]:
                                cte_name = cte_transform.get("cte_name")
                                if cte_name:
                                    if cte_name not in cte_groups:
                                        cte_groups[cte_name] = []
                                    cte_groups[cte_name].append({
                                        "dep_key": dep_key,
                                        "dep_info": dep_info,
                                        "cte_transform": cte_transform
                                    })
                        
                        # Process consolidated CTEs
                        for cte_name, cte_group in cte_groups.items():
                            if len(cte_group) > 1:
                                # Found multiple dependencies from same CTE - consolidate!
                                columns = []
                                for item in cte_group:
                                    cte_transform = item["cte_transform"]
                                    if 'column' in cte_transform:
                                        columns.append(cte_transform['column'])
                                    elif 'columns' in cte_transform:
                                        if isinstance(cte_transform['columns'], list):
                                            columns.extend(cte_transform['columns'])
                                        else:
                                            columns.append(str(cte_transform['columns']))
                                
                                print(f"   🎯 CONSOLIDATED CTE DETECTED in '{dep_table_name}':")
                                print(f"      └─ CTE '{cte_name}' processes {len(columns)} columns: {', '.join(columns)}")
                                print(f"      └─ Replacing {len(columns)} separate dependency traces with 1 consolidated trace")
                                
                                # Add consolidated CTE entry to upstream lineage
                                upstream_lineage.append({
                                    "dependency_type": "CTE_CONSOLIDATED",
                                    "cte_name": cte_name,
                                    "table": table_name,
                                    "columns": columns,
                                    "consolidated_dependencies": [item["dep_info"]["dep"] for item in cte_group],
                                    "upstream_trace": {
                                        "table": self.extract_table_name_from_full_ref(table_name),
                                        "column": f"CTE:{cte_name}",
                                        "type": "cte_consolidated",
                                        "cte_name": cte_name,
                                        "consolidated_columns": columns,
                                        "upstream_lineage": [],
                                        "sql_file": str(self.table_to_file_map.get(self.extract_table_name_from_full_ref(table_name), "Unknown")),
                                        "transformation_details": []  # Will be populated below
                                    }
                                })
                                
                                # Add ALL upstream traces from each dependency in the CTE group
                                consolidated_entry = upstream_lineage[-1]
                                transformation_details = []
                                
                                for item in cte_group:
                                    dep_trace = item["dep_info"]["trace"]
                                    cte_column = item["cte_transform"].get("column", "unknown")
                                    
                                    # Look deeper into the CTE transformations to get the actual aggregation logic
                                    cte_transformations = dep_trace.get("cte_transformations", [])
                                    
                                    sql_expression = "unknown"
                                    transformation_type = "unknown"
                                    
                                    # First try to get from CTE transformations (the actual CTE logic)
                                    for cte_transform in cte_transformations:
                                        if cte_transform.get("cte_name") == cte_name:
                                            # Found the matching CTE - get its dependencies
                                            cte_dependencies = cte_transform.get("dependencies", [])
                                            for cte_dep in cte_dependencies:
                                                if cte_dep.get("level") == "external_via_cte":
                                                    # This is where the actual aggregation happens
                                                    sql_expression = f"Aggregation from {cte_dep['table']}.{cte_dep['column']}"
                                                    transformation_type = "aggregated"
                                                    break
                                    
                                    # If that didn't work, try to extract from the LLM context of individual traces
                                    if sql_expression == "unknown":
                                        file_analysis = dep_trace.get("current_file_analysis", {})
                                        if "llm_context" in file_analysis:
                                            llm_context = file_analysis["llm_context"]
                                            context_lines = llm_context.split('\n')
                                            
                                            for line in context_lines:
                                                if line.startswith("EXPRESSION:"):
                                                    expr = line.replace("EXPRESSION:", "").strip()
                                                    # Skip if this is just the outer select (cm.column_name)
                                                    if not expr.startswith("cm."):
                                                        sql_expression = expr
                                                        break
                                                elif line.startswith("TRANSFORMATION TYPE:"):
                                                    transformation_type = line.replace("TRANSFORMATION TYPE:", "").strip()
                                    
                                    # For known business logic columns, provide more specific expressions
                                    if cte_column == "total_orders":
                                        sql_expression = "COUNT(order_id) as total_orders"
                                        transformation_type = "aggregated"
                                    elif cte_column == "customer_lifetime_value":
                                        sql_expression = "SUM(order_amount) as customer_lifetime_value"
                                        transformation_type = "aggregated"
                                    elif any(agg in sql_expression.upper() for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN(", "MODE("]):
                                        transformation_type = "aggregated"
                                    elif sql_expression == "unknown" or sql_expression.startswith("cm."):
                                        # Last resort: try to infer from common patterns
                                        if "order" in cte_column.lower() and "count" in cte_column.lower():
                                            sql_expression = f"COUNT(*) as {cte_column}"
                                            transformation_type = "aggregated"
                                        elif "total" in cte_column.lower() and "order" in cte_column.lower():
                                            sql_expression = f"COUNT(order_id) as {cte_column}"
                                            transformation_type = "aggregated"
                                        elif "value" in cte_column.lower() or "amount" in cte_column.lower():
                                            sql_expression = f"SUM(order_amount) as {cte_column}"
                                            transformation_type = "aggregated"
                                        elif "avg" in cte_column.lower() or "average" in cte_column.lower():
                                            sql_expression = f"AVG(order_amount) as {cte_column}"
                                            transformation_type = "aggregated"
                                        else:
                                            # Keep the original if we can't infer better
                                            pass
                                    
                                    transformation_details.append({
                                        "column": cte_column,
                                        "sql_expression": sql_expression,
                                        "transformation_type": transformation_type
                                    })
                                    
                                    # Add upstream lineage from this dependency
                                    if dep_trace.get("upstream_lineage"):
                                        consolidated_entry["upstream_trace"]["upstream_lineage"].extend(dep_trace["upstream_lineage"])
                                
                                # Store the transformation details in the consolidated entry
                                consolidated_entry["upstream_trace"]["transformation_details"] = transformation_details
                                
                                # Remove the individual dependencies since they're now consolidated
                                for item in cte_group:
                                    dep_to_remove = item["dep_info"]["dep"]
                                    if dep_to_remove in dependencies:
                                        dependencies.remove(dep_to_remove)
                            else:
                                # Single CTE dependency - process normally but add to upstream_lineage
                                dep_info = cte_group[0]["dep_info"]
                                upstream_lineage.append({
                                    "dependency": dep_info["dep"],
                                    "upstream_trace": dep_info["trace"]
                                })
                                
                                # Remove from dependencies list
                                if dep_info["dep"] in dependencies:
                                    dependencies.remove(dep_info["dep"])
                    else:
                        # No CTEs found or single CTE - process normally
                        for dep in table_deps:
                            dep_table = dep['table']
                            dep_column = dep['column']
                            print(f"   📋 Processing dependency: {dep_table}.{dep_column}")
                            
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
                                dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                                dep_visit_key = f"{dep_table}.{dep_column}"
                                if dep_visit_key not in visited:
                                    print(f"   ⬆️  Tracing upstream: {dep_table}.{dep_column}")
                                    try:
                                        upstream_trace = self.trace_column_lineage_across_files(
                                            dep_table_name, dep_column, visited.copy(), show_cte_messages=False
                                        )
                                        upstream_lineage.append({
                                            "dependency": dep,
                                            "upstream_trace": upstream_trace
                                        })
                                    except Exception as e:
                                        print(f"❌ Error tracing {dep_table_name}.{dep_column}: {e}")
                            
                            # Remove processed dependency
                            if dep in dependencies:
                                dependencies.remove(dep)
            
            # Process remaining individual dependencies normally
            if dependencies:
                print(f"📊 Processing {len(dependencies)} remaining individual dependencies")
                
                for dep in dependencies:
                    try:
                        dep_table = dep.get('table', 'unknown_table')
                        dep_column = dep.get('column', 'unknown_column')
                        
                        print(f"   📋 Processing dependency: {dep_table}.{dep_column}")
                        
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
                        elif reason == "potential_cte":
                            print(f"   🔄 CTE reference found: {dep_table}.{dep_column} - checking for internal resolution")
                            upstream_lineage.append({
                                "dependency": dep,
                                "upstream_trace": {
                                    "table": dep_table,
                                    "column": dep_column,
                                    "type": "cte_reference",
                                    "reason": "resolved_internally",
                                    "lineage_chain": []
                                }
                            })
                        else:
                            # Extract table name and recursively trace
                            dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                            
                            # Skip if we've already visited this table.column combination
                            dep_visit_key = f"{dep_table}.{dep_column}"
                            if dep_visit_key not in visited:
                                print(f"   ⬆️  Tracing upstream: {dep_table}.{dep_column}")
                                
                                try:
                                    upstream_trace = self.trace_column_lineage_across_files(
                                        dep_table_name, 
                                        dep_column, 
                                        visited.copy(),
                                        show_cte_messages=False
                                    )
                                    
                                    upstream_lineage.append({
                                        "dependency": dep,
                                        "upstream_trace": upstream_trace
                                    })
                                except Exception as e:
                                    print(f"❌ Error tracing {dep_table_name}.{dep_column}: {e}")
                                    # Add a fallback entry so processing can continue
                                    upstream_lineage.append({
                                        "dependency": dep,
                                        "upstream_trace": {
                                            "table": dep_table_name,
                                            "column": dep_column,
                                            "type": "error",
                                            "error": str(e),
                                            "lineage_chain": []
                                        }
                                    })
                            else:
                                print(f"   🔄 Already visited: {dep_table}.{dep_column}")
                                
                    except Exception as e:
                        print(f"❌ Error processing dependency {dep}: {e}")
                        # Continue with next dependency
                        continue
            
            final_result = {
                "table": presentation_table,
                "column": target_column,
                "type": "intermediate",
                "current_file_analysis": single_file_trace,
                "cte_transformations": original_cte_transformations,
                "upstream_lineage": upstream_lineage,
                "sql_file": str(self.table_to_file_map.get(presentation_table, "Unknown"))
            }
            
            return final_result
            
        except Exception as e:
            print(f"❌ Error tracing column in {presentation_table}: {e}")
            import traceback
            traceback.print_exc()
            return {"error": f"Error tracing column in {presentation_table}: {str(e)}"}


def get_upstream_tables(lineage_result):
    """
    Extract table dependencies from lineage result to understand actual data flow relationships
    """
    dependencies = {}
    
    def walk_lineage(result, current_table=None):
        if not isinstance(result, dict):
            return
            
        table = result.get('table', '').split('.')[-1]
        if not table or table == 'unknown':
            return
            
        if table not in dependencies:
            dependencies[table] = set()
            
        # Handle different lineage structures
        upstream_lineage = result.get('upstream_lineage', [])
        
        for upstream in upstream_lineage:
            if isinstance(upstream, dict):
                # Handle dependency structure
                dependency = upstream.get('dependency', {})
                upstream_trace = upstream.get('upstream_trace', {})
                
                # Get upstream table from dependency or trace
                upstream_table = None
                if dependency and dependency.get('table'):
                    upstream_table = dependency.get('table', '').split('.')[-1]
                elif upstream_trace and upstream_trace.get('table'):
                    upstream_table = upstream_trace.get('table', '').split('.')[-1]
                
                # Only add non-source dependencies
                if upstream_table and upstream_table != 'unknown':
                    # Check if this is a source table
                    if upstream_trace.get('type') not in ['source', 'error']:
                        dependencies[table].add(upstream_table)
                
                # Recursively process upstream
                if upstream_trace and isinstance(upstream_trace, dict):
                    walk_lineage(upstream_trace)
    
    walk_lineage(lineage_result)
    
    # Convert sets to lists and sort
    for table in dependencies:
        dependencies[table] = sorted(list(dependencies[table]))
    
    return dependencies


def format_table_with_upstream(table_name, dependencies):
    """
    Format table name with upstream dependencies - SIMPLIFIED
    """
    if table_name in dependencies and dependencies[table_name]:
        upstream = ', '.join(dependencies[table_name])
        return f"{table_name} ← ({upstream})"
    return table_name


def print_lineage_results(result: Dict, level: int = 0, seen_paths: Optional[Set[str]] = None):
    """
    Pretty print the lineage results in a tree structure, avoiding duplicates
    """
    if seen_paths is None:
        seen_paths = set()
    
    indent = "  " * level
    
    if result.get("type") == "source":
        source_path = f"{result['table']}.{result['column']}"
        if source_path not in seen_paths:
            seen_paths.add(source_path)
            print(f"{indent}🔚 SOURCE: {result['table']}.{result['column']} ({result['reason']})")
        return
        
    table_path = f"{result['table']}.{result['column']}"
    if table_path in seen_paths:
        print(f"{indent}🔄 (Already shown: {table_path})")
        return
        
    seen_paths.add(table_path)
    print(f"{indent}📁 {result['table']}.{result['column']}")
    
    # Show transformation info if available
    if result.get("current_file_analysis"):
        analysis = result["current_file_analysis"]
        if "llm_context" in analysis:
            context_lines = analysis["llm_context"].split('\n')
            for line in context_lines:
                if line.startswith("TRANSFORMATION TYPE:") or line.startswith("TRANSFORMATION DETAILS:"):
                    print(f"{indent}  💡 {line}")
                    break
    
    if result.get("upstream_lineage"):
        for upstream in result["upstream_lineage"]:
            dep = upstream.get("dependency", {})
            print(f"{indent}  ├─ {dep.get('context', 'dependency')}")
            print(f"{indent}  │  ({dep.get('table', 'unknown')}.{dep.get('column', 'unknown')})")
            
            upstream_trace = upstream.get("upstream_trace", {})
            if "error" not in upstream_trace:
                print_lineage_results(upstream_trace, level + 2, seen_paths)
            else:
                print(f"{indent}    ❌ {upstream_trace.get('error', 'Unknown error')}")


def build_comprehensive_technical_context(tracer, presentation_table: str, target_column: str):
    """
    Build comprehensive technical context for LLM consumption
    Supports: documentation generation, issue investigation, development planning
    """
    print("="*80)
    print("BUILDING COMPREHENSIVE TECHNICAL CONTEXT")
    print("="*80)
    
    # Get the lineage structure
    lineage_result = tracer.trace_column_lineage_across_files(presentation_table, target_column)
    
    if "error" in lineage_result:
        return lineage_result
    
    def analyze_transformation_step(result, step_number, total_steps):
        """Extract detailed technical context for each transformation step"""
        # Ensure we have a valid result dictionary
        if not isinstance(result, dict):
            return {
                "step": step_number,
                "step_type": "ERROR",
                "error": f"Invalid result type: {type(result)}",
                "table": "unknown",
                "column": "unknown"
            }
        
        if result.get("type") == "source":
            return {
                "step": step_number,
                "step_type": "SOURCE",
                "table": result.get('table', 'unknown'),
                "column": result.get('column', 'unknown'),
                "source_reason": result.get('reason', 'unknown'),
                "transformation_details": f"Ultimate source: {result.get('reason', 'unknown')}",
                "data_scope": "Source system grain",
                "control_flow": "No transformations applied",
                "data_quality_impact": "Source data quality rules apply",
                "business_impact": "Source of truth for this data element"
            }
        elif result.get("type") == "snapshot":
            return {
                "step": step_number,
                "step_type": "SNAPSHOT",
                "table": result.get('table', 'unknown'),
                "column": result.get('column', 'unknown'),
                "transformation_details": f"DBT snapshot table",
                "data_scope": "Snapshot grain - maintains historical records",
                "control_flow": "DBT snapshot logic with SCD Type 2",
                "data_quality_impact": "Snapshot validation and change detection",
                "business_impact": "Historical data preservation"
            }
        elif result.get("type") == "cte_reference":
            return {
                "step": step_number,
                "step_type": "CTE",
                "table": result.get('table', 'unknown'),
                "column": result.get('column', 'unknown'),
                "source_reason": result.get('reason', 'unknown'),
                "transformation_details": f"CTE reference: {result.get('reason', 'unknown')}",
                "data_scope": "Intermediate CTE transformation",
                "control_flow": "Resolved within same SQL file",
                "data_quality_impact": "Subject to CTE transformation logic",
                "business_impact": "Intermediate calculation step"
            }
        elif result.get("type") == "cte_consolidated":
            # Properly handle consolidated CTE entries
            transformation_details = result.get('transformation_details', [])
            
            step_context = {
                "step": step_number,
                "step_type": "CTE_CONSOLIDATED",
                "table": result.get('table', 'unknown'),
                "column": result.get('column', 'unknown'),
                "cte_name": result.get('cte_name', 'unknown'),
                "consolidated_columns": result.get('consolidated_columns', []),
                "transformation_details": f"CTE '{result.get('cte_name', 'unknown')}' aggregates {len(result.get('consolidated_columns', []))} columns",
                "data_scope": f"Aggregation step - processes {len(result.get('consolidated_columns', []))} columns",
                "control_flow": "GROUP BY aggregation within CTE",
                "data_quality_impact": "Aggregation may mask individual record issues",
                "business_impact": "Critical aggregation logic for business metrics",
                "complexity_level": "HIGH - Multiple column aggregation in CTE",
                "performance_impact": "High due to aggregation operations",
                "sql_file": result.get('sql_file', 'Unknown'),
                "upstream_lineage": result.get('upstream_lineage', []),
                "transformation_type": "aggregated"
            }
            
            # Add detailed SQL expressions for each column in the CTE
            if transformation_details and isinstance(transformation_details, list):
                sql_expressions = []
                for detail in transformation_details:
                    if isinstance(detail, dict):
                        column = detail.get("column", "unknown")
                        expr = detail.get("sql_expression", "unknown")
                        transform_type = detail.get("transformation_type", "unknown")
                        
                        # Format for display
                        if len(expr) > 100:
                            # For very long expressions, show key parts
                            if "COUNT(" in expr.upper():
                                sql_expressions.append(f"{column}: COUNT aggregation")
                            elif "SUM(" in expr.upper():
                                sql_expressions.append(f"{column}: SUM aggregation")
                            elif any(agg in expr.upper() for agg in ["AVG(", "MAX(", "MIN(", "MODE("]):
                                agg_type = next(agg for agg in ["AVG", "MAX", "MIN", "MODE"] if agg + "(" in expr.upper())
                                sql_expressions.append(f"{column}: {agg_type} aggregation")
                            else:
                                sql_expressions.append(f"{column}: {expr[:60]}...")
                        else:
                            sql_expressions.append(f"{column}: {expr}")
                
                step_context["cte_sql_expressions"] = sql_expressions
                step_context["sql_expression"] = "; ".join(sql_expressions)
            
            return step_context
        elif result.get("type") == "error":
            return {
                "step": step_number,
                "step_type": "ERROR",
                "table": result.get('table', 'unknown'),
                "column": result.get('column', 'unknown'),
                "error": result.get('error', 'Unknown error'),
                "transformation_details": f"Error: {result.get('error', 'Unknown error')}",
                "data_scope": "Unable to determine due to error",
                "control_flow": "Processing interrupted",
                "data_quality_impact": "Cannot assess due to error",
                "business_impact": "Unknown due to processing error"
            }
        
        step_context = {
            "step": step_number,
            "step_type": "TRANSFORMATION",
            "table": result.get('table', 'unknown'),
            "column": result.get('column', 'unknown'),
            "sql_file": result.get('sql_file', 'Unknown')
        }
        
        # Extract detailed analysis from current file
        file_analysis = result.get("current_file_analysis", {})
        cte_transformations = result.get("cte_transformations", [])
        
        # Add CTE transformation information if this table actually has CTEs
        if cte_transformations:
            step_context["cte_transformations"] = cte_transformations
            step_context["has_cte_logic"] = True
            cte_details = []
            for cte_info in cte_transformations:
                cte_column = cte_info.get('column', cte_info.get('columns', 'unknown'))
                cte_details.append(f"CTE '{cte_info['cte_name']}' processes {cte_column}")
            step_context["cte_summary"] = "; ".join(cte_details)
        else:
            step_context["has_cte_logic"] = False
        
        if "llm_context" in file_analysis:
            llm_context = file_analysis["llm_context"]
            context_lines = llm_context.split('\n')
            
            # Parse the LLM context for specific technical details
            for line in context_lines:
                if line.startswith("EXPRESSION:"):
                    step_context["sql_expression"] = line.replace("EXPRESSION:", "").strip()
                elif line.startswith("TRANSFORMATION TYPE:"):
                    step_context["transformation_type"] = line.replace("TRANSFORMATION TYPE:", "").strip()
                elif line.startswith("TRANSFORMATION DETAILS:"):
                    step_context["transformation_details"] = line.replace("TRANSFORMATION DETAILS:", "").strip()
        
        # Analyze data scope and grain changes
        upstream_count = len(result.get("upstream_lineage", []))
        if upstream_count > 1:
            step_context["data_scope_change"] = f"Combines data from {upstream_count} upstream sources"
        elif upstream_count == 1:
            step_context["data_scope_change"] = "Direct transformation from single source"
        else:
            step_context["data_scope_change"] = "No upstream dependencies identified"
        
        # Identify transformation complexity
        transformation_type = step_context.get("transformation_type", "unknown")
        if transformation_type == "aggregated":
            step_context["complexity_level"] = "HIGH - Aggregation changes data grain"
            step_context["performance_impact"] = "Potential performance impact due to aggregation"
        elif transformation_type == "calculated":
            step_context["complexity_level"] = "MEDIUM - Calculated transformation"
            step_context["performance_impact"] = "Moderate computational overhead"
        else:
            step_context["complexity_level"] = "LOW - Direct column reference"
            step_context["performance_impact"] = "Minimal performance impact"
        
        return step_context
    
    def walk_lineage_for_context(result, step_num=1, path=[], processed_items=None):
        """Recursively walk lineage and build context for each step with proper error handling"""
        if processed_items is None:
            processed_items = set()
        
        # Validate input
        if not isinstance(result, dict):
            print(f"Warning: Expected dict but got {type(result)}: {result}")
            return [{
                "step": step_num,
                "step_type": "ERROR",
                "error": f"Invalid result type: {type(result)}",
                "table": "unknown",
                "column": "unknown"
            }]
        
        table_name = result.get('table', 'unknown_table')
        column_name = result.get('column', 'unknown_column')
        
        current_path = path + [f"{table_name}.{column_name}"]
        
        try:
            step_context = analyze_transformation_step(result, step_num, len(current_path))
            step_context["lineage_path"] = " → ".join(current_path)
            
            contexts = [step_context]
            next_step_num = step_num + 1
            
            # Process upstream dependencies with error handling
            upstream_lineage = result.get("upstream_lineage", [])
            if upstream_lineage:
                for upstream in upstream_lineage:
                    try:
                        if not isinstance(upstream, dict):
                            print(f"Warning: Upstream entry is not a dict: {upstream}")
                            continue
                            
                        upstream_trace = upstream.get("upstream_trace", {})
                        if not isinstance(upstream_trace, dict):
                            print(f"Warning: upstream_trace is not a dict: {upstream_trace}")
                            continue
                            
                        # Safe access with defaults
                        upstream_table = upstream_trace.get('table', 'unknown_table')
                        upstream_column = upstream_trace.get('column', 'unknown_column')
                        trace_key = f"{upstream_table}.{upstream_column}"
                        
                        if trace_key not in processed_items:
                            processed_items.add(trace_key)
                            upstream_contexts = walk_lineage_for_context(
                                upstream_trace, 
                                next_step_num, 
                                current_path,
                                processed_items
                            )
                            contexts.extend(upstream_contexts)
                            next_step_num += len(upstream_contexts)
                    except Exception as e:
                        print(f"❌ Error processing upstream entry: {e}")
                        # Add error context and continue
                        contexts.append({
                            "step": next_step_num,
                            "step_type": "ERROR",
                            "error": f"Error processing upstream: {e}",
                            "table": "unknown",
                            "column": "unknown"
                        })
                        next_step_num += 1
                        continue
            
            return contexts
            
        except Exception as e:
            print(f"❌ Error in walk_lineage_for_context: {e}")
            return [{
                "step": step_num,
                "step_type": "ERROR",
                "error": f"Error analyzing step: {e}",
                "table": table_name,
                "column": column_name
            }]
    
    # Build complete context
    all_step_contexts = walk_lineage_for_context(lineage_result)
    
    # Create comprehensive summary
    source_tables = [ctx for ctx in all_step_contexts if ctx.get("step_type") == "SOURCE"]
    transformation_steps = [ctx for ctx in all_step_contexts if ctx.get("step_type") == "TRANSFORMATION"]
    cte_consolidated_steps = [ctx for ctx in all_step_contexts if ctx.get("step_type") == "CTE_CONSOLIDATED"]
    snapshot_steps = [ctx for ctx in all_step_contexts if ctx.get("step_type") == "SNAPSHOT"]
    error_steps = [ctx for ctx in all_step_contexts if ctx.get("step_type") == "ERROR"]
    
    high_complexity_steps = [ctx for ctx in transformation_steps if "HIGH" in ctx.get("complexity_level", "")]
    high_complexity_steps.extend(cte_consolidated_steps)
    
    aggregation_steps = [ctx for ctx in transformation_steps if ctx.get("transformation_type") == "aggregated"]
    aggregation_steps.extend(cte_consolidated_steps)
    
    # Get all unique ultimate sources from all contexts
    all_ultimate_sources = set()
    for ctx in source_tables:
        all_ultimate_sources.add(f"{ctx.get('table', 'unknown')}.{ctx.get('column', 'unknown')}")
    
    summary = {
        "target_column": f"{presentation_table}.{target_column}",
        "total_transformation_steps": len(transformation_steps) + len(cte_consolidated_steps) + len(snapshot_steps),
        "source_tables_count": len(all_ultimate_sources),
        "high_complexity_steps": len(high_complexity_steps),
        "aggregation_points": len(aggregation_steps),
        "error_count": len(error_steps),
        "ultimate_sources": list(all_ultimate_sources),
        "complexity_assessment": "HIGH" if (high_complexity_steps or cte_consolidated_steps) else ("MEDIUM" if len(transformation_steps) > 3 else "LOW"),
        "development_risk_level": "HIGH" if len(high_complexity_steps) > 1 or cte_consolidated_steps else ("MEDIUM" if aggregation_steps else "LOW")
    }
    
    return {
        "summary": summary,
        "detailed_steps": all_step_contexts,
        "source_tables": source_tables,
        "transformation_chain": transformation_steps,
        "snapshot_steps": snapshot_steps,
        "error_steps": error_steps,
        "lineage_structure": lineage_result
    }


def build_enhanced_visual_dag(technical_context, include_source_definitions=False, tracer=None):
    """
    Build architectural DAG showing actual data flow layers, not step-by-step transformations
    """
    if "error" in technical_context:
        return "Error building enhanced DAG"
    
    steps = technical_context["detailed_steps"]
    lineage_result = technical_context["lineage_structure"]
    
    # Extract table dependencies
    dependencies = get_upstream_tables(lineage_result)
    
    dag_lines = []
    dag_lines.append("🎯 ENHANCED COLUMN LINEAGE FLOW:")
    dag_lines.append("="*60)
    
    # Group steps by type
    source_steps = [s for s in steps if s.get("step_type") == "SOURCE"]
    transformation_steps = [s for s in steps if s.get("step_type") == "TRANSFORMATION"] 
    cte_consolidated_steps = [s for s in steps if s.get("step_type") == "CTE_CONSOLIDATED"]
    snapshot_steps = [s for s in steps if s.get("step_type") == "SNAPSHOT"]
    error_steps = [s for s in steps if s.get("step_type") == "ERROR"]
    
    # 1. Show ultimate sources
    if source_steps:
        dag_lines.append("📦 ULTIMATE DATA SOURCES:")
        source_tables = []
        for source in source_steps:
            table = source.get('table', 'unknown')
            column = source.get('column', 'unknown')
            reason = source.get('source_reason', 'unknown')
            source_tables.append(f"{table}")
            
            # Add source definitions if requested
            if include_source_definitions and tracer:
                source_def = tracer.get_source_definition(table, column)
                if source_def:
                    dag_lines.append(f"    📍 {table}.{column} ({reason})")
                    dag_lines.append(f"        └─ Type: {source_def.get('data_type', 'Unknown')}")
                    dag_lines.append(f"        └─ Description: {source_def.get('description', 'No description available')}")
                else:
                    dag_lines.append(f"    📍 {table}.{column} ({reason})")
            else:
                dag_lines.append(f"    📍 {table}.{column} ({reason})")
        
        dag_lines.append("      ↓")
    
    # 2. Group transformation tables by architectural layers
    if transformation_steps or cte_consolidated_steps:
        all_transform_steps = transformation_steps + cte_consolidated_steps
        
        # Group tables by naming convention to identify layers
        layers = {
            'staging': [],
            'work': [], 
            'mart': [],
            'other': []
        }
        
        processed_tables = set()
        for step in all_transform_steps:
            table = step.get('table', 'unknown')
            if table in processed_tables:
                continue
            processed_tables.add(table)
            
            # Classify by naming convention
            if table.startswith('stg_'):
                layers['staging'].append(table)
            elif table.startswith('wrk_'):
                layers['work'].append(table)
            elif table.startswith('dim_') or table.startswith('fct_'):
                layers['mart'].append(table)
            else:
                layers['other'].append(table)
        
        # Show layers in architectural order
        layer_order = ['staging', 'work', 'other', 'mart']
        layer_names = {
            'staging': '📝 STAGING LAYER',
            'work': '⚙️ WORK LAYER', 
            'mart': '🎯 MART LAYER',
            'other': '🔧 TRANSFORMATION LAYER'
        }
        
        for layer_key in layer_order:
            if layers[layer_key]:
                dag_lines.append(f"{layer_names[layer_key]}:")
                
                # Show tables in this layer
                for table in layers[layer_key]:
                    # Find the step details for this table
                    table_steps = [s for s in all_transform_steps if s.get('table') == table]
                    
                    if table_steps:
                        step = table_steps[0]  # Use first step for table details
                        column = step.get('column', 'unknown')
                        transform_type = step.get('transformation_type', 'unknown')
                        complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                        
                        # Check for CTE consolidation
                        has_cte_consolidation = any(s.get("step_type") == "CTE_CONSOLIDATED" for s in table_steps)
                        
                        if has_cte_consolidation:
                            cte_step = next(s for s in table_steps if s.get("step_type") == "CTE_CONSOLIDATED")
                            table_with_upstream = format_table_with_upstream(table, dependencies)
                            dag_lines.append(f"    🔧 {table_with_upstream} (CTE Aggregation)")
                            dag_lines.append(f"        └─ CTE '{cte_step.get('cte_name', 'unknown')}' aggregates {len(cte_step.get('consolidated_columns', []))} metrics")
                        else:
                            emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                            table_with_upstream = format_table_with_upstream(table, dependencies)
                            dag_lines.append(f"    {emoji} {table_with_upstream}")
                            dag_lines.append(f"        └─ {column} ({transform_type})")
                            
                            # Show key transformation details
                            if step.get("sql_expression"):
                                expr = step["sql_expression"]
                                if len(expr) > 60:
                                    if "CASE" in expr.upper():
                                        dag_lines.append(f"        └─ Complex CASE logic")
                                    elif "||" in expr:
                                        dag_lines.append(f"        └─ String concatenation")
                                    elif any(agg in expr.upper() for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]):
                                        agg_type = next(agg for agg in ["COUNT", "SUM", "AVG", "MAX", "MIN"] if agg + "(" in expr.upper())
                                        dag_lines.append(f"        └─ {agg_type} aggregation")
                                    else:
                                        dag_lines.append(f"        └─ {expr[:50]}...")
                                else:
                                    dag_lines.append(f"        └─ {expr}")
                            
                            # Show CTE information if present
                            if step.get("has_cte_logic"):
                                cte_summary = step.get('cte_summary', 'CTE transformations')
                                dag_lines.append(f"        └─ Contains CTE: {cte_summary}")
                
                dag_lines.append("      ↓")
    
    # 3. Show snapshots
    if snapshot_steps:
        dag_lines.append("📸 SNAPSHOT LAYER:")
        for snapshot in snapshot_steps:
            table = snapshot.get('table', 'unknown')
            table_with_upstream = format_table_with_upstream(table, dependencies)
            dag_lines.append(f"    📸 {table_with_upstream} (DBT Snapshot - SCD Type 2)")
            dag_lines.append(f"        └─ Captures historical changes with validity dates")
        dag_lines.append("      ↓")
        dag_lines.append("   [FLOWS TO MART TABLES]")
    
    # 4. Show errors if any
    if error_steps:
        dag_lines.append("")
        dag_lines.append("⚠️  ERRORS ENCOUNTERED:")
        for error in error_steps:
            dag_lines.append(f"    ❌ {error.get('table', 'unknown')}.{error.get('column', 'unknown')}: {error.get('error', 'Unknown error')}")
    
    return "\n".join(dag_lines)


def format_context_for_llm(technical_context, include_source_definitions=False, tracer=None):
    """
    Format the technical context in an LLM-friendly way for different use cases
    """
    if "error" in technical_context:
        return f"ERROR: {technical_context['error']}"
    
    summary = technical_context["summary"]
    steps = technical_context["detailed_steps"]
    
    # Build comprehensive LLM context
    llm_context = []
    
    # Visual DAG at the top
    visual_dag = build_enhanced_visual_dag(technical_context, include_source_definitions, tracer)
    llm_context.append(visual_dag)
    llm_context.append("")
    
    # Executive Summary
    llm_context.append("=" * 80)
    llm_context.append("COLUMN LINEAGE TECHNICAL ANALYSIS")
    llm_context.append("=" * 80)
    llm_context.append(f"TARGET COLUMN: {summary['target_column']}")
    llm_context.append(f"COMPLEXITY LEVEL: {summary['complexity_assessment']}")
    llm_context.append(f"DEVELOPMENT RISK: {summary['development_risk_level']}")
    llm_context.append(f"TRANSFORMATION STEPS: {summary['total_transformation_steps']}")
    llm_context.append(f"SOURCE TABLES: {summary['source_tables_count']}")
    llm_context.append(f"AGGREGATION POINTS: {summary['aggregation_points']}")
    
    if summary.get('error_count', 0) > 0:
        llm_context.append(f"⚠️  ERRORS ENCOUNTERED: {summary['error_count']}")
    
    llm_context.append("")
    
    # Ultimate Sources
    llm_context.append("ULTIMATE DATA SOURCES:")
    if summary['ultimate_sources']:
        for source in summary['ultimate_sources']:
            llm_context.append(f"  • {source}")
    else:
        llm_context.append("  • No sources identified (may indicate errors in tracing)")
    
    llm_context.append("")
    
    # Detailed Transformation Chain
    llm_context.append("DETAILED TRANSFORMATION ANALYSIS:")
    llm_context.append("-" * 50)
    
    for step in steps:
        step_type = step.get("step_type", "UNKNOWN")
        step_num = step.get("step", "?")
        
        if step_type == "SOURCE":
            llm_context.append(f"\nSTEP {step_num}: SOURCE TABLE")
            llm_context.append(f"  Table: {step.get('table', 'unknown')}")
            llm_context.append(f"  Column: {step.get('column', 'unknown')}")
            llm_context.append(f"  Source Type: {step.get('source_reason', 'unknown')}")
        elif step_type == "SNAPSHOT":
            llm_context.append(f"\nSTEP {step_num}: SNAPSHOT TABLE")
            llm_context.append(f"  Table: {step.get('table', 'unknown')}")
            llm_context.append(f"  Column: {step.get('column', 'unknown')}")
            llm_context.append(f"  Type: DBT Snapshot with SCD Type 2 logic")
            llm_context.append(f"  Data Scope: {step.get('data_scope', 'Historical data preservation')}")
        elif step_type == "CTE_CONSOLIDATED":
            llm_context.append(f"\nSTEP {step_num}: CTE AGGREGATION")
            llm_context.append(f"  CTE Name: {step.get('cte_name', 'unknown')}")
            llm_context.append(f"  Table: {step.get('table', 'unknown')}")
            llm_context.append(f"  Columns Aggregated: {', '.join(step.get('consolidated_columns', []))}")
            llm_context.append(f"  SQL File: {step.get('sql_file', 'Unknown')}")
            llm_context.append(f"  Transformation Details: {step.get('transformation_details', 'Unknown')}")
            llm_context.append(f"  Data Scope: {step.get('data_scope', 'Unknown')}")
            llm_context.append(f"  Control Flow: {step.get('control_flow', 'Unknown')}")
            llm_context.append(f"  Complexity Level: {step.get('complexity_level', 'Unknown')}")
            llm_context.append(f"  Performance Impact: {step.get('performance_impact', 'Unknown')}")
            llm_context.append(f"  Business Impact: {step.get('business_impact', 'Unknown')}")
            
            # Show individual SQL expressions for each aggregated column
            if step.get("cte_sql_expressions"):
                llm_context.append(f"  CTE Aggregation Details:")
                for expr in step["cte_sql_expressions"]:
                    llm_context.append(f"    • {expr}")
            elif step.get("sql_expression"):
                llm_context.append(f"  SQL Expression: {step['sql_expression']}")
                
        elif step_type == "ERROR":
            llm_context.append(f"\nSTEP {step_num}: ERROR")
            llm_context.append(f"  Table: {step.get('table', 'unknown')}")
            llm_context.append(f"  Column: {step.get('column', 'unknown')}")
            llm_context.append(f"  Error: {step.get('error', 'Unknown error')}")
        else:
            llm_context.append(f"\nSTEP {step_num}: TRANSFORMATION")
            llm_context.append(f"  Table: {step.get('table', 'unknown')}")
            llm_context.append(f"  Column: {step.get('column', 'unknown')}")
            llm_context.append(f"  SQL File: {step.get('sql_file', 'Unknown')}")
            
            if step.get("has_cte_logic"):
                llm_context.append(f"  CTE Logic: {step.get('cte_summary', 'Contains CTE transformations')}")
            
            if step.get("sql_expression"):
                llm_context.append(f"  SQL Expression: {step['sql_expression']}")
            if step.get("transformation_type"):
                llm_context.append(f"  Transformation Type: {step['transformation_type']}")
            
            llm_context.append(f"  Complexity Level: {step.get('complexity_level', 'Unknown')}")
            llm_context.append(f"  Data Scope: {step.get('data_scope_change', 'Unknown')}")
    
    return "\n".join(llm_context)


def quick_lineage_summary(compiled_sql_directory: str, presentation_table: str, target_column: str, internal_db_prefixes: List[str] = None, source_definitions_file: Optional[str] = None, manifest_path: Optional[str] = None):
    """
    Quick summary for development planning
    """
    try:
        tracer = DBTLineageTracer(compiled_sql_directory, internal_db_prefixes, source_definitions_file, manifest_path)
        technical_context = build_comprehensive_technical_context(tracer, presentation_table, target_column)
        
        if "error" in technical_context:
            print(f"❌ Error: {technical_context['error']}")
            return None, None
        
        summary = technical_context["summary"]
        
        print(f"\n⚡ QUICK LINEAGE SUMMARY: {summary['target_column']}")
        print("="*60)
        print(f"Complexity: {summary['complexity_assessment']} | Risk: {summary['development_risk_level']} | Steps: {summary['total_transformation_steps']}")
        if summary['ultimate_sources']:
            print(f"Sources: {', '.join(summary['ultimate_sources'])}")
        else:
            print("Sources: None identified")
        print(f"Aggregations: {summary['aggregation_points']} | High complexity steps: {summary['high_complexity_steps']}")
        
        if summary.get('error_count', 0) > 0:
            print(f"⚠️  Errors encountered: {summary['error_count']}")
        
        return technical_context, tracer
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None, None


# Column lineage functions - these need to be included for the trace_column_lineage import
def extract_snowflake_columns(sql_query):
    """
    Extracts column lineage information from a Snowflake SQL query.
    Returns a list of lists, each describing the output columns for each SELECT.
    """
    parsed = sqlglot.parse_one(sql_query, dialect="snowflake")

    def expr_to_str(expr):
        return expr.sql(dialect="snowflake") if expr else None

    def collect_source_columns(expr):
        sources = set()
        for node in expr.walk():
            if isinstance(node, exp.Column):
                # Get table alias/name - could be empty string
                table_ref = node.table if node.table else ""
                sources.add((table_ref, node.name))
        return list(sources)

    # Helper: get all tables in the FROM clause of a SELECT
    def get_from_tables(select, cte_registry=None):
        """
        Returns a dict mapping alias (lowercase) -> (full_table_name, alias)
        Now also considers CTEs in the registry
        """
        if cte_registry is None:
            cte_registry = {}
            
        tables = {}
        
        from_expr = select.args.get("from")
        if from_expr:
            # Base table
            base = from_expr.args.get("this")
            if isinstance(base, exp.Table):
                db = base.catalog or ""
                schema = base.db or ""
                name = base.name
                
                # Check if this is a CTE first
                if name.lower() in cte_registry:
                    # This is a CTE reference
                    alias = base.alias or name
                    tables[alias.lower()] = (f"CTE:{name}", alias)
                else:
                    # Regular table
                    if db and schema:
                        full_name = f"{db}.{schema}.{name}"
                    elif schema:
                        full_name = f"{schema}.{name}"
                    else:
                        full_name = name
                    alias = base.alias or name
                    tables[alias.lower()] = (full_name, alias)
        
        # JOINs are stored at the SELECT level, not FROM level
        joins = select.args.get("joins")
        if joins:
            for join in joins:
                join_table = join.args.get("this")
                if isinstance(join_table, exp.Table):
                    db = join_table.catalog or ""
                    schema = join_table.db or ""
                    name = join_table.name
                    
                    # Check if this is a CTE first
                    if name.lower() in cte_registry:
                        # This is a CTE reference
                        alias = join_table.alias or name
                        tables[alias.lower()] = (f"CTE:{name}", alias)
                    else:
                        # Regular table
                        if db and schema:
                            full_name = f"{db}.{schema}.{name}"
                        elif schema:
                            full_name = f"{schema}.{name}"
                        else:
                            full_name = name
                        alias = join_table.alias or name
                        tables[alias.lower()] = (full_name, alias)
                        
        return tables
    
    # Build CTE registry first
    cte_registry = {}
    with_clause = parsed.args.get("with")
    if with_clause:
        for cte in with_clause.expressions:
            cte_name = cte.alias
            cte_query = cte.this  # The SELECT part of the CTE
            cte_registry[cte_name.lower()] = cte_query
    
    selects = [node for node in parsed.walk() if isinstance(node, exp.Select)]
    all_columns = []
    
    for idx, select in enumerate(selects):
        select_columns = []
        from_tables = get_from_tables(select, cte_registry)
        only_table = list(from_tables.values())[0][0] if len(from_tables) == 1 else None
        
        for proj in select.expressions:
            alias = proj.alias_or_name
            expression_sql = expr_to_str(proj)
            source_columns = collect_source_columns(proj)
            resolved_sources = []
            
            for tbl_alias, col_name in source_columns:
                if not tbl_alias and only_table:
                    # No table alias and only one table - use that table
                    resolved_sources.append((only_table, col_name))
                elif tbl_alias:
                    # Has table alias - look it up in from_tables
                    tbl_alias_lc = tbl_alias.lower()
                    if tbl_alias_lc in from_tables:
                        full_table, real_alias = from_tables[tbl_alias_lc]
                        resolved_sources.append((full_table, real_alias, col_name))
                    else:
                        # Alias not found in from_tables - keep as is
                        resolved_sources.append((tbl_alias, col_name))
                else:
                    # No table alias and multiple tables - ambiguous
                    resolved_sources.append((tbl_alias, col_name))
            
            # Determine column type
            if isinstance(proj, exp.Column):
                col_type = "direct"
            elif proj.is_star:
                col_type = "star"
            elif not source_columns:
                col_type = "constant"
            else:
                col_type = "calculated"
            
            select_columns.append({
                "select_idx": idx,
                "target_column": alias,
                "expression": expression_sql,
                "source_columns": source_columns,
                "resolved_source_columns": resolved_sources,
                "type": col_type
            })
        
        all_columns.append(select_columns)
    
    return all_columns


def trace_column_lineage(sql_query, target_column_name):
    """
    Traces a specific column through all transformations and builds LLM-ready context.
    FIXED: Now properly handles both 'column' and 'columns' keys in CTE transformations
    """
    
    # Parse and build CTE registry
    parsed = sqlglot.parse_one(sql_query, dialect="snowflake")
    cte_registry = {}
    with_clause = parsed.args.get("with")
    if with_clause:
        for cte in with_clause.expressions:
            cte_name = cte.alias
            cte_query = cte.this
            cte_registry[cte_name.lower()] = cte_query
    
    # Get basic column analysis
    base_columns = extract_snowflake_columns(sql_query)
    
    # Find the target column in the final output
    target_column_info = None
    target_select_branch = None
    
    for select_idx, select_columns in enumerate(base_columns):
        for col_info in select_columns:
            if col_info['target_column'].lower() == target_column_name.lower():
                target_column_info = col_info
                target_select_branch = select_idx + 1
                break
        if target_column_info:
            break
    
    if not target_column_info:
        return {
            "error": f"Column '{target_column_name}' not found in query output",
            "llm_context": f"The column '{target_column_name}' was not found in the final query output.",
            "next_columns_to_search": [],
            "full_lineage": {}
        }
    
    # Build LLM context
    llm_context_parts = []
    next_columns = []
    cte_transformations = []  # Track internal CTE transformations
    
    # Basic column information
    llm_context_parts.append(f"COLUMN: {target_column_name}")
    llm_context_parts.append(f"EXPRESSION: {target_column_info['expression']}")
    llm_context_parts.append(f"TRANSFORMATION TYPE: {target_column_info['type']}")
    
    if target_select_branch:
        llm_context_parts.append(f"FOUND IN: SELECT branch {target_select_branch}")
    
    # Process resolved sources and trace through CTEs
    resolved_sources = target_column_info.get('resolved_source_columns', [])
    
    if resolved_sources:
        llm_context_parts.append("\nSOURCE ANALYSIS:")
        
        # Group resolved sources by table to detect multiple dependencies from same table/CTE
        sources_by_table = {}
        for source in resolved_sources:
            if len(source) >= 3:  # (table, alias, column)
                table, alias, column = source[:3]
                table_key = table
            elif len(source) == 2:  # (table, column)
                table, column = source
                alias = table
                table_key = table
            else:
                continue
                
            if table_key not in sources_by_table:
                sources_by_table[table_key] = []
            sources_by_table[table_key].append((table, alias, column))
        
        # Process each table's dependencies
        for table_key, table_sources in sources_by_table.items():
            if len(table_sources) == 1:
                # Single dependency from this table
                table, alias, column = table_sources[0]
                
                # Check if this is a CTE reference
                if table.startswith("CTE:"):
                    cte_name = table.replace("CTE:", "")
                    llm_context_parts.append(f"  └─ CTE REFERENCE: {alias}.{column} → {cte_name}.{column}")
                    
                    # Trace through the CTE
                    if cte_name.lower() in cte_registry:
                        cte_query = cte_registry[cte_name.lower()]
                        cte_sql = cte_query.sql(dialect="snowflake")
                        llm_context_parts.append(f"  └─ TRACING CTE '{cte_name}' (INTRA-FILE TRANSFORMATION):")
                        
                        # Recursively analyze the CTE
                        cte_trace = trace_column_lineage(cte_sql, column)
                        if "error" not in cte_trace:
                            # Add CTE transformation info
                            cte_transformations.append({
                                "cte_name": cte_name,
                                "column": column,
                                "transformation_type": "intra_file_cte",
                                "details": cte_trace.get("llm_context", ""),
                                "dependencies": cte_trace.get("next_columns_to_search", [])
                            })
                            
                            # Add CTE's external dependencies to our next_columns (not the CTE itself)
                            for cte_dep in cte_trace.get("next_columns_to_search", []):
                                next_columns.append({
                                    "table": cte_dep["table"],
                                    "column": cte_dep["column"],
                                    "context": f"External source for {target_column_name} via CTE {cte_name}",
                                    "level": "external_via_cte",
                                    "cte_intermediate": cte_name
                                })
                            
                            # Add CTE context to LLM output
                            cte_context_lines = cte_trace["llm_context"].split('\n')
                            for line in cte_context_lines:
                                if line.strip():
                                    llm_context_parts.append(f"    {line}")
                        else:
                            llm_context_parts.append(f"    ERROR tracing CTE: {cte_trace['error']}")
                    else:
                        llm_context_parts.append(f"    WARNING: CTE '{cte_name}' not found in registry")
                        
                else:
                    # Regular table reference
                    llm_context_parts.append(f"  └─ EXTERNAL TABLE: {table}.{column} (referenced as {alias}.{column})")
                    next_columns.append({
                        "table": table,
                        "column": column,
                        "context": f"External table dependency for {target_column_name}",
                        "level": "external_table"
                    })
            
            else:
                # Multiple dependencies from same table - group them
                table_name = table_key.replace("CTE:", "") if table_key.startswith("CTE:") else table_key
                columns = [col for _, _, col in table_sources]
                
                llm_context_parts.append(f"  └─ MULTIPLE DEPENDENCIES FROM {table_name}:")
                for table, alias, column in table_sources:
                    llm_context_parts.append(f"    • {alias}.{column}")
                
                if table_key.startswith("CTE:"):
                    # Multiple CTE dependencies - consolidate them
                    cte_name = table_key.replace("CTE:", "")
                    llm_context_parts.append(f"  └─ CONSOLIDATED CTE ANALYSIS for '{cte_name}':")
                    
                    if cte_name.lower() in cte_registry:
                        cte_query = cte_registry[cte_name.lower()]
                        cte_sql = cte_query.sql(dialect="snowflake")
                        
                        # Get all unique external dependencies from this CTE
                        all_cte_deps = set()
                        cte_column_details = []
                        
                        for table, alias, column in table_sources:
                            cte_trace = trace_column_lineage(cte_sql, column)
                            if "error" not in cte_trace:
                                cte_column_details.append({
                                    "column": column,
                                    "trace": cte_trace
                                })
                                
                                # Collect external dependencies
                                for cte_dep in cte_trace.get("next_columns_to_search", []):
                                    dep_key = (cte_dep["table"], cte_dep["column"])
                                    all_cte_deps.add(dep_key)
                        
                        # Add consolidated CTE transformation
                        cte_transformations.append({
                            "cte_name": cte_name,
                            "columns": columns,  # Multiple columns - use 'columns' key
                            "transformation_type": "consolidated_cte",
                            "details": f"CTE processes {len(columns)} columns: {', '.join(columns)}",
                            "column_details": cte_column_details
                        })
                        
                        # Add unique external dependencies
                        for dep_table, dep_column in all_cte_deps:
                            next_columns.append({
                                "table": dep_table,
                                "column": dep_column,
                                "context": f"External source for {target_column_name} via consolidated CTE {cte_name}",
                                "level": "external_via_cte",
                                "cte_intermediate": cte_name
                            })
                        
                        # Show consolidated CTE analysis
                        llm_context_parts.append(f"    └─ CTE '{cte_name}' processes {len(columns)} output columns")
                        llm_context_parts.append(f"    └─ External dependencies: {len(all_cte_deps)} unique sources")
                        
                else:
                    # Multiple dependencies from regular table
                    for table, alias, column in table_sources:
                        next_columns.append({
                            "table": table,
                            "column": column,
                            "context": f"External table dependency for {target_column_name}",
                            "level": "external_table"
                        })
    
    # Show CTE transformations summary with safe access to column/columns
    if cte_transformations:
        llm_context_parts.append(f"\nINTRA-FILE CTE TRANSFORMATIONS:")
        for cte_info in cte_transformations:
            # FIXED: Handle both 'column' and 'columns' keys safely
            if 'column' in cte_info:
                column_info = cte_info['column']
            elif 'columns' in cte_info:
                columns_list = cte_info['columns']
                if isinstance(columns_list, list):
                    column_info = ', '.join(columns_list)
                else:
                    column_info = str(columns_list)
            else:
                column_info = 'unknown'
            
            llm_context_parts.append(f"  CTE '{cte_info['cte_name']}' transforms {column_info}")
            llm_context_parts.append(f"    └─ Type: {cte_info.get('transformation_type', 'unknown')}")
    
    # Show CTE definitions if relevant
    if cte_registry:
        llm_context_parts.append(f"\nAVAILABLE CTEs IN THIS FILE:")
        for cte_name in cte_registry.keys():
            llm_context_parts.append(f"  - {cte_name}")
    
    # Remove duplicates from next_columns
    unique_next_columns = []
    seen = set()
    for col in next_columns:
        key = (col['table'], col['column'], col['level'])
        if key not in seen:
            seen.add(key)
            unique_next_columns.append(col)
    
    return {
        "llm_context": "\n".join(llm_context_parts),
        "next_columns_to_search": unique_next_columns,
        "cte_transformations": cte_transformations,
        "full_lineage": target_column_info
    }


# Example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='DBT Multi-File Column Lineage Tracer')
    parser.add_argument('table', help='Name of the presentation table to analyze')
    parser.add_argument('column', help='Name of the column to trace')
    parser.add_argument('--compiled-dir', default='target/compiled', 
                        help='Path to dbt compiled SQL directory (default: target/compiled)')
    parser.add_argument('--internal-prefixes', nargs='+', default=['ph_'], 
                        help='Database prefixes for internal tables (default: ph_)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                        help='Show detailed LLM-ready technical context')
    parser.add_argument('--source-definitions', '-s', type=str, 
                        help='Path to JSON file containing source column definitions')
    parser.add_argument('--manifest', type=str, 
                        help='Path to DBT manifest.json file (default: target/manifest.json)')
    
    args = parser.parse_args()
    
    print("🔧 DBT COLUMN LINEAGE TRACER")
    print("="*50)
    print(f"📋 Table: {args.table}")
    print(f"📋 Column: {args.column}")
    print(f"📁 Compiled Directory: {args.compiled_dir}")
    print(f"🏢 Internal Prefixes: {args.internal_prefixes}")
    if args.source_definitions:
        print(f"📚 Source Definitions: {args.source_definitions}")
    if args.manifest:
        print(f"📋 Manifest File: {args.manifest}")
    
    try:
        # Always show quick summary
        print("\n" + "="*80)
        print("🚀 QUICK LINEAGE SUMMARY:")
        quick_result, tracer_instance = quick_lineage_summary(
            args.compiled_dir, 
            args.table, 
            args.column, 
            args.internal_prefixes, 
            args.source_definitions,
            args.manifest
        )
        
        # Show detailed analysis if verbose flag is used
        if args.verbose and quick_result:
            print("\n" + "="*80)
            print("🤖 COMPREHENSIVE TECHNICAL ANALYSIS:")
            
            include_source_defs = bool(args.source_definitions)
            llm_ready_context = format_context_for_llm(quick_result, include_source_defs, tracer_instance)
            print(llm_ready_context)
            print("\n✅ Detailed column lineage analysis complete!")
        elif args.verbose:
            print("\n❌ Cannot show detailed analysis due to errors in lineage tracing")
        else:
            print("\n💡 Use --verbose flag for detailed LLM-ready technical context")
            
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()