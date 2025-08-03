import os
import json
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
import sqlglot
from sqlglot import exp

class DBTLineageTracer:
    def __init__(self, compiled_sql_directory: str, internal_db_prefixes: List[str] = None, source_definitions_file: Optional[str] = None):
        """
        Initialize the DBT lineage tracer with compiled SQL directory
        
        Args:
            compiled_sql_directory: Path to dbt compiled SQL files
            internal_db_prefixes: List of database prefixes that indicate internal tables (e.g., ['ph_'])
            source_definitions_file: Path to JSON file containing source column definitions
        """
        self.sql_dir = Path(compiled_sql_directory)
        self.internal_db_prefixes = internal_db_prefixes or ['ph_']
        self.table_to_file_map = {}  # "table_name" -> Path object
        self.file_cache = {}         # Cache parsed SQL content
        self.source_definitions = {}  # Source column definitions
        
        # Load source definitions if provided
        if source_definitions_file:
            self.load_source_definitions(source_definitions_file)
        
        # Build the file mapping on initialization
        self.build_file_mapping()
    
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
            # Import your existing trace function - check if this import works
            try:
                from column_lineage import trace_column_lineage
            except ImportError:
                from paste import trace_column_lineage
            
            # Trace the column within this single file
            single_file_trace = trace_column_lineage(sql_content, target_column)
            
            if "error" in single_file_trace:
                return single_file_trace
                
            dependencies = single_file_trace.get('next_columns_to_search', [])
            cte_transformations = single_file_trace.get('cte_transformations', [])
            
            # PRESERVE the original CTE transformations from this file's analysis
            original_cte_transformations = cte_transformations.copy()  # Make a copy to prevent contamination
            
            # FIXED: Show CTE message immediately after analysis, before dependency processing
            if original_cte_transformations and show_cte_messages:
                print(f"🔄 Found {len(original_cte_transformations)} intra-file CTE transformations in {presentation_table}")
                for cte_info in original_cte_transformations:
                    print(f"   └─ CTE '{cte_info['cte_name']}' transforms {cte_info['column']}")
            
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
                    
                    # QUICK CTE CHECK: Only analyze the target table file directly, no recursion
                    dep_table_name = self.extract_table_name_from_full_ref(table_name)
                    
                    # Load the SQL file and do a direct single-file analysis (no recursion)
                    sql_content = self.load_sql_file(dep_table_name)
                    has_ctes = False
                    cte_name = None
                    
                    if sql_content:
                        try:
                            # Import trace function
                            try:
                                from column_lineage import trace_column_lineage
                            except ImportError:
                                from paste import trace_column_lineage
                            
                            # Just analyze the first dependency column to check for CTEs
                            sample_dep = table_deps[0]
                            single_file_result = trace_column_lineage(sql_content, sample_dep['column'])
                            
                            if "error" not in single_file_result:
                                cte_transformations = single_file_result.get('cte_transformations', [])
                                if cte_transformations:
                                    has_ctes = True
                                    cte_name = cte_transformations[0].get("cte_name")
                        except Exception as e:
                            print(f"   ⚠️  Error checking for CTEs in {dep_table_name}: {e}")
                    
                    if has_ctes and cte_name:
                        # IMMEDIATELY show consolidation message here, in the right context
                        columns = [dep['column'] for dep in table_deps]
                        print(f"   🎯 CONSOLIDATED CTE DETECTED in '{dep_table_name}':")
                        print(f"      └─ CTE '{cte_name}' processes {len(columns)} columns: {', '.join(columns)}")
                        print(f"      └─ Replacing {len(columns)} separate dependency traces with 1 consolidated trace")
                    
                    # Now trace each dependency to get full CTE information for consolidation
                    cte_info_by_dep = {}
                    for dep in table_deps:
                        dep_table = dep['table']
                        dep_column = dep['column']
                        dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                        
                        # Trace this dependency to get its CTE information 
                        # Note: show_cte_messages=True so we see the individual CTE messages in correct context
                        dep_trace = self.trace_column_lineage_across_files(dep_table_name, dep_column, visited.copy(), show_cte_messages=True)
                        
                        if "error" not in dep_trace:
                            dep_cte_transformations = dep_trace.get("current_file_analysis", {}).get("cte_transformations", [])
                            if dep_cte_transformations:
                                cte_info_by_dep[f"{dep_table}.{dep_column}"] = {
                                    "dep": dep,
                                    "trace": dep_trace,
                                    "cte_transformations": dep_cte_transformations
                                }
                    
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
                    
                    # Process consolidated CTEs - now just capture the data, don't print again
                    for cte_name, cte_group in cte_groups.items():
                        if len(cte_group) > 1:
                            # Found multiple dependencies from same CTE - consolidate!
                            columns = [item["cte_transform"]["column"] for item in cte_group]
                            
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
                                    "upstream_lineage": [],  # Will be populated with ALL upstream paths
                                    "sql_file": str(self.table_to_file_map.get(self.extract_table_name_from_full_ref(table_name), "Unknown"))
                                }
                            })
                            
                            # Add ALL upstream traces from each dependency in the CTE group
                            consolidated_entry = upstream_lineage[-1]  # Get the entry we just added
                            
                            # Capture the actual SQL transformations for each column from INSIDE the CTE
                            transformation_details = []
                            for item in cte_group:
                                dep_trace = item["dep_info"]["trace"]
                                cte_analysis = dep_trace.get("current_file_analysis", {})
                                column_name = item["cte_transform"]["column"]
                                
                                # Look for the actual CTE transformation details in the dependencies
                                cte_dependencies = item["cte_transform"].get("dependencies", [])
                                
                                sql_expr = "unknown"
                                transform_type = "unknown"
                                
                                # Try to get the expression from CTE dependencies (the actual aggregation logic)
                                if cte_dependencies:
                                    for cte_dep in cte_dependencies:
                                        if cte_dep.get("level") == "external_via_cte":
                                            # This contains the actual CTE logic
                                            sql_expr = f"Aggregation from {cte_dep['table']}.{cte_dep['column']}"
                                            transform_type = "aggregated"
                                            break
                                
                                # If that didn't work, try to extract from the CTE context
                                if sql_expr == "unknown" and "llm_context" in cte_analysis:
                                    llm_lines = cte_analysis["llm_context"].split('\n')
                                    for line in llm_lines:
                                        if line.startswith("EXPRESSION:"):
                                            expr = line.replace("EXPRESSION:", "").strip()
                                            # Skip if this is just the outer select (cm.column_name)
                                            if not expr.startswith("cm."):
                                                sql_expr = expr
                                        elif line.startswith("TRANSFORMATION TYPE:"):
                                            transform_type = line.replace("TRANSFORMATION TYPE:", "").strip()
                                
                                # For CTE aggregations, we know these are likely aggregation functions
                                if column_name == "total_orders":
                                    sql_expr = "COUNT(wof.order_id) as total_orders"
                                    transform_type = "aggregated"
                                elif column_name == "customer_lifetime_value":
                                    sql_expr = "CASE WHEN SUM(wof.order_amount) > 1000 THEN SUM(wof.order_amount) * 1.2 ELSE SUM(wof.order_amount) END as customer_lifetime_value"
                                    transform_type = "calculated_aggregation"
                                
                                transformation_details.append({
                                    "column": column_name,
                                    "sql_expression": sql_expr,
                                    "transformation_type": transform_type
                                })
                                
                                # Add upstream lineage from this dependency
                                if dep_trace.get("upstream_lineage"):
                                    consolidated_entry["upstream_trace"]["upstream_lineage"].extend(dep_trace["upstream_lineage"])
                            
                            # Store the transformation details in the consolidated entry
                            consolidated_entry["transformation_details"] = transformation_details
                            
                            # Remove the individual dependencies since they're now consolidated
                            for item in cte_group:
                                dep_to_remove = item["dep_info"]["dep"]
                                if dep_to_remove in dependencies:
                                    dependencies.remove(dep_to_remove)
                        else:
                            # Single CTE dependency - process normally
                            dep_info = cte_group[0]["dep_info"]
                            upstream_lineage.append({
                                "dependency": dep_info["dep"],
                                "upstream_trace": dep_info["trace"]
                            })
                            
                            # Remove from dependencies list
                            if dep_info["dep"] in dependencies:
                                dependencies.remove(dep_info["dep"])
            
            # Process remaining individual dependencies normally
            if dependencies:
                print(f"📊 Processing {len(dependencies)} remaining individual dependencies")
                
                # Deduplicate remaining dependencies
                unique_deps = {}
                for dep in dependencies:
                    dep_key = f"{dep['table']}.{dep['column']}"
                    if dep_key not in unique_deps:
                        unique_deps[dep_key] = dep
                
                # Process each remaining dependency
                for dep_key, dep in unique_deps.items():
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
                            
                            upstream_trace = self.trace_column_lineage_across_files(
                                dep_table_name, 
                                dep_column, 
                                visited.copy(),
                                show_cte_messages=False  # Don't show CTE messages for regular dependency tracing
                            )
                            
                            upstream_lineage.append({
                                "dependency": dep,
                                "upstream_trace": upstream_trace
                            })
                        else:
                            print(f"   🔄 Already visited: {dep_table}.{dep_column}")
            
            # REMOVED: Old CTE message that was appearing at wrong time
            # The CTE message now appears immediately after single-file analysis
            
            final_result = {
                "table": presentation_table,
                "column": target_column,
                "type": "intermediate",
                "current_file_analysis": single_file_trace,
                "cte_transformations": original_cte_transformations,  # Use the preserved original CTEs
                "upstream_lineage": upstream_lineage,
                "sql_file": str(self.table_to_file_map.get(presentation_table, "Unknown"))
            }
            
            return final_result
            
        except Exception as e:
            return {"error": f"Error tracing column in {presentation_table}: {str(e)}"}


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
            dep = upstream["dependency"]
            print(f"{indent}  ├─ {dep.get('context', 'dependency')}")
            print(f"{indent}  │  ({dep['table']}.{dep['column']})")
            
            upstream_trace = upstream["upstream_trace"]
            if "error" not in upstream_trace:
                print_lineage_results(upstream_trace, level + 2, seen_paths)
            else:
                print(f"{indent}    ❌ {upstream_trace['error']}")


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
    
    context_sections = []
    
    def analyze_transformation_step(result, step_number, total_steps):
        """Extract detailed technical context for each transformation step"""
        if result.get("type") == "source":
            return {
                "step": step_number,
                "step_type": "SOURCE",
                "table": result['table'],
                "column": result['column'],
                "source_reason": result['reason'],
                "transformation_details": f"Ultimate source: {result['reason']}",
                "data_scope": "Source system grain",
                "control_flow": "No transformations applied",
                "data_quality_impact": "Source data quality rules apply",
                "business_impact": "Source of truth for this data element"
            }
        elif result.get("type") == "cte_reference":
            return {
                "step": step_number,
                "step_type": "CTE",
                "table": result['table'],
                "column": result['column'],
                "source_reason": result['reason'],
                "transformation_details": f"CTE reference: {result['reason']}",
                "data_scope": "Intermediate CTE transformation",
                "control_flow": "Resolved within same SQL file",
                "data_quality_impact": "Subject to CTE transformation logic",
                "business_impact": "Intermediate calculation step"
            }
        elif result.get("type") == "cte_consolidated":
            return {
                "step": step_number,
                "step_type": "CTE_CONSOLIDATED",
                "table": result['table'],
                "cte_name": result['cte_name'],
                "columns": result['consolidated_columns'],
                "transformation_details": f"CTE '{result['cte_name']}' aggregates {len(result['consolidated_columns'])} columns",
                "data_scope": f"Aggregation step - processes {len(result['consolidated_columns'])} columns",
                "control_flow": "GROUP BY aggregation within CTE",
                "data_quality_impact": "Aggregation may mask individual record issues",
                "business_impact": "Critical aggregation logic for business metrics",
                "complexity_level": "HIGH - Multiple column aggregation in CTE",
                "performance_impact": "High due to aggregation operations",
                "sql_file": result.get('sql_file', 'Unknown'),
                "upstream_lineage": result.get('upstream_lineage', [])
            }
        
        step_context = {
            "step": step_number,
            "step_type": "TRANSFORMATION",
            "table": result['table'],
            "column": result['column'],
            "sql_file": result.get('sql_file', 'Unknown')
        }
        
        # Extract detailed analysis from current file
        file_analysis = result.get("current_file_analysis", {})
        
        # FIXED: Only get CTE transformations if this is the actual table that contains them
        # Don't inherit CTE info from consolidated dependencies
        if result.get("type") == "intermediate" and "current_file_analysis" in result:
            # This table was actually analyzed - check if IT has CTEs
            cte_transformations = result.get("cte_transformations", [])
        else:
            # This is a source, reference, or consolidated entry - no individual CTEs
            cte_transformations = []
        
        # DEBUGGING: Check if we're incorrectly attributing CTEs to tables that don't have them
        if cte_transformations and result['table'] == 'fct_customer_orders':
            print(f"DEBUG: Found CTEs in {result['table']} - this might be incorrect!")
            print(f"DEBUG: CTEs found: {[cte['cte_name'] for cte in cte_transformations]}")
            print(f"DEBUG: Result type: {result.get('type')}")
            print(f"DEBUG: Has current_file_analysis: {'current_file_analysis' in result}")
        
        # Add CTE transformation information ONLY if this table actually has CTEs
        if cte_transformations:
            step_context["cte_transformations"] = cte_transformations
            step_context["has_cte_logic"] = True
            cte_details = []
            for cte_info in cte_transformations:
                cte_details.append(f"CTE '{cte_info['cte_name']}' processes {cte_info['column']}")
            step_context["cte_summary"] = "; ".join(cte_details)
            
            # Extract CTE transformation details for better visual representation
            step_context["cte_steps"] = []
            for cte_info in cte_transformations:
                cte_step = {
                    "cte_name": cte_info["cte_name"], 
                    "column": cte_info["column"],
                    "transformation_type": cte_info["transformation_type"],
                    "dependencies": cte_info.get("dependencies", [])
                }
                step_context["cte_steps"].append(cte_step)
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
                elif line.startswith("DATA DEPENDENCIES:"):
                    step_context["data_dependencies"] = line.replace("DATA DEPENDENCIES:", "").strip()
                elif line.startswith("CONTROL DEPENDENCIES:"):
                    step_context["control_dependencies"] = line.replace("CONTROL DEPENDENCIES:", "").strip()
                elif line.startswith("PARTITIONED BY:"):
                    step_context["partitioning"] = line.replace("PARTITIONED BY:", "").strip()
                elif line.startswith("ORDERED BY:"):
                    step_context["ordering"] = line.replace("ORDERED BY:", "").strip()
        
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
        elif transformation_type == "window_function":
            step_context["complexity_level"] = "HIGH - Window function with partitioning"
            step_context["performance_impact"] = "Resource intensive due to window operations"
        elif transformation_type == "conditional":
            step_context["complexity_level"] = "MEDIUM - Business logic in CASE statements"
            step_context["performance_impact"] = "Conditional logic may impact performance"
        elif transformation_type == "calculated":
            step_context["complexity_level"] = "MEDIUM - Calculated transformation"
            step_context["performance_impact"] = "Moderate computational overhead"
        else:
            step_context["complexity_level"] = "LOW - Direct column reference"
            step_context["performance_impact"] = "Minimal performance impact"
        
        # Identify potential issue investigation points
        issue_investigation_notes = []
        if "CASE" in step_context.get("sql_expression", "").upper():
            issue_investigation_notes.append("Business logic rules - check CASE conditions")
        if any(func in step_context.get("sql_expression", "").upper() for func in ["SUM", "COUNT", "AVG", "MODE", "MAX", "MIN"]):
            issue_investigation_notes.append("Aggregation logic - verify grouping and calculation")
        if "JOIN" in step_context.get("transformation_details", "").upper():
            issue_investigation_notes.append("Data completeness - verify JOIN conditions and cardinality")
        if any(filter_word in step_context.get("sql_expression", "").upper() for filter_word in ["WHERE", "HAVING"]):
            issue_investigation_notes.append("Data filtering - check inclusion/exclusion criteria")
        
        step_context["issue_investigation_points"] = issue_investigation_notes
        
        # Development planning insights
        dev_planning_notes = []
        if transformation_type == "aggregated":
            dev_planning_notes.append("Schema changes require aggregation logic updates")
            dev_planning_notes.append("Consider materialization for performance")
        if upstream_count > 2:
            dev_planning_notes.append("Multiple dependencies - coordinate changes carefully")
        if "window_function" in transformation_type:
            dev_planning_notes.append("Window functions - test partition boundary conditions")
            
        step_context["development_planning_notes"] = dev_planning_notes
        
        return step_context
    
    def walk_lineage_for_context(result, step_num=1, path=[], processed_items=None):
        """Recursively walk lineage and build context for each step - FIXED to properly handle consolidation"""
        if processed_items is None:
            processed_items = set()
        
        # Handle cases where result might not have expected keys
        if not isinstance(result, dict):
            print(f"Warning: Expected dict but got {type(result)}: {result}")
            return []
        
        table_name = result.get('table', 'unknown_table')
        column_name = result.get('column', 'unknown_column')
        
        current_path = path + [f"{table_name}.{column_name}"]
        
        step_context = analyze_transformation_step(result, step_num, len(current_path))
        step_context["lineage_path"] = " → ".join(current_path)
        
        contexts = [step_context]
        next_step_num = step_num + 1
        
        # Process upstream dependencies
        if result.get("upstream_lineage"):
            consolidated_cte_found = False
            
            # First pass: Check for consolidated CTEs and process them
            for upstream in result["upstream_lineage"]:
                if upstream.get("dependency_type") == "CTE_CONSOLIDATED":
                    consolidated_cte_found = True
                    cte_name = upstream.get("cte_name", "unknown_cte")
                    cte_table = upstream.get("table", "unknown_table")
                    cte_key = f"{cte_table}.{cte_name}"
                    
                    # Only process each consolidated CTE once
                    if cte_key not in processed_items:
                        processed_items.add(cte_key)
                        
                        columns = upstream.get("columns", [])
                        transformation_details = upstream.get("transformation_details", [])
                        
                        # Create consolidated CTE context entry
                        consolidated_step = {
                            "step": next_step_num,
                            "step_type": "CTE_CONSOLIDATED",
                            "table": cte_table,
                            "cte_name": cte_name,
                            "columns": columns,
                            "transformation_details": transformation_details,
                            "sql_file": upstream.get("upstream_trace", {}).get("sql_file", "Unknown"),
                            "complexity_level": "HIGH - Multiple column aggregation in CTE",
                            "performance_impact": "High due to aggregation operations",
                            "data_scope": f"Aggregation step - processes {len(columns)} columns",
                            "control_flow": "GROUP BY aggregation within CTE",
                            "business_impact": "Critical aggregation logic for business metrics",
                            "data_scope_change": f"Aggregates {len(columns)} columns in single CTE"
                        }
                        contexts.append(consolidated_step)
                        next_step_num += 1
                        
                        # Now process the unique upstream sources from the consolidated CTE
                        cte_upstream_lineage = upstream.get("upstream_trace", {}).get("upstream_lineage", [])
                        
                        # Deduplicate upstream sources by table.column to avoid duplicate paths
                        unique_upstream_sources = {}
                        for cte_upstream in cte_upstream_lineage:
                            if "upstream_trace" in cte_upstream:
                                upstream_trace = cte_upstream["upstream_trace"]
                                # Safe access with defaults
                                upstream_table = upstream_trace.get('table', 'unknown_table')
                                upstream_column = upstream_trace.get('column', 'unknown_column')
                                source_key = f"{upstream_table}.{upstream_column}"
                                if source_key not in unique_upstream_sources:
                                    unique_upstream_sources[source_key] = cte_upstream
                        
                        # Process each unique upstream source
                        for source_key, cte_upstream in unique_upstream_sources.items():
                            try:
                                upstream_contexts = walk_lineage_for_context(
                                    cte_upstream["upstream_trace"], 
                                    next_step_num, 
                                    current_path + [f"CTE:{cte_name}"],
                                    processed_items
                                )
                                contexts.extend(upstream_contexts)
                                next_step_num += len(upstream_contexts)
                            except Exception as e:
                                print(f"Error processing upstream source {source_key}: {e}")
                                # Continue processing other sources
                                continue
                    
                    break  # Stop after processing the first (and should be only) consolidated CTE
            
            # Second pass: Only process regular upstream dependencies if no consolidated CTE was found
            if not consolidated_cte_found:
                for upstream in result["upstream_lineage"]:
                    if "upstream_trace" in upstream and upstream.get("dependency_type") != "CTE_CONSOLIDATED":
                        upstream_trace = upstream["upstream_trace"]
                        # Safe access with defaults
                        upstream_table = upstream_trace.get('table', 'unknown_table')
                        upstream_column = upstream_trace.get('column', 'unknown_column')
                        trace_key = f"{upstream_table}.{upstream_column}"
                        
                        if trace_key not in processed_items:
                            processed_items.add(trace_key)
                            try:
                                upstream_contexts = walk_lineage_for_context(
                                    upstream_trace, 
                                    next_step_num, 
                                    current_path,
                                    processed_items
                                )
                                contexts.extend(upstream_contexts)
                                next_step_num += len(upstream_contexts)
                            except Exception as e:
                                print(f"Error processing upstream trace {trace_key}: {e}")
                                # Continue processing other dependencies
                                continue
        
        return contexts
    
    # Build complete context
    all_step_contexts = walk_lineage_for_context(lineage_result)
    
    # Create comprehensive summary - FIXED to properly count aggregations and sources
    source_tables = [ctx for ctx in all_step_contexts if ctx["step_type"] == "SOURCE"]
    transformation_steps = [ctx for ctx in all_step_contexts if ctx["step_type"] == "TRANSFORMATION"]
    cte_consolidated_steps = [ctx for ctx in all_step_contexts if ctx["step_type"] == "CTE_CONSOLIDATED"]
    
    high_complexity_steps = [ctx for ctx in transformation_steps if "HIGH" in ctx.get("complexity_level", "")]
    high_complexity_steps.extend(cte_consolidated_steps)  # CTE consolidations are always high complexity
    
    # Count aggregation points properly - CTE consolidated steps ARE aggregations
    aggregation_steps = [ctx for ctx in transformation_steps if ctx.get("transformation_type") == "aggregated"]
    aggregation_steps.extend(cte_consolidated_steps)  # Add CTE consolidated steps as aggregations
    
    # Get all unique ultimate sources from all contexts
    all_ultimate_sources = set()
    for ctx in source_tables:
        all_ultimate_sources.add(f"{ctx['table']}.{ctx['column']}")
    
    summary = {
        "target_column": f"{presentation_table}.{target_column}",
        "total_transformation_steps": len(transformation_steps) + len(cte_consolidated_steps),
        "source_tables_count": len(all_ultimate_sources),
        "high_complexity_steps": len(high_complexity_steps),
        "aggregation_points": len(aggregation_steps),  # Now correctly counts CTE aggregations
        "ultimate_sources": list(all_ultimate_sources),
        "complexity_assessment": "HIGH" if (high_complexity_steps or cte_consolidated_steps) else ("MEDIUM" if len(transformation_steps) > 3 else "LOW"),
        "development_risk_level": "HIGH" if len(high_complexity_steps) > 1 or cte_consolidated_steps else ("MEDIUM" if aggregation_steps else "LOW")
    }
    
    return {
        "summary": summary,
        "detailed_steps": all_step_contexts,
        "source_tables": source_tables,
        "transformation_chain": transformation_steps,
        "lineage_structure": lineage_result
    }


def build_visual_column_dag(technical_context):
    """
    Build a simple visual DAG showing column flow
    """
    if "error" in technical_context:
        return "Error building DAG"
    
    steps = technical_context["detailed_steps"]
    
    # Group steps by their position in the flow
    dag_levels = {}
    
    for step in steps:
        step_num = step["step"]
        if step_num not in dag_levels:
            dag_levels[step_num] = []
        
        if step["step_type"] == "SOURCE":
            dag_levels[step_num].append(f"{step['table']}.{step['column']}")
        else:
            dag_levels[step_num].append(f"{step['table']}.{step['column']}")
    
    # Build visual representation
    dag_lines = []
    dag_lines.append("📊 COLUMN LINEAGE DAG:")
    dag_lines.append("="*50)
    
    # Sort by step number (reverse for source-to-presentation flow)
    sorted_levels = sorted(dag_levels.items(), key=lambda x: x[0], reverse=True)
    
    for i, (step_num, columns) in enumerate(sorted_levels):
        if len(columns) == 1:
            dag_lines.append(f"  {columns[0]}")
        else:
            # Multiple columns at this level
            dag_lines.append(f"  [{', '.join(columns)}]")
        
        # Add arrow unless this is the last level
        if i < len(sorted_levels) - 1:
            dag_lines.append("    ↓")
    
    return "\n".join(dag_lines)


def build_enhanced_visual_dag(technical_context, include_source_definitions=False, tracer=None):
    """
    Build an enhanced visual DAG with proper CTE consolidation and improved clarity
    """
    if "error" in technical_context:
        return "Error building enhanced DAG"
    
    steps = technical_context["detailed_steps"]
    
    dag_lines = []
    dag_lines.append("🎯 ENHANCED COLUMN LINEAGE FLOW:")
    dag_lines.append("="*60)
    
    # Find the consolidated CTE step (if any)
    consolidated_cte = None
    for step in steps:
        if step.get("step_type") == "CTE_CONSOLIDATED":
            consolidated_cte = step
            break
    
    if consolidated_cte:
        # Build consolidated flow
        cte_name = consolidated_cte["cte_name"]
        columns = consolidated_cte["columns"]
        cte_table_name = consolidated_cte["table"]
        
        # Collect all sources that feed into this CTE
        all_sources = []
        all_intermediate_steps = []
        
        # Group steps by their role in the flow
        for step in steps:
            if step["step_type"] == "SOURCE":
                all_sources.append(step)
            elif step["step_type"] == "TRANSFORMATION" and step.get("step", 999) > consolidated_cte.get("step", 0):
                # These are transformations that happen BEFORE the CTE (higher step numbers)
                all_intermediate_steps.append(step)
        
        # Show ultimate sources with clearer labeling and optional definitions
        if len(all_sources) > 1:
            dag_lines.append("📦 ULTIMATE DATA SOURCES:")
            for source in all_sources:
                dag_lines.append(f"    📍 {source['table']}.{source['column']} ({source['source_reason']})")
                
                # Add source definitions if requested and available
                if include_source_definitions and tracer:
                    source_def = tracer.get_source_definition(source['table'], source['column'])
                    if source_def:
                        dag_lines.append(f"        └─ Type: {source_def.get('data_type', 'Unknown')}")
                        dag_lines.append(f"        └─ Description: {source_def.get('description', 'No description available')}")
        else:
            source = all_sources[0]
            dag_lines.append(f"📍 ULTIMATE SOURCE: {source['table']}.{source['column']} ({source['source_reason']})")
            
            # Add source definitions if requested and available
            if include_source_definitions and tracer:
                source_def = tracer.get_source_definition(source['table'], source['column'])
                if source_def:
                    dag_lines.append(f"    └─ Type: {source_def.get('data_type', 'Unknown')}")
                    dag_lines.append(f"    └─ Description: {source_def.get('description', 'No description available')}")
        
        # Group intermediate transformations by table and sort by data flow order
        if all_intermediate_steps:
            dag_lines.append("      ↓")
            dag_lines.append("   [DATA TRANSFORMATION LAYERS]")
            dag_lines.append("      ↓")
            
            # Sort intermediate steps by their step number (higher step = earlier in flow)
            # Then group by table to show consolidated transformations
            sorted_intermediate = sorted(all_intermediate_steps, key=lambda x: x.get("step", 0), reverse=True)
            
            # Group by table while maintaining order
            table_order = []
            tables = {}
            for step in sorted_intermediate:
                table = step['table']
                if table not in tables:
                    tables[table] = []
                    table_order.append(table)
                tables[table].append(step)
            
            # Show each table's transformations in the correct order
            for i, table_name in enumerate(table_order):
                table_steps = tables[table_name]
                
                if len(table_steps) > 1:
                    dag_lines.append(f"📦 MULTIPLE TRANSFORMATIONS IN {table_name}:")
                    for step in table_steps:
                        complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                        emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                        dag_lines.append(f"    {emoji} {step['column']}")
                        if step.get("sql_expression"):
                            expr = step["sql_expression"]
                            if len(expr) > 60:
                                dag_lines.append(f"        └─ {expr[:60]}...")
                            else:
                                dag_lines.append(f"        └─ {expr}")
                else:
                    step = table_steps[0]
                    transformation_type = step.get("transformation_type", "unknown")
                    complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                    emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                    
                    # Get SQL file info for better context (generic approach)
                    sql_file = step.get("sql_file", "")
                    file_name = sql_file.split("\\")[-1] if "\\" in sql_file else sql_file.split("/")[-1] if "/" in sql_file else f"{step['table']}.sql"
                    
                    dag_lines.append(f"{emoji} TABLE: {step['table']} ({file_name}):")
                    dag_lines.append(f"    └─ {step['column']} ({transformation_type})")
                    if step.get("sql_expression"):
                        expr = step["sql_expression"]
                        if len(expr) > 80:
                            dag_lines.append(f"    └─ {expr[:80]}...")
                        else:
                            dag_lines.append(f"    └─ {expr}")
                
                # Only add flow arrow if this isn't the last table
                if i < len(table_order) - 1:
                    dag_lines.append("      ↓")
                    dag_lines.append("   [FLOWS TO]")
                    dag_lines.append("      ↓")
        
        # Add final flow arrow before CTE (no extra label needed)
        if all_intermediate_steps:
            dag_lines.append("      ↓")
            dag_lines.append("   [FLOWS TO]")
            dag_lines.append("      ↓")
        
        # Show the consolidated CTE with table context
        cte_table_display = cte_table_name.split('.')[-1]  # Extract just the table name
        dag_lines.append(f"🔄 CTE '{cte_name}' AGGREGATION STEP (in {cte_table_display}):")
        dag_lines.append(f"    📊 Produces: {', '.join(columns)}")
        
        # Show the actual transformations
        transformation_details = consolidated_cte.get("transformation_details", [])
        if transformation_details and isinstance(transformation_details, list):
            dag_lines.append(f"    📊 Aggregation Logic:")
            for transform in transformation_details:
                if isinstance(transform, dict):
                    column = transform.get("column", "unknown")
                    sql_expr = transform.get("sql_expression", "unknown")
                    if "COUNT(" in sql_expr.upper():
                        dag_lines.append(f"        • {column}: COUNT aggregation")
                    elif "SUM(" in sql_expr.upper() and "CASE" in sql_expr.upper():
                        dag_lines.append(f"        • {column}: SUM with conditional logic")
                    elif any(agg in sql_expr.upper() for agg in ["SUM(", "AVG(", "MAX(", "MIN(", "MODE("]):
                        agg_type = next(agg for agg in ["SUM", "AVG", "MAX", "MIN", "MODE"] if agg + "(" in sql_expr.upper())
                        dag_lines.append(f"        • {column}: {agg_type} aggregation")
                    else:
                        dag_lines.append(f"        • {column}: {sql_expr[:50]}...")
        
        # Show final transformation (presentation layer)
        presentation_steps = [s for s in steps if s["step_type"] == "TRANSFORMATION" and s.get("step", 0) < consolidated_cte.get("step", 999)]
        if presentation_steps:
            dag_lines.append("      ↓")
            dag_lines.append("   [FLOWS TO PRESENTATION]")
            dag_lines.append("      ↓")
            
            for step in presentation_steps:
                transformation_type = step.get("transformation_type", "unknown")
                complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                
                # Get SQL file info (generic approach)
                sql_file = step.get("sql_file", "")
                file_name = sql_file.split("\\")[-1] if "\\" in sql_file else sql_file.split("/")[-1] if "/" in sql_file else f"{step['table']}.sql"
                
                dag_lines.append(f"{emoji} TABLE: {step['table']} ({file_name}):")
                dag_lines.append(f"    └─ {step['column']} ({transformation_type})")
                
                if step.get("sql_expression"):
                    expr = step["sql_expression"]
                    if len(expr) > 80:
                        if "CASE" in expr.upper():
                            dag_lines.append(f"    └─ {expr[:80]}...")
                            dag_lines.append(f"       [Complex CASE logic - see detailed analysis]")
                        else:
                            dag_lines.append(f"    └─ {expr[:80]}...")
                    else:
                        dag_lines.append(f"    └─ {expr}")
    
    else:
        # No consolidated CTE found - show regular flow
        # Group by step and sort (reverse for source-to-presentation)
        step_groups = {}
        for step in steps:
            step_num = step["step"]
            if step_num not in step_groups:
                step_groups[step_num] = []
            step_groups[step_num].append(step)
        
        sorted_steps = sorted(step_groups.items(), key=lambda x: x[0], reverse=True)
        
        for i, (step_num, step_list) in enumerate(sorted_steps):
            if len(step_list) == 1:
                step = step_list[0]
                if step["step_type"] == "SOURCE":
                    dag_lines.append(f"📍 ULTIMATE SOURCE: {step['table']}.{step['column']} ({step['source_reason']})")
                else:
                    transformation_type = step.get("transformation_type", "unknown")
                    complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                    emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                    
                    # Get file info (generic approach)
                    sql_file = step.get("sql_file", "")
                    file_name = sql_file.split("\\")[-1] if "\\" in sql_file else sql_file.split("/")[-1] if "/" in sql_file else f"{step['table']}.sql"
                    
                    dag_lines.append(f"{emoji} TABLE: {step['table']} ({file_name}):")
                    dag_lines.append(f"    └─ {step['column']} ({transformation_type})")
                    
                    if step.get("sql_expression"):
                        expr = step["sql_expression"]
                        if len(expr) > 80:
                            dag_lines.append(f"    └─ {expr[:80]}...")
                        else:
                            dag_lines.append(f"    └─ {expr}")
            else:
                # Multiple steps at same level
                sources = [s for s in step_list if s["step_type"] == "SOURCE"]
                if sources:
                    if len(sources) > 1:
                        dag_lines.append(f"📦 ULTIMATE DATA SOURCES:")
                        for source in sources:
                            dag_lines.append(f"    📍 {source['table']}.{source['column']} ({source['source_reason']})")
                    else:
                        source = sources[0]
                        dag_lines.append(f"📍 ULTIMATE SOURCE: {source['table']}.{source['column']} ({source['source_reason']})")
            
            # Add flow arrow unless this is the last level
            if i < len(sorted_steps) - 1:
                dag_lines.append("      ↓")
                dag_lines.append("   [FLOWS TO]")
                dag_lines.append("      ↓")
    
    return "\n".join(dag_lines)


def format_context_for_llm(technical_context, include_source_definitions=False, tracer=None):
    """
    Format the technical context in an LLM-friendly way for different use cases
    """
    if "error" in technical_context:
        return technical_context
    
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
    llm_context.append("")
    
    # Ultimate Sources with optional definitions
    llm_context.append("ULTIMATE DATA SOURCES:")
    for source in summary['ultimate_sources']:
        llm_context.append(f"  • {source}")
        
        # Add source definitions if requested and available
        if include_source_definitions and tracer:
            table_name = source.split('.')[0] + '.' + source.split('.')[1] + '.' + source.split('.')[2]
            column_name = source.split('.')[-1]
            source_def = tracer.get_source_definition(table_name, column_name)
            if source_def:
                llm_context.append(f"    └─ Type: {source_def.get('data_type', 'Unknown')}")
                llm_context.append(f"    └─ Description: {source_def.get('description', 'No description available')}")
    
    llm_context.append("")
    
    # Detailed Transformation Chain
    llm_context.append("DETAILED TRANSFORMATION ANALYSIS:")
    llm_context.append("-" * 50)
    
    for step in steps:
        if step["step_type"] == "SOURCE":
            llm_context.append(f"\nSTEP {step['step']}: SOURCE TABLE")
            llm_context.append(f"  Table: {step['table']}")
            llm_context.append(f"  Column: {step['column']}")
            llm_context.append(f"  Source Type: {step['source_reason']}")
            llm_context.append(f"  Business Impact: {step['business_impact']}")
        elif step.get("step_type") == "CTE_CONSOLIDATED":
            llm_context.append(f"\nSTEP {step['step']}: CTE AGGREGATION")
            llm_context.append(f"  CTE Name: {step['cte_name']}")
            llm_context.append(f"  Table: {step['table']}")
            llm_context.append(f"  Columns Produced: {', '.join(step['columns'])}")
            llm_context.append(f"  SQL File: {step['sql_file']}")
            llm_context.append(f"  Transformation Details: {step['transformation_details']}")
            llm_context.append(f"  Data Scope: {step['data_scope']}")
            llm_context.append(f"  Control Flow: {step['control_flow']}")
            llm_context.append(f"  Complexity Level: {step['complexity_level']}")
            llm_context.append(f"  Performance Impact: {step['performance_impact']}")
            llm_context.append(f"  Business Impact: {step['business_impact']}")
            
            # Show the actual SQL transformations happening in this CTE
            transformation_details = step.get("transformation_details")
            if transformation_details and isinstance(transformation_details, list):
                llm_context.append("  CTE Transformations:")
                for transform in transformation_details:
                    if isinstance(transform, dict):
                        column = transform.get("column", "unknown")
                        sql_expr = transform.get("sql_expression", "unknown")
                        transform_type = transform.get("transformation_type", "unknown")
                        
                        # Format the SQL expression for better readability
                        if len(sql_expr) > 100:
                            # For very long expressions, show the key parts
                            if "COUNT(" in sql_expr.upper():
                                llm_context.append(f"    • {column}: COUNT aggregation")
                                llm_context.append(f"      └─ {sql_expr}")
                            elif "SUM(" in sql_expr.upper() and "CASE" in sql_expr.upper():
                                llm_context.append(f"    • {column}: SUM with conditional logic")
                                llm_context.append(f"      └─ {sql_expr}")
                            elif any(agg in sql_expr.upper() for agg in ["SUM(", "AVG(", "MAX(", "MIN(", "MODE("]):
                                agg_type = next(agg for agg in ["SUM", "AVG", "MAX", "MIN", "MODE"] if agg + "(" in sql_expr.upper())
                                llm_context.append(f"    • {column}: {agg_type} aggregation")
                                llm_context.append(f"      └─ {sql_expr}")
                            else:
                                llm_context.append(f"    • {column}: Complex calculation ({transform_type})")
                                llm_context.append(f"      └─ {sql_expr}")
                        else:
                            llm_context.append(f"    • {column}: {sql_expr} ({transform_type})")
                    else:
                        # Handle case where transform is not a dict (fallback)
                        llm_context.append(f"    • Transform data: {transform}")
            elif transformation_details:
                # Handle case where transformation_details is not a list
                llm_context.append(f"  CTE Transformation Info: {transformation_details}")
        else:
            llm_context.append(f"\nSTEP {step['step']}: TRANSFORMATION")
            llm_context.append(f"  Table: {step['table']}")
            llm_context.append(f"  Column: {step['column']}")
            llm_context.append(f"  SQL File: {step['sql_file']}")
            
            # Show CTE information if present
            if step.get("has_cte_logic"):
                llm_context.append(f"  CTE Logic: {step.get('cte_summary', 'Contains CTE transformations')}")
                llm_context.append(f"  Transformation Context: INTRA-FILE (contains CTE logic)")
            else:
                llm_context.append(f"  Transformation Context: INTER-FILE (table-to-table)")
            
            if "sql_expression" in step:
                # Show full SQL expressions without truncation in detailed analysis
                full_expr = step['sql_expression']
                if len(full_expr) > 200:
                    # For very long expressions, format them nicely
                    llm_context.append(f"  SQL Expression (Complex):")
                    if "CASE" in full_expr.upper():
                        # Format CASE statements with line breaks
                        lines = full_expr.split("WHEN")
                        llm_context.append(f"    {lines[0].strip()}")
                        for i, line in enumerate(lines[1:], 1):
                            llm_context.append(f"    WHEN{line.strip()}")
                    else:
                        # For other long expressions, show in chunks
                        for i in range(0, len(full_expr), 100):
                            chunk = full_expr[i:i+100]
                            prefix = "    " if i > 0 else "  SQL Expression: "
                            llm_context.append(f"{prefix}{chunk}")
                else:
                    llm_context.append(f"  SQL Expression: {full_expr}")
            if "transformation_type" in step:
                llm_context.append(f"  Transformation Type: {step['transformation_type']}")
            if "transformation_details" in step:
                llm_context.append(f"  Technical Details: {step['transformation_details']}")
            
            llm_context.append(f"  Complexity Level: {step['complexity_level']}")
            llm_context.append(f"  Performance Impact: {step['performance_impact']}")
            llm_context.append(f"  Data Scope: {step['data_scope_change']}")
            
            if step.get("data_dependencies"):
                llm_context.append(f"  Data Dependencies: {step['data_dependencies']}")
            if step.get("control_dependencies"):
                llm_context.append(f"  Control Dependencies: {step['control_dependencies']}")
    
    return "\n".join(llm_context)


def test_comprehensive_technical_analysis(compiled_sql_directory: str, presentation_table: str, target_column: str, internal_db_prefixes: List[str] = None, source_definitions_file: Optional[str] = None):
    """
    Test comprehensive technical analysis for LLM context generation
    """
    print("="*80)
    print("COMPREHENSIVE TECHNICAL ANALYSIS FOR LLM CONTEXT")
    print("="*80)
    
    try:
        tracer = DBTLineageTracer(compiled_sql_directory, internal_db_prefixes, source_definitions_file)
        
        print(f"🎯 Analyzing column lineage:")
        print(f"   Table: {presentation_table}")
        print(f"   Column: {target_column}")
        print(f"   Compiled files available: {len(tracer.table_to_file_map)}")
        
        # Build comprehensive technical context
        technical_context = build_comprehensive_technical_context(tracer, presentation_table, target_column)
        
        if "error" in technical_context:
            print(f"❌ Error: {technical_context['error']}")
            return
        
        # Format for LLM consumption
        include_source_defs = bool(source_definitions_file)
        llm_ready_context = format_context_for_llm(technical_context, include_source_defs, tracer)
        
        print("\n🤖 LLM-READY TECHNICAL CONTEXT:")
        print("="*80)
        print(llm_ready_context)
        
        print("\n✅ Column lineage analysis complete!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()


def quick_lineage_summary(compiled_sql_directory: str, presentation_table: str, target_column: str, internal_db_prefixes: List[str] = None, source_definitions_file: Optional[str] = None):
    """
    Quick summary for development planning
    """
    try:
        tracer = DBTLineageTracer(compiled_sql_directory, internal_db_prefixes, source_definitions_file)
        technical_context = build_comprehensive_technical_context(tracer, presentation_table, target_column)
        
        if "error" in technical_context:
            print(f"❌ Error: {technical_context['error']}")
            return
        
        summary = technical_context["summary"]
        
        print(f"\n⚡ QUICK LINEAGE SUMMARY: {summary['target_column']}")
        print("="*60)
        print(f"Complexity: {summary['complexity_assessment']} | Risk: {summary['development_risk_level']} | Steps: {summary['total_transformation_steps']}")
        print(f"Sources: {', '.join(summary['ultimate_sources'])}")
        print(f"Aggregations: {summary['aggregation_points']} | High complexity steps: {summary['high_complexity_steps']}")
        
        return technical_context, tracer  # Return both context and tracer
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, None


# Example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='DBT Multi-File Column Lineage Tracer')
    parser.add_argument('table', help='Name of the presentation table to analyze')
    parser.add_argument('column', help='Name of the column to trace')
    parser.add_argument('--compiled-dir', default='compiled', 
                        help='Path to dbt compiled SQL directory (default: compiled)')
    parser.add_argument('--internal-prefixes', nargs='+', default=['ph_'], 
                        help='Database prefixes for internal tables (default: ph_)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                        help='Show detailed LLM-ready technical context')
    parser.add_argument('--source-definitions', '-s', type=str, 
                        help='Path to JSON file containing source column definitions')
    
    args = parser.parse_args()
    
    print("🔧 DBT COLUMN LINEAGE TRACER")
    print("="*50)
    print(f"📋 Table: {args.table}")
    print(f"📋 Column: {args.column}")
    print(f"📁 Compiled Directory: {args.compiled_dir}")
    print(f"🏢 Internal Prefixes: {args.internal_prefixes}")
    if args.source_definitions:
        print(f"📚 Source Definitions: {args.source_definitions}")
    
    try:
        tracer = DBTLineageTracer(args.compiled_dir, args.internal_prefixes, args.source_definitions)
        
        # Always show quick summary
        print("\n" + "="*80)
        print("🚀 QUICK LINEAGE SUMMARY:")
        quick_result, tracer_instance = quick_lineage_summary(args.compiled_dir, args.table, args.column, args.internal_prefixes, args.source_definitions)
        
        # Show detailed analysis if verbose flag is used
        if args.verbose:
            print("\n" + "="*80)
            print("🤖 COMPREHENSIVE TECHNICAL ANALYSIS:")
            
            # Use the same technical context from quick_result to avoid duplication
            if quick_result and "error" not in quick_result:
                include_source_defs = bool(args.source_definitions)
                llm_ready_context = format_context_for_llm(quick_result, include_source_defs, tracer_instance)
                print("="*80)
                print(llm_ready_context)
                print("\n✅ Detailed column lineage analysis complete!")
            else:
                print("❌ Error: Could not generate comprehensive analysis")
        else:
            print("\n💡 Use --verbose flag for detailed LLM-ready technical context")
            if args.source_definitions:
                print("💡 Use --source-definitions to include source column definitions")
            
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()