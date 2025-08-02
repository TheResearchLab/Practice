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
                print(f"   ✅ Successfully imported column_lineage functions")
            except ImportError:
                print(f"   ⚠️  Could not import from column_lineage, trying paste module")
                from paste import trace_column_lineage
            
            # Trace the column within this single file
            single_file_trace = trace_column_lineage(sql_content, target_column)
            
            if "error" in single_file_trace:
                return single_file_trace
                
            dependencies = single_file_trace.get('next_columns_to_search', [])
            cte_transformations = single_file_trace.get('cte_transformations', [])
            
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
                    
                    # Trace each dependency to see if they come from the same CTE
                    cte_info_by_dep = {}
                    for dep in table_deps:
                        dep_table = dep['table']
                        dep_column = dep['column']
                        dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                        
                        # Trace this dependency to get its CTE information
                        dep_trace = self.trace_column_lineage_across_files(dep_table_name, dep_column, visited.copy())
                        
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
                    
                    # Process consolidated CTEs
                    for cte_name, cte_group in cte_groups.items():
                        if len(cte_group) > 1:
                            # Found multiple dependencies from same CTE - consolidate!
                            columns = [item["cte_transform"]["column"] for item in cte_group]
                            print(f"   🔄 Consolidated CTE '{cte_name}' processes {len(columns)} columns: {', '.join(columns)}")
                            print(f"   ⚡ Processing consolidated CTE instead of {len(columns)} separate dependencies")
                            
                            # Get external dependencies from the CTE (should be same for all)
                            sample_trace = cte_group[0]["dep_info"]["trace"]
                            cte_external_deps = sample_trace.get("upstream_lineage", [])
                            
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
                            for item in cte_group:
                                dep_trace = item["dep_info"]["trace"]
                                if dep_trace.get("upstream_lineage"):
                                    # Add each upstream lineage from this dependency
                                    consolidated_entry["upstream_trace"]["upstream_lineage"].extend(dep_trace["upstream_lineage"])
                            
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
                                visited.copy()
                            )
                            
                            upstream_lineage.append({
                                "dependency": dep,
                                "upstream_trace": upstream_trace
                            })
                        else:
                            print(f"   🔄 Already visited: {dep_table}.{dep_column}")
            
            # Show summary of what we found
            if cte_transformations:
                print(f"🔄 Found {len(cte_transformations)} intra-file CTE transformations")
                for cte_info in cte_transformations:
                    print(f"   └─ CTE '{cte_info['cte_name']}' transforms {cte_info['column']}")
            
            return {
                "table": presentation_table,
                "column": target_column,
                "type": "intermediate",
                "current_file_analysis": single_file_trace,
                "cte_transformations": cte_transformations,
                "upstream_lineage": upstream_lineage,
                "sql_file": str(self.table_to_file_map.get(presentation_table, "Unknown"))
            }
            
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
        cte_transformations = result.get("cte_transformations", [])
        
        # Add CTE transformation information
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
    
    def walk_lineage_for_context(result, step_num=1, path=[]):
        """Recursively walk lineage and build context for each step"""
        current_path = path + [f"{result['table']}.{result['column']}"]
        
        step_context = analyze_transformation_step(result, step_num, len(current_path))
        step_context["lineage_path"] = " → ".join(current_path)
        
        contexts = [step_context]
        
        # Process upstream dependencies
        if result.get("upstream_lineage"):
            for upstream in result["upstream_lineage"]:
                # Check if this is a consolidated CTE entry
                if upstream.get("dependency_type") == "CTE_CONSOLIDATED":
                    # This is a consolidated CTE - extract all its sources
                    cte_name = upstream.get("cte_name")
                    columns = upstream.get("columns", [])
                    cte_upstream_lineage = upstream.get("upstream_trace", {}).get("upstream_lineage", [])
                    
                    # Create consolidated CTE context entry
                    consolidated_step = {
                        "step": step_num + 1,
                        "step_type": "CTE_CONSOLIDATED",
                        "table": upstream.get("table"),
                        "cte_name": cte_name,
                        "columns": columns,
                        "transformation_details": f"CTE '{cte_name}' aggregates {len(columns)} columns: {', '.join(columns)}",
                        "sql_file": upstream.get("upstream_trace", {}).get("sql_file", "Unknown"),
                        "complexity_level": "HIGH - Multiple column aggregation in CTE",
                        "performance_impact": "High due to aggregation operations",
                        "data_scope": f"Aggregation step - processes {len(columns)} columns",
                        "control_flow": "GROUP BY aggregation within CTE",
                        "business_impact": "Critical aggregation logic for business metrics",
                        "data_scope_change": f"Aggregates {len(columns)} columns in single CTE",
                        "upstream_dependencies": cte_upstream_lineage
                    }
                    contexts.append(consolidated_step)
                    
                    # Process all upstream dependencies of the consolidated CTE
                    for cte_upstream in cte_upstream_lineage:
                        if "upstream_trace" in cte_upstream:
                            upstream_contexts = walk_lineage_for_context(
                                cte_upstream["upstream_trace"], 
                                step_num + 2, 
                                current_path + [f"CTE:{cte_name}"]
                            )
                            contexts.extend(upstream_contexts)
                
                elif "upstream_trace" in upstream:
                    # Regular upstream dependency
                    upstream_contexts = walk_lineage_for_context(
                        upstream["upstream_trace"], 
                        step_num + 1, 
                        current_path
                    )
                    contexts.extend(upstream_contexts)
        
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


def build_enhanced_visual_dag(technical_context):
    """
    Build an enhanced visual DAG with transformation types and explicit CTE steps
    """
    if "error" in technical_context:
        return "Error building enhanced DAG"
    
    steps = technical_context["detailed_steps"]
    
    # Build hierarchical structure with CTE consolidation
    dag_lines = []
    dag_lines.append("🎯 ENHANCED COLUMN LINEAGE FLOW:")
    dag_lines.append("="*60)
    
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
                dag_lines.append(f"📍 SOURCE: {step['table']}.{step['column']} ({step['source_reason']})")
            elif step["step_type"] == "CTE":
                dag_lines.append(f"🔄 CTE: {step['table']}.{step['column']} (intra-file transformation)")
                if step.get("transformation_details"):
                    details = step["transformation_details"][:60] + "..." if len(step["transformation_details"]) > 60 else step["transformation_details"]
                    dag_lines.append(f"    └─ {details}")
            elif step["step_type"] == "CTE_CONSOLIDATED":
                # Special handling for consolidated CTE steps
                cte_name = step["cte_name"]
                columns = step["columns"]
                dag_lines.append(f"🔄 CTE '{cte_name}' AGGREGATION STEP:")
                
                # Show what columns are being produced
                if len(columns) <= 3:
                    dag_lines.append(f"    📊 Produces: {', '.join(columns)}")
                else:
                    dag_lines.append(f"    📊 Produces: {', '.join(columns[:3])} + {len(columns)-3} more")
                
                # Show the upstream sources that feed into this CTE
                upstream_lineage = step.get("upstream_lineage", [])
                if upstream_lineage:
                    source_tables = set()
                    transformation_details = []
                    
                    # Collect all unique sources and their transformations
                    for upstream in upstream_lineage:
                        if upstream.get("upstream_trace", {}).get("type") == "source":
                            trace = upstream["upstream_trace"]
                            source_tables.add(f"{trace['table']}.{trace['column']}")
                        elif "dependency" in upstream:
                            dep = upstream["dependency"]
                            transformation_details.append(f"{dep['table']}.{dep['column']}")
                    
                    if source_tables:
                        dag_lines.append(f"    📊 Ultimate Sources: {', '.join(sorted(source_tables))}")
                    
                    if transformation_details:
                        dag_lines.append(f"    📊 Intermediate transformations:")
                        for detail in transformation_details[:3]:  # Show first 3
                            dag_lines.append(f"        • {detail}")
                        if len(transformation_details) > 3:
                            dag_lines.append(f"        • ... and {len(transformation_details)-3} more")
                        
            else:
                transformation_type = step.get("transformation_type", "unknown")
                complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                
                emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                
                # Check if this step has CTE transformations
                cte_steps = step.get("cte_steps", [])
                if cte_steps:
                    dag_lines.append(f"{emoji} TABLE: {step['table']}.{step['column']} (via CTE transformations)")
                    
                    # Show each CTE step as an intermediate transformation
                    for cte_step in cte_steps:
                        cte_name = cte_step["cte_name"]
                        cte_column = cte_step["column"]
                        dependencies = cte_step.get("dependencies", [])
                        
                        dag_lines.append(f"    🔄 CTE '{cte_name}' TRANSFORMS:")
                        
                        if dependencies:
                            # Show what the CTE depends on
                            dep_sources = []
                            for dep in dependencies:
                                if dep.get("level") == "external_via_cte":
                                    dep_sources.append(f"{dep['table']}.{dep['column']}")
                            
                            if dep_sources:
                                if len(dep_sources) == 1:
                                    dag_lines.append(f"        📊 {dep_sources[0]} → [aggregation] → {cte_column}")
                                else:
                                    dag_lines.append(f"        📊 [{', '.join(dep_sources)}] → [aggregation] → {cte_column}")
                        else:
                            dag_lines.append(f"        📊 [unknown sources] → {cte_column}")
                else:
                    dag_lines.append(f"{emoji} TABLE: {step['table']}.{step['column']} ({transformation_type})")
                
                # Add expression if available
                if step.get("sql_expression"):
                    expr = step["sql_expression"]
                    # For complex expressions, format them properly instead of truncating
                    if len(expr) > 80:
                        # Break long expressions into multiple lines
                        if "CASE" in expr.upper():
                            # Special handling for CASE statements
                            dag_lines.append(f"    └─ {expr[:80]}...")
                            dag_lines.append(f"       [Complex CASE logic - see detailed analysis]")
                        elif any(keyword in expr.upper() for keyword in ["SUM", "COUNT", "AVG", "MODE", "MAX", "MIN"]):
                            # Aggregation functions
                            dag_lines.append(f"    └─ {expr}")
                        else:
                            # Other long expressions - show more characters
                            dag_lines.append(f"    └─ {expr[:120]}...")
                    else:
                        dag_lines.append(f"    └─ {expr}")
        else:
            # Multiple columns at same level - check for CTE consolidation
            cte_steps = [s for s in step_list if s["step_type"] == "CTE_CONSOLIDATED"]
            regular_steps = [s for s in step_list if s["step_type"] != "CTE_CONSOLIDATED"]
            
            # Show CTE consolidated steps
            for cte_step in cte_steps:
                cte_name = cte_step["cte_name"]
                columns = cte_step["columns"]
                dag_lines.append(f"🔄 CTE '{cte_name}' AGGREGATION STEP:")
                dag_lines.append(f"    📊 Produces: {', '.join(columns)}")
                
                # Show ALL upstream sources that feed into this consolidated CTE
                upstream_lineage = cte_step.get("upstream_lineage", [])
                if upstream_lineage:
                    # Collect all ultimate sources
                    all_sources = set()
                    transformation_chain = []
                    
                    def collect_sources_recursive(lineage_item, depth=0):
                        if isinstance(lineage_item, dict):
                            if lineage_item.get("type") == "source":
                                all_sources.add(f"{lineage_item['table']}.{lineage_item['column']}")
                            elif lineage_item.get("upstream_lineage"):
                                for upstream in lineage_item["upstream_lineage"]:
                                    if "upstream_trace" in upstream:
                                        collect_sources_recursive(upstream["upstream_trace"], depth + 1)
                            elif "dependency" in lineage_item and "upstream_trace" in lineage_item:
                                collect_sources_recursive(lineage_item["upstream_trace"], depth + 1)
                    
                    for upstream in upstream_lineage:
                        if "upstream_trace" in upstream:
                            collect_sources_recursive(upstream["upstream_trace"])
                        elif "dependency" in upstream:
                            dep = upstream["dependency"]
                            transformation_chain.append(f"{dep['table']}.{dep['column']}")
                    
                    if all_sources:
                        if len(all_sources) == 1:
                            dag_lines.append(f"    📊 Ultimate Source: {list(all_sources)[0]}")
                        else:
                            dag_lines.append(f"    📊 Ultimate Sources: {', '.join(sorted(all_sources))}")
                    
                    if transformation_chain:
                        dag_lines.append(f"    📊 Transformation Chain:")
                        for transform in transformation_chain[:5]:  # Show first 5
                            dag_lines.append(f"        • {transform}")
                        if len(transformation_chain) > 5:
                            dag_lines.append(f"        • ... and {len(transformation_chain)-5} more")
            
            # Show regular multiple sources
            if regular_steps:
                if len(regular_steps) > 1:
                    dag_lines.append(f"📦 MULTIPLE SOURCES:")
                    for step in regular_steps:
                        if step["step_type"] == "SOURCE":
                            dag_lines.append(f"    📍 {step['table']}.{step['column']}")
                        elif step["step_type"] == "CTE":
                            dag_lines.append(f"    🔄 CTE: {step['table']}.{step['column']}")
                        else:
                            dag_lines.append(f"    🔧 TABLE: {step['table']}.{step['column']}")
                else:
                    # Single regular step
                    step = regular_steps[0]
                    if step["step_type"] == "SOURCE":
                        dag_lines.append(f"📍 SOURCE: {step['table']}.{step['column']} ({step['source_reason']})")
                    else:
                        transformation_type = step.get("transformation_type", "unknown")
                        complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                        emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                        dag_lines.append(f"{emoji} TABLE: {step['table']}.{step['column']} ({transformation_type})")
        
        # Add flow arrow unless this is the last level
        if i < len(sorted_steps) - 1:
            dag_lines.append("      ↓")
            dag_lines.append("   [FLOWS TO]")
            dag_lines.append("      ↓")
    
    return "\n".join(dag_lines)


def format_context_for_llm(technical_context):
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
    visual_dag = build_enhanced_visual_dag(technical_context)
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
    
    # Ultimate Sources
    llm_context.append("ULTIMATE DATA SOURCES:")
    for source in summary['ultimate_sources']:
        llm_context.append(f"  • {source}")
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
            
            # Show upstream sources for this CTE
            if step.get("upstream_lineage"):
                llm_context.append("  CTE Input Sources:")
                for upstream in step["upstream_lineage"]:
                    dep = upstream["dependency"]
                    llm_context.append(f"    - {dep['table']}.{dep['column']} → {dep['context']}")
            
            llm_context.append("  Issue Investigation Points:")
            llm_context.append("    - Aggregation logic - verify grouping and calculation")
            llm_context.append("    - Data completeness - check for missing records in aggregation")
            
            llm_context.append("  Development Planning Notes:")
            llm_context.append("    - Schema changes require aggregation logic updates")
            llm_context.append("    - Consider materialization for performance")
            llm_context.append("    - Multiple column dependencies - coordinate changes carefully")
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
            
            if step.get("issue_investigation_points"):
                llm_context.append("  Issue Investigation Points:")
                for point in step["issue_investigation_points"]:
                    llm_context.append(f"    - {point}")
            
            if step.get("development_planning_notes"):
                llm_context.append("  Development Planning Notes:")
                for note in step["development_planning_notes"]:
                    llm_context.append(f"    - {note}")
    
    # Use Case Specific Sections - REMOVED per user request
    # The generic use case guidance was not needed for production use
    
    return "\n".join(llm_context)


def test_comprehensive_technical_analysis(compiled_sql_directory: str, presentation_table: str, target_column: str, internal_db_prefixes: List[str] = None):
    """
    Test comprehensive technical analysis for LLM context generation
    """
    print("="*80)
    print("COMPREHENSIVE TECHNICAL ANALYSIS FOR LLM CONTEXT")
    print("="*80)
    
    try:
        tracer = DBTLineageTracer(compiled_sql_directory, internal_db_prefixes)
        
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
        llm_ready_context = format_context_for_llm(technical_context)
        
        print("\n🤖 LLM-READY TECHNICAL CONTEXT:")
        print("="*80)
        print(llm_ready_context)
        
        print("\n✅ Column lineage analysis complete!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()


def quick_lineage_summary(compiled_sql_directory: str, presentation_table: str, target_column: str, internal_db_prefixes: List[str] = None):
    """
    Quick summary for development planning
    """
    try:
        tracer = DBTLineageTracer(compiled_sql_directory, internal_db_prefixes)
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
        
        return technical_context
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


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


# Old function removed - using new comprehensive analysis instead


# Example usage
if __name__ == "__main__":
    # Test configuration for multiple source columns
    compiled_sql_directory = "compiled"
    presentation_table = "fct_customer_orders"
    target_column = "customer_segment"  # This should have multiple source dependencies
    internal_db_prefixes = ['ph_']
    
    print("🔧 RUNNING NEW COMPREHENSIVE ANALYSIS")
    print("="*60)
    
    # Quick summary for development planning
    # print("🚀 QUICK DEVELOPMENT PLANNING SUMMARY:")
    # quick_result = quick_lineage_summary(compiled_sql_directory, presentation_table, target_column, internal_db_prefixes)
    
    print("\n" + "="*100)
    
    # Comprehensive analysis for LLM context
    print("🤖 COMPREHENSIVE TECHNICAL ANALYSIS:")
    test_comprehensive_technical_analysis(compiled_sql_directory, presentation_table, target_column, internal_db_prefixes)