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
            
            # Process each dependency with proper error handling
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
    error_steps = [ctx for ctx in all_step_contexts if ctx.get("step_type") == "ERROR"]
    
    high_complexity_steps = [ctx for ctx in transformation_steps if "HIGH" in ctx.get("complexity_level", "")]
    aggregation_steps = [ctx for ctx in transformation_steps if ctx.get("transformation_type") == "aggregated"]
    
    # Get all unique ultimate sources
    all_ultimate_sources = set()
    for ctx in source_tables:
        all_ultimate_sources.add(f"{ctx.get('table', 'unknown')}.{ctx.get('column', 'unknown')}")
    
    summary = {
        "target_column": f"{presentation_table}.{target_column}",
        "total_transformation_steps": len(transformation_steps),
        "source_tables_count": len(all_ultimate_sources),
        "high_complexity_steps": len(high_complexity_steps),
        "aggregation_points": len(aggregation_steps),
        "error_count": len(error_steps),
        "ultimate_sources": list(all_ultimate_sources),
        "complexity_assessment": "HIGH" if high_complexity_steps else ("MEDIUM" if len(transformation_steps) > 3 else "LOW"),
        "development_risk_level": "HIGH" if len(high_complexity_steps) > 1 or error_steps else ("MEDIUM" if aggregation_steps else "LOW")
    }
    
    return {
        "summary": summary,
        "detailed_steps": all_step_contexts,
        "source_tables": source_tables,
        "transformation_chain": transformation_steps,
        "error_steps": error_steps,
        "lineage_structure": lineage_result
    }


def build_enhanced_visual_dag(technical_context, include_source_definitions=False, tracer=None):
    """
    Build an enhanced visual DAG with proper flow visualization
    """
    if "error" in technical_context:
        return "Error building enhanced DAG"
    
    steps = technical_context["detailed_steps"]
    
    dag_lines = []
    dag_lines.append("🎯 ENHANCED COLUMN LINEAGE FLOW:")
    dag_lines.append("="*60)
    
    # Group steps by type for better visualization
    source_steps = [s for s in steps if s.get("step_type") == "SOURCE"]
    transformation_steps = [s for s in steps if s.get("step_type") == "TRANSFORMATION"]
    error_steps = [s for s in steps if s.get("step_type") == "ERROR"]
    
    # Show ultimate sources first
    if source_steps:
        dag_lines.append("📦 ULTIMATE DATA SOURCES:")
        for source in source_steps:
            table = source.get('table', 'unknown')
            column = source.get('column', 'unknown')
            reason = source.get('source_reason', 'unknown')
            dag_lines.append(f"    📍 {table}.{column} ({reason})")
            
            # Add source definitions if requested and available
            if include_source_definitions and tracer:
                source_def = tracer.get_source_definition(table, column)
                if source_def:
                    dag_lines.append(f"        └─ Type: {source_def.get('data_type', 'Unknown')}")
                    dag_lines.append(f"        └─ Description: {source_def.get('description', 'No description available')}")
        
        dag_lines.append("      ↓")
        dag_lines.append("   [FLOWS THROUGH TRANSFORMATION LAYERS]")
        dag_lines.append("      ↓")
    
    # Group transformation steps by table to show file-level processing
    if transformation_steps:
        # Sort by step number (reverse for bottom-up flow)
        sorted_transformations = sorted(transformation_steps, key=lambda x: x.get("step", 0), reverse=True)
        
        # Group by table while maintaining order
        table_groups = {}
        table_order = []
        for step in sorted_transformations:
            table = step.get('table', 'unknown')
            if table not in table_groups:
                table_groups[table] = []
                table_order.append(table)
            table_groups[table].append(step)
        
        # Show each table's transformations
        for i, table_name in enumerate(table_order):
            table_steps = table_groups[table_name]
            
            if len(table_steps) == 1:
                step = table_steps[0]
                complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                
                # Get SQL file info
                sql_file = step.get("sql_file", "")
                file_name = sql_file.split("\\")[-1] if "\\" in sql_file else sql_file.split("/")[-1] if "/" in sql_file else f"{table_name}.sql"
                
                dag_lines.append(f"{emoji} TABLE: {table_name} ({file_name}):")
                dag_lines.append(f"    └─ {step.get('column', 'unknown')} ({step.get('transformation_type', 'unknown')})")
                
                # Show key transformation details
                if step.get("sql_expression"):
                    expr = step["sql_expression"]
                    if len(expr) > 80:
                        if "CASE" in expr.upper():
                            dag_lines.append(f"    └─ Complex CASE logic")
                        elif "||" in expr:
                            dag_lines.append(f"    └─ String concatenation")
                        else:
                            dag_lines.append(f"    └─ {expr[:60]}...")
                    else:
                        dag_lines.append(f"    └─ {expr}")
                
                # Show CTE information if present
                if step.get("has_cte_logic"):
                    dag_lines.append(f"    └─ Contains CTE: {step.get('cte_summary', 'CTE transformations')}")
            
            else:
                # Multiple transformations in same table
                dag_lines.append(f"📦 MULTIPLE TRANSFORMATIONS IN {table_name}:")
                for step in table_steps:
                    complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                    emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                    dag_lines.append(f"    {emoji} {step.get('column', 'unknown')} ({step.get('transformation_type', 'unknown')})")
            
            # Add flow arrow unless this is the last table
            if i < len(table_order) - 1:
                dag_lines.append("      ↓")
                dag_lines.append("   [FLOWS TO]")
                dag_lines.append("      ↓")
    
    # Show errors if any
    if error_steps:
        dag_lines.append("      ↓")
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


def quick_lineage_summary(compiled_sql_directory: str, presentation_table: str, target_column: str, internal_db_prefixes: List[str] = None, source_definitions_file: Optional[str] = None):
    """
    Quick summary for development planning
    """
    try:
        tracer = DBTLineageTracer(compiled_sql_directory, internal_db_prefixes, source_definitions_file)
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
        # Always show quick summary
        print("\n" + "="*80)
        print("🚀 QUICK LINEAGE SUMMARY:")
        quick_result, tracer_instance = quick_lineage_summary(args.compiled_dir, args.table, args.column, args.internal_prefixes, args.source_definitions)
        
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