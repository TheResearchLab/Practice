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
            print(f"📊 Found {len(dependencies)} dependencies")
            
            # Deduplicate dependencies to avoid repeated tracing
            unique_deps = {}
            for dep in dependencies:
                dep_key = f"{dep['table']}.{dep['column']}"
                if dep_key not in unique_deps:
                    unique_deps[dep_key] = dep
            
            # Now recursively trace each unique dependency
            upstream_lineage = []
            
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
                else:
                    # Extract table name and recursively trace
                    dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                    
                    # Skip if we've already visited this table.column combination
                    if dep_key not in visited:
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
        
        step_context = {
            "step": step_number,
            "step_type": "TRANSFORMATION",
            "table": result['table'],
            "column": result['column'],
            "sql_file": result.get('sql_file', 'Unknown')
        }
        
        # Extract detailed analysis from current file
        file_analysis = result.get("current_file_analysis", {})
        
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
                upstream_contexts = walk_lineage_for_context(
                    upstream["upstream_trace"], 
                    step_num + 1, 
                    current_path
                )
                contexts.extend(upstream_contexts)
        
        return contexts
    
    # Build complete context
    all_step_contexts = walk_lineage_for_context(lineage_result)
    
    # Create comprehensive summary
    source_tables = [ctx for ctx in all_step_contexts if ctx["step_type"] == "SOURCE"]
    transformation_steps = [ctx for ctx in all_step_contexts if ctx["step_type"] == "TRANSFORMATION"]
    
    high_complexity_steps = [ctx for ctx in transformation_steps if "HIGH" in ctx.get("complexity_level", "")]
    aggregation_steps = [ctx for ctx in transformation_steps if ctx.get("transformation_type") == "aggregated"]
    
    summary = {
        "target_column": f"{presentation_table}.{target_column}",
        "total_transformation_steps": len(transformation_steps),
        "source_tables_count": len(source_tables),
        "high_complexity_steps": len(high_complexity_steps),
        "aggregation_points": len(aggregation_steps),
        "ultimate_sources": [f"{src['table']}.{src['column']}" for src in source_tables],
        "complexity_assessment": "HIGH" if high_complexity_steps else ("MEDIUM" if len(transformation_steps) > 3 else "LOW"),
        "development_risk_level": "HIGH" if len(high_complexity_steps) > 1 else ("MEDIUM" if aggregation_steps else "LOW")
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
    Build an enhanced visual DAG with transformation types
    """
    if "error" in technical_context:
        return "Error building enhanced DAG"
    
    steps = technical_context["detailed_steps"]
    
    # Build hierarchical structure
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
            else:
                transformation_type = step.get("transformation_type", "unknown")
                complexity = step.get("complexity_level", "").split(" - ")[0] if " - " in step.get("complexity_level", "") else ""
                
                emoji = "🔧" if complexity == "HIGH" else ("⚙️" if complexity == "MEDIUM" else "📝")
                dag_lines.append(f"{emoji} {step['table']}.{step['column']} ({transformation_type})")
                
                # Add expression if available
                if step.get("sql_expression"):
                    expr = step["sql_expression"][:50] + "..." if len(step["sql_expression"]) > 50 else step["sql_expression"]
                    dag_lines.append(f"    └─ {expr}")
        else:
            # Multiple columns at same level
            dag_lines.append(f"📦 MULTIPLE SOURCES:")
            for step in step_list:
                if step["step_type"] == "SOURCE":
                    dag_lines.append(f"    📍 {step['table']}.{step['column']}")
                else:
                    dag_lines.append(f"    🔧 {step['table']}.{step['column']}")
        
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
        else:
            llm_context.append(f"\nSTEP {step['step']}: TRANSFORMATION")
            llm_context.append(f"  Table: {step['table']}")
            llm_context.append(f"  Column: {step['column']}")
            llm_context.append(f"  SQL File: {step['sql_file']}")
            
            if "sql_expression" in step:
                llm_context.append(f"  SQL Expression: {step['sql_expression']}")
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
    
    # Use Case Specific Sections
    llm_context.append("\n" + "=" * 80)
    llm_context.append("USE CASE SPECIFIC GUIDANCE")
    llm_context.append("=" * 80)
    
    # Documentation Generation Context
    llm_context.append("\nFOR DOCUMENTATION GENERATION:")
    llm_context.append("- Source column comments should be retrieved for: " + ", ".join(summary['ultimate_sources']))
    llm_context.append(f"- Data grain changes occur at {summary['aggregation_points']} aggregation points")
    llm_context.append(f"- Business logic complexity: {summary['complexity_assessment']}")
    
    # Issue Investigation Context  
    llm_context.append("\nFOR ISSUE INVESTIGATION:")
    high_risk_steps = [s for s in steps if "HIGH" in s.get("complexity_level", "")]
    if high_risk_steps:
        llm_context.append("- High-risk transformation points:")
        for step in high_risk_steps:
            llm_context.append(f"  • {step['table']}.{step['column']} - {step.get('transformation_details', 'Complex transformation')}")
    
    # Development Planning Context
    llm_context.append("\nFOR DEVELOPMENT PLANNING:")
    llm_context.append(f"- Change impact scope: {summary['total_transformation_steps']} models affected")
    llm_context.append(f"- Testing complexity: {summary['development_risk_level']} due to transformation complexity")
    if summary['aggregation_points'] > 0:
        llm_context.append(f"- Performance considerations: {summary['aggregation_points']} aggregation points require careful optimization")
    
    return "\n".join(llm_context)
    """
    Format the technical context in an LLM-friendly way for different use cases
    """
    if "error" in technical_context:
        return technical_context
    
    summary = technical_context["summary"]
    steps = technical_context["detailed_steps"]
    
    # Build comprehensive LLM context
    llm_context = []
    
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
        else:
            llm_context.append(f"\nSTEP {step['step']}: TRANSFORMATION")
            llm_context.append(f"  Table: {step['table']}")
            llm_context.append(f"  Column: {step['column']}")
            llm_context.append(f"  SQL File: {step['sql_file']}")
            
            if "sql_expression" in step:
                llm_context.append(f"  SQL Expression: {step['sql_expression']}")
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
    
    # Use Case Specific Sections
    llm_context.append("\n" + "=" * 80)
    llm_context.append("USE CASE SPECIFIC GUIDANCE")
    llm_context.append("=" * 80)
    
    # Documentation Generation Context
    llm_context.append("\nFOR DOCUMENTATION GENERATION:")
    llm_context.append("- Source column comments should be retrieved for: " + ", ".join(summary['ultimate_sources']))
    llm_context.append(f"- Data grain changes occur at {summary['aggregation_points']} aggregation points")
    llm_context.append(f"- Business logic complexity: {summary['complexity_assessment']}")
    
    # Issue Investigation Context  
    llm_context.append("\nFOR ISSUE INVESTIGATION:")
    high_risk_steps = [s for s in steps if "HIGH" in s.get("complexity_level", "")]
    if high_risk_steps:
        llm_context.append("- High-risk transformation points:")
        for step in high_risk_steps:
            llm_context.append(f"  • {step['table']}.{step['column']} - {step.get('transformation_details', 'Complex transformation')}")
    
    # Development Planning Context
    llm_context.append("\nFOR DEVELOPMENT PLANNING:")
    llm_context.append(f"- Change impact scope: {summary['total_transformation_steps']} models affected")
    llm_context.append(f"- Testing complexity: {summary['development_risk_level']} due to transformation complexity")
    if summary['aggregation_points'] > 0:
        llm_context.append(f"- Performance considerations: {summary['aggregation_points']} aggregation points require careful optimization")
    
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
        
        # Also save to file for easy LLM input
        output_file = f"lineage_context_{presentation_table}_{target_column}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(llm_ready_context)
        
        print(f"\n💾 Context saved to: {output_file}")
        print("📋 This context can be used for:")
        print("   • Documentation generation (with source column comments)")
        print("   • Issue investigation and root cause analysis")
        print("   • Development planning and impact assessment")
        print("   • Architecture review and optimization planning")
        
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
    # Test configuration
    compiled_sql_directory = "compiled"
    presentation_table = "fct_customer_orders"
    target_column = "primary_product_category"
    internal_db_prefixes = ['ph_']
    
    print("🔧 RUNNING NEW COMPREHENSIVE ANALYSIS")
    print("="*60)
    
    # Quick summary for development planning
    print("🚀 QUICK DEVELOPMENT PLANNING SUMMARY:")
    quick_result = quick_lineage_summary(compiled_sql_directory, presentation_table, target_column, internal_db_prefixes)
    
    print("\n" + "="*100)
    
    # Comprehensive analysis for LLM context
    print("🤖 COMPREHENSIVE TECHNICAL ANALYSIS:")
    test_comprehensive_technical_analysis(compiled_sql_directory, presentation_table, target_column, internal_db_prefixes)