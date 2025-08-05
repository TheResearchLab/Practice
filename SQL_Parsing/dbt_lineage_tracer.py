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
        FIXED: Generic solution that properly bridges manifest -> SQL references
        """
        if not self.manifest_data:
            return []
        
        nodes = self.manifest_data.get('nodes', {})
        
        # Find the snapshot node - use GENERIC matching approach
        snapshot_node = None
        matching_relation_name = None
        
        for key, node in nodes.items():
            if not key.startswith('snapshot.'):
                continue
                
            # Get the actual relation_name from manifest (this is the SQL reference)
            relation_name = node.get('relation_name', '')
            
            # Clean relation_name (remove quotes, normalize)
            clean_relation = relation_name.replace('"', '').lower()
            clean_table_name = table_name.lower()
            
            # Check if this snapshot matches our table reference
            # Support multiple matching strategies:
            # 1. Full match: database.schema.table == relation_name
            # 2. Suffix match: table matches the last part of relation_name
            # 3. Name match: snapshot name/alias matches
            
            if (clean_relation == clean_table_name or 
                clean_relation.endswith(f".{clean_table_name}") or
                node.get('name', '').lower() == clean_table_name or
                node.get('alias', '').lower() == clean_table_name):
                
                snapshot_node = node
                matching_relation_name = relation_name
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
            if not source_node:
                continue
                
            # CRITICAL FIX: Use the relation_name from manifest, not constructed name
            # This is the ACTUAL table reference that will appear in SQL
            dependency_relation_name = source_node.get('relation_name', '')
            
            if dependency_relation_name:
                # Clean the relation name for SQL matching
                clean_dependency_name = dependency_relation_name.replace('"', '')
                
                dependencies.append({
                    "table": clean_dependency_name,  # This will match SQL references
                    "column": "*",
                    "context": "snapshot_source", 
                    "level": "external_table",
                    "resource_type": source_node.get('resource_type', 'unknown'),
                    "node_key": source_node_key,
                    "original_relation_name": dependency_relation_name  # Keep original for debugging
                })
            else:
                # Fallback: construct name if relation_name missing (shouldn't happen in good manifests)
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
                    "context": "snapshot_source_fallback",
                    "level": "external_table",
                    "resource_type": source_node.get('resource_type', 'unknown'),
                    "node_key": source_node_key,
                    "constructed": True  # Flag that this was constructed, not from relation_name
                })
        
        return dependencies
    
    def _resolve_table_to_relation_name(self, table_reference: str) -> Optional[str]:
        """
        NEW METHOD: Convert any table reference to its actual relation_name from manifest
        This bridges the gap between SQL references and manifest metadata
        
        Args:
            table_reference: Any table reference found in SQL (e.g., "database.schema.table")
            
        Returns:
            The actual relation_name from manifest, or None if not found
        """
        if not self.manifest_data:
            return None
            
        nodes = self.manifest_data.get('nodes', {})
        clean_table_ref = table_reference.replace('"', '').lower()
        
        # Search through all nodes to find matching relation_name
        for key, node in nodes.items():
            relation_name = node.get('relation_name', '')
            if not relation_name:
                continue
                
            clean_relation = relation_name.replace('"', '').lower()
            
            # Multiple matching strategies
            if (clean_relation == clean_table_ref or
                clean_relation.endswith(f".{clean_table_ref}") or
                clean_table_ref.endswith(f".{clean_relation}") or
                clean_relation.split('.')[-1] == clean_table_ref.split('.')[-1]):
                
                return relation_name.replace('"', '')  # Return clean relation name
        
        return None
    
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
            
        # Check if file exists in our project - TRY STAGING PATTERN TOO
        table_name = self.extract_table_name_from_full_ref(full_table_name)
        
        # Try exact match first
        if table_name in self.table_to_file_map:
            return False, "internal_table"
        
        # Try staging pattern for source tables
        stg_pattern = f"stg_{table_name}"
        if stg_pattern in self.table_to_file_map:
            return False, f"internal_via_staging"
            
        return True, "missing_file"
    
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
        FIXED: Properly bridges between SQL references, manifest data, and back to SQL
        FIXED: Now properly tracks resolved table names for accurate DAG visualization
        FIXED: Implements staging boundary logic - stops tracing at staging models
        """
        if visited is None:
            visited = set()
            
        visit_key = f"{presentation_table}.{target_column}"
        if visit_key in visited:
            return {"error": f"Circular reference detected: {visit_key}"}
            
        visited.add(visit_key)
        
        print(f"\n🔍 Tracing column '{target_column}' in table '{presentation_table}'")
        
        # STEP 1: Check if this is a snapshot using the ACTUAL table reference
        # The presentation_table might be a full SQL reference like "database.schema.table"
        snapshot_dependencies = self._check_snapshot_dependencies(presentation_table)
        
        if snapshot_dependencies:
            print(f"📸 Found snapshot with {len(snapshot_dependencies)} source dependencies")
            upstream_lineage = []
            
            for i, dep in enumerate(snapshot_dependencies, 1):
                dep_table = dep['table']  # This is now the ACTUAL relation_name from manifest
                print(f"   📋 Snapshot dependency {i}: {dep_table}")
                
                # Check if this dependency is a source (external) table
                is_source, reason = self.is_source_table(dep_table)
                
                if is_source:
                    print(f"   ✅ Found external source: {dep_table} ({reason})")
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
                    # This is an internal table - continue tracing
                    # CRITICAL: Extract just the table name for file lookup
                    dep_table_name = self.extract_table_name_from_full_ref(dep_table)
                    dep_visit_key = f"{dep_table}.{target_column}"
                    
                    if dep_visit_key not in visited:
                        print(f"   ⬆️  Tracing upstream: {dep_table} -> {dep_table_name}")
                        try:
                            # FIXED: Pass the table name for file lookup, but preserve full reference in results
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
                        print(f"   🔄 Already visited: {dep_table}")
            
            return {
                "table": presentation_table,
                "column": target_column,
                "type": "snapshot",
                "snapshot_source_count": len(snapshot_dependencies),
                "upstream_lineage": upstream_lineage,
                "sql_file": str(self.table_to_file_map.get(self.extract_table_name_from_full_ref(presentation_table), "Unknown"))
            }
        
        # STEP 2: Not a snapshot - proceed with regular SQL file analysis
        # For SQL file analysis, we need the table name, not the full reference
        sql_table_name = self.extract_table_name_from_full_ref(presentation_table)
        original_table_name = sql_table_name  # Keep track of original name
        resolved_table_name = None  # Track if we resolved it
        
        sql_content = self.load_sql_file(sql_table_name)
        
        # NEW: If direct load failed, try staging pattern for source tables
        if not sql_content:
            stg_pattern = f"stg_{sql_table_name}"
            sql_content = self.load_sql_file(stg_pattern)
            if sql_content:
                print(f"🔄 Resolved {sql_table_name} → {stg_pattern}")
                resolved_table_name = stg_pattern  # Track that we resolved it
                sql_table_name = stg_pattern  # Update the table name for downstream processing
        
        if not sql_content:
            # If still no file found, treat as source
            print(f"✅ Found source table: {presentation_table} (missing_file)")
            return {
                "table": presentation_table,
                "column": target_column,
                "type": "source",
                "reason": "missing_file",
                "lineage_chain": []
            }
        
        try:
            # Import the fixed column lineage function
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
            
            # Show CTE transformations info
            if cte_transformations and show_cte_messages:
                print(f"🔄 Found {len(cte_transformations)} intra-file CTE transformations in {sql_table_name}")
                for cte_info in cte_transformations:
                    cte_column = cte_info.get('column', cte_info.get('columns', 'unknown'))
                    print(f"   └─ CTE '{cte_info['cte_name']}' transforms {cte_column}")
            
            upstream_lineage = []
            print(f"📊 Found {len(dependencies)} external dependencies")
            
            # CRITICAL FIX: Use resolved name to determine current layer
            current_table_name = resolved_table_name if resolved_table_name else sql_table_name
            is_staging_model = current_table_name.startswith('stg_')
            
            if is_staging_model:
                print(f"🏁 STAGING BOUNDARY: {current_table_name} is a staging model - treating all dependencies as sources")
            
            # STEP 3: Process dependencies - handle both snapshot and regular dependencies
            for dep in dependencies:
                try:
                    dep_table = dep.get('table', 'unknown_table')
                    dep_column = dep.get('column', 'unknown_column')
                    
                    print(f"   📋 Processing dependency: {dep_table}.{dep_column}")
                    
                    # NEW STAGING BOUNDARY LOGIC: If we're in a staging model, treat all dependencies as sources
                    if is_staging_model:
                        print(f"   🏁 Staging boundary: treating {dep_table}.{dep_column} as ultimate source")
                        upstream_lineage.append({
                            "dependency": dep,
                            "upstream_trace": {
                                "table": dep_table,
                                "column": dep_column,
                                "type": "source",
                                "reason": "staging_boundary",
                                "lineage_chain": []
                            }
                        })
                        continue  # Skip all other resolution logic for staging models
                    
                    # EXISTING LOGIC: Only applies to non-staging models
                    # CRITICAL FIX: For each dependency, check if it maps to a snapshot or model in manifest
                    # This handles cases where SQL references a table that's actually managed by dbt
                    
                    # First, try to resolve this table reference through the manifest
                    resolved_relation_name = self._resolve_table_to_relation_name(dep_table)
                    
                    if resolved_relation_name:
                        print(f"   🔍 Resolved via manifest: {dep_table} -> {resolved_relation_name}")
                        # Check if the resolved name is a snapshot
                        resolved_snapshot_deps = self._check_snapshot_dependencies(resolved_relation_name)
                        
                        if resolved_snapshot_deps:
                            print(f"   📸 Resolved dependency is a snapshot!")
                            # Recursively trace through the snapshot
                            resolved_table_name_dep = self.extract_table_name_from_full_ref(resolved_relation_name)
                            dep_visit_key = f"{resolved_relation_name}.{dep_column}"
                            
                            if dep_visit_key not in visited:
                                upstream_trace = self.trace_column_lineage_across_files(
                                    resolved_table_name_dep,
                                    dep_column,
                                    visited.copy(),
                                    show_cte_messages=False
                                )
                                upstream_lineage.append({
                                    "dependency": dep,
                                    "upstream_trace": upstream_trace,
                                    "resolved_via_manifest": True,
                                    "original_reference": dep_table,
                                    "resolved_reference": resolved_relation_name
                                })
                            else:
                                print(f"   🔄 Already visited resolved: {resolved_relation_name}")
                            continue
                    
                    # If not resolved via manifest, proceed with original logic
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
                        print(f"   🔄 CTE reference: {dep_table}.{dep_column}")
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
                        # Internal table - continue tracing
                        dep_table_name = self.extract_table_name_from_full_ref(dep_table)
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
                    continue
            
            # CRITICAL FIX: Use the resolved table name for accurate layer classification
            # This ensures the DAG shows stg_source_table_3 instead of source_table_3
            result_table_name = resolved_table_name if resolved_table_name else presentation_table
            
            return {
                "table": result_table_name,  # Use resolved name if available
                "column": target_column,
                "type": "intermediate",
                "current_file_analysis": single_file_trace,
                "cte_transformations": cte_transformations,
                "upstream_lineage": upstream_lineage,
                "sql_file": str(self.table_to_file_map.get(sql_table_name, "Unknown")),
                "original_table_name": original_table_name,  # Keep track of original for debugging
                "was_resolved": bool(resolved_table_name),  # Flag indicating if resolution occurred
                "is_staging_boundary": is_staging_model  # Flag indicating this is a staging boundary
            }
            
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
    Build architectural DAG showing actual data flow layers with CORRECT ORDER and PARALLEL FLOWS
    FIXED: Uses dependency relationships to determine proper flow, shows parallel tables within layers
    """
    if "error" in technical_context:
        return "Error building enhanced DAG"
    
    steps = technical_context["detailed_steps"]
    lineage_result = technical_context["lineage_structure"]
    
    dag_lines = []
    dag_lines.append("🎯 ENHANCED COLUMN LINEAGE FLOW:")
    dag_lines.append("="*60)
    
    # Build a complete table registry from all steps
    all_tables = {}
    for step in steps:
        table = step.get('table', 'unknown')
        step_type = step.get('step_type', 'unknown')
        
        if table not in all_tables:
            all_tables[table] = {
                'step_info': step,
                'layer': None,
                'dependencies': set()
            }
        
        # Classify by type and naming convention
        if step_type == "SOURCE":
            all_tables[table]['layer'] = 'source'
        elif step_type == "SNAPSHOT":
            all_tables[table]['layer'] = 'snapshot'
        elif table.startswith('stg_'):
            all_tables[table]['layer'] = 'staging'
        elif table.startswith('wrk_'):
            all_tables[table]['layer'] = 'work'
        elif table.startswith('dim_') or table.startswith('fct_'):
            all_tables[table]['layer'] = 'mart'
        else:
            all_tables[table]['layer'] = 'other'
    
    # Extract dependencies from lineage structure
    def extract_all_dependencies(result):
        """Recursively extract all table->table dependencies"""
        deps = {}
        
        def walk_dependencies(node, current_table=None):
            if not isinstance(node, dict):
                return
                
            table = node.get('table', '').split('.')[-1]  # Get table name only
            if table and table != 'unknown':
                if table not in deps:
                    deps[table] = set()
                
                upstream_lineage = node.get('upstream_lineage', [])
                for upstream in upstream_lineage:
                    if isinstance(upstream, dict):
                        upstream_trace = upstream.get('upstream_trace', {})
                        if upstream_trace and isinstance(upstream_trace, dict):
                            upstream_table = upstream_trace.get('table', '').split('.')[-1]
                            if upstream_table and upstream_table != 'unknown':
                                # Only add non-source dependencies for ordering
                                if upstream_trace.get('type') not in ['source', 'error']:
                                    deps[table].add(upstream_table)
                            
                            # Recursively process upstream
                            walk_dependencies(upstream_trace)
        
        walk_dependencies(result)
        return deps
    
    table_dependencies = extract_all_dependencies(lineage_result)
    
    # Update all_tables with extracted dependencies
    for table, deps in table_dependencies.items():
        if table in all_tables:
            all_tables[table]['dependencies'] = deps
    
    # Group tables by layer
    layers = {
        'source': [],
        'staging': [],
        'work': [],
        'other': [],
        'snapshot': [],
        'mart': []
    }
    
    for table, info in all_tables.items():
        layer = info['layer']
        if layer in layers:
            layers[layer].append(table)
    
    # Layer display configuration
    layer_order = ['source', 'staging', 'work', 'other', 'snapshot', 'mart']
    layer_config = {
        'source': {'name': '📦 ULTIMATE DATA SOURCES', 'emoji': '📍'},
        'staging': {'name': '📝 STAGING LAYER', 'emoji': '📝'},
        'work': {'name': '⚙️ WORK LAYER', 'emoji': '⚙️'},
        'other': {'name': '🔧 TRANSFORMATION LAYER', 'emoji': '🔧'},
        'snapshot': {'name': '📸 SNAPSHOT LAYER', 'emoji': '📸'},
        'mart': {'name': '🎯 MART LAYER', 'emoji': '🎯'}
    }
    
    # Display each layer
    for layer_key in layer_order:
        layer_tables = layers[layer_key]
        if not layer_tables:
            continue
            
        layer_config_item = layer_config[layer_key]
        
        if layer_key == 'source':
            # Special handling for sources
            dag_lines.append(f"{layer_config_item['name']}:")
            for table in layer_tables:
                step_info = all_tables[table]['step_info']
                column = step_info.get('column', 'unknown')
                reason = step_info.get('source_reason', 'unknown')
                
                if include_source_definitions and tracer:
                    source_def = tracer.get_source_definition(table, column)
                    if source_def:
                        dag_lines.append(f"    {layer_config_item['emoji']} {table}.{column} ({reason})")
                        dag_lines.append(f"        └─ Type: {source_def.get('data_type', 'Unknown')}")
                        dag_lines.append(f"        └─ Description: {source_def.get('description', 'No description available')}")
                    else:
                        dag_lines.append(f"    {layer_config_item['emoji']} {table}.{column} ({reason})")
                else:
                    dag_lines.append(f"    {layer_config_item['emoji']} {table}.{column} ({reason})")
        
        elif layer_key == 'snapshot':
            # Special handling for snapshots
            dag_lines.append(f"{layer_config_item['name']}:")
            for table in layer_tables:
                # Get upstream dependencies to show convergence
                upstream_deps = table_dependencies.get(table, set())
                
                if len(upstream_deps) > 1:
                    # Multiple sources - show convergence
                    upstream_list = sorted(list(upstream_deps))
                    dag_lines.append(f"    {layer_config_item['emoji']} {table} ← ({', '.join(upstream_list)}) (DBT Snapshot - SCD Type 2)")
                    dag_lines.append(f"        └─ Converges {len(upstream_deps)} upstream tables into historical snapshot")
                elif len(upstream_deps) == 1:
                    # Single source
                    upstream_table = list(upstream_deps)[0]
                    dag_lines.append(f"    {layer_config_item['emoji']} {table} ← ({upstream_table}) (DBT Snapshot - SCD Type 2)")
                    dag_lines.append(f"        └─ Captures historical changes with validity dates")
                else:
                    # No dependencies found
                    dag_lines.append(f"    {layer_config_item['emoji']} {table} (DBT Snapshot - SCD Type 2)")
                    dag_lines.append(f"        └─ Captures historical changes with validity dates")
        
        else:
            # Regular transformation layers
            dag_lines.append(f"{layer_config_item['name']}:")
            
            # Sort tables within layer by dependencies (if any)
            def sort_by_dependencies(tables):
                # Simple sort - tables with fewer dependencies first
                return sorted(tables, key=lambda t: len(table_dependencies.get(t, set())))
            
            sorted_tables = sort_by_dependencies(layer_tables)
            
            for table in sorted_tables:
                step_info = all_tables[table]['step_info']
                column = step_info.get('column', 'unknown')
                transform_type = step_info.get('transformation_type', 'unknown')
                complexity = step_info.get("complexity_level", "").split(" - ")[0] if " - " in step_info.get("complexity_level", "") else ""
                
                # Get upstream dependencies for display
                upstream_deps = table_dependencies.get(table, set())
                
                # Format table with upstream
                if upstream_deps:
                    upstream_list = sorted(list(upstream_deps))
                    table_display = f"{table} ← ({', '.join(upstream_list)})"
                else:
                    table_display = table
                
                # Choose emoji based on complexity or layer
                if complexity == "HIGH":
                    emoji = "🔧"
                elif complexity == "MEDIUM":
                    emoji = "⚙️"
                else:
                    emoji = layer_config_item['emoji']
                
                dag_lines.append(f"    {emoji} {table_display}")
                dag_lines.append(f"        └─ {column} ({transform_type})")
                
                # Show transformation details
                if step_info.get("sql_expression"):
                    expr = step_info["sql_expression"]
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
                if step_info.get("has_cte_logic"):
                    cte_summary = step_info.get('cte_summary', 'CTE transformations')
                    dag_lines.append(f"        └─ Contains CTE: {cte_summary}")
        
        # Add flow arrow between layers (except after the last layer)
        if layer_key != 'mart':
            dag_lines.append("      ↓")
    
    # Show errors if any
    error_steps = [s for s in steps if s.get("step_type") == "ERROR"]
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