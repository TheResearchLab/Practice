import sqlglot
from sqlglot import exp

def extract_snowflake_columns(sql_query, existing_cte_registry=None):
    """
    Extracts column lineage information from a Snowflake SQL query.
    Returns a list of lists, each describing the output columns for each SELECT.
    FIXED: Now handles SELECT * properly and accepts existing CTE registry
    FIXED: Now properly handles UNION ALL - analyzes ALL SELECT statements
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
    
    # Build CTE registry first (or use existing one)
    cte_registry = existing_cte_registry or {}
    with_clause = parsed.args.get("with")
    
    # If no existing registry, build it from this query
    if not existing_cte_registry and with_clause:
        for cte in with_clause.expressions:
            cte_name = cte.alias
            cte_query = cte.this  # The SELECT part of the CTE
            cte_registry[cte_name.lower()] = cte_query
    
    # CRITICAL FIX: Find ALL SELECT statements, including those in UNION operations
    def find_all_selects(node):
        """Recursively find all SELECT statements, including those in UNION operations"""
        selects = []
        
        if isinstance(node, exp.Select):
            selects.append(node)
        elif isinstance(node, exp.Union):
            # For UNION operations, recursively find selects in left and right sides
            if hasattr(node, 'left') and node.left:
                selects.extend(find_all_selects(node.left))
            if hasattr(node, 'right') and node.right:
                selects.extend(find_all_selects(node.right))
        elif hasattr(node, 'expressions'):
            # Check if any expressions contain selects
            for expr in node.expressions:
                selects.extend(find_all_selects(expr))
        
        # Also check common node attributes that might contain selects
        for attr_name in ['this', 'expression']:
            if hasattr(node, attr_name):
                attr_value = getattr(node, attr_name)
                if attr_value:
                    selects.extend(find_all_selects(attr_value))
        
        return selects
    
    # Get all SELECT statements (including those in UNIONs)
    all_selects = find_all_selects(parsed)
    
    # FIXED: Filter out CTE definition selects (same logic as before)
    cte_select_ids = set()
    if with_clause:
        for cte in with_clause.expressions:
            cte_select = cte.this
            # Mark all selects within this CTE (including nested UNIONs)
            cte_selects = find_all_selects(cte_select)
            for cte_sel in cte_selects:
                cte_select_ids.add(id(cte_sel))
    
    # Only include selects that are NOT part of CTE definitions
    final_selects = [select for select in all_selects if id(select) not in cte_select_ids]
    
    # If no final selects found, fall back to all selects (for simple queries)
    if not final_selects:
        final_selects = all_selects
    
    print(f"🔍 Found {len(final_selects)} SELECT statements to analyze (including UNION branches)")
    
    all_columns = []
    
    for idx, select in enumerate(final_selects):
        print(f"   📊 Analyzing SELECT statement {idx + 1}/{len(final_selects)}")
        
        select_columns = []
        from_tables = get_from_tables(select, cte_registry)
        only_table = list(from_tables.values())[0][0] if len(from_tables) == 1 else None
        
        for proj in select.expressions:
            alias = proj.alias_or_name
            expression_sql = expr_to_str(proj)
            source_columns = collect_source_columns(proj)
            resolved_sources = []
            
            # FIXED: Handle SELECT * expansion
            if proj.is_star:
                # For SELECT *, create dependencies on all source tables
                for table_alias, (full_table, real_alias) in from_tables.items():
                    resolved_sources.append((full_table, real_alias, "*"))
                col_type = "star"
            else:
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
                "type": col_type,
                "union_branch": idx  # Track which UNION branch this came from
            })
        
        all_columns.append(select_columns)
    
    return all_columns


def trace_column_lineage(sql_query, target_column_name, existing_cte_registry=None):
    """
    Traces a specific column through all transformations and builds LLM-ready context.
    FIXED: Properly handles aliases, single names, and recursive CTE resolution
    FIXED: Now handles UNION ALL - traces column through ALL SELECT statements
    """
    
    def should_stop_tracing(full_table_name, internal_prefixes=['ph_'], cte_registry=None):
        """Determine if we should STOP tracing (found external source)"""
        
        # Always continue for CTE references - they need recursive resolution
        if full_table_name.startswith("CTE:"):
            return False, "cte_reference"
        
        # Check if this is actually a CTE name (without CTE: prefix)
        if cte_registry and full_table_name.lower() in cte_registry:
            return False, "is_cte_name"
        
        # Parse the table name
        parts = full_table_name.split('.')
        
        if len(parts) >= 3:
            # Full qualified name: database.schema.table
            database = parts[0]
            database_lower = database.lower()
            starts_with_internal = any(database_lower.startswith(prefix.lower()) for prefix in internal_prefixes)
            
            if not starts_with_internal:
                return True, "external_database"  # STOP - external database
            else:
                return False, "internal_database"  # CONTINUE - internal database
                
        elif len(parts) == 2:
            # schema.table - check if schema indicates external
            schema = parts[0].lower()
            if any(schema.startswith(prefix.lower()) for prefix in internal_prefixes):
                return False, "internal_schema"  # CONTINUE - internal schema
            else:
                return True, "external_schema"  # STOP - external schema
            
        else:
            # Single name - check if it's a CTE first, then treat as external
            if cte_registry and full_table_name.lower() in cte_registry:
                return False, "single_name_is_cte"  # CONTINUE - it's a CTE
            else:
                return True, "external_source_table"  # STOP - treat as external
    
    # Parse and build CTE registry first (or use existing one for nested calls)
    parsed = sqlglot.parse_one(sql_query, dialect="snowflake")
    cte_registry = existing_cte_registry or {}
    with_clause = parsed.args.get("with")
    
    # If no existing registry, build it from this query
    if not existing_cte_registry and with_clause:
        for cte in with_clause.expressions:
            cte_name = cte.alias
            cte_query = cte.this
            cte_registry[cte_name.lower()] = cte_query
    
    # Get basic column analysis for ALL SELECT statements (pass CTE registry for nested CTE detection)
    base_columns = extract_snowflake_columns(sql_query, cte_registry)
    
    # Find the target column in ALL SELECT statements (UNION branches)
    target_column_matches = []
    
    for select_idx, select_columns in enumerate(base_columns):
        for col_info in select_columns:
            if col_info['target_column'].lower() == target_column_name.lower():
                target_column_matches.append({
                    'col_info': col_info,
                    'select_branch': select_idx + 1
                })
            # Also check if target column could come from SELECT *
            elif col_info['type'] == 'star':
                # For star selections, assume any requested column could be available
                target_column_matches.append({
                    'col_info': {
                        'target_column': target_column_name,
                        'expression': f"* (includes {target_column_name})",
                        'type': 'star',
                        'resolved_source_columns': col_info['resolved_source_columns'],
                        'select_idx': select_idx,
                        'union_branch': select_idx
                    },
                    'select_branch': select_idx + 1
                })
    
    if not target_column_matches:
        return {
            "error": f"Column '{target_column_name}' not found in any SELECT statement",
            "llm_context": f"The column '{target_column_name}' was not found in any of the {len(base_columns)} SELECT statements.",
            "next_columns_to_search": [],
            "full_lineage": {}
        }
    
    print(f"🔍 Found '{target_column_name}' in {len(target_column_matches)} SELECT statement(s)")
    
    # Build LLM context combining all branches
    llm_context_parts = []
    all_next_columns = []
    all_cte_transformations = []
    
    # Basic column information
    llm_context_parts.append(f"COLUMN: {target_column_name}")
    llm_context_parts.append(f"FOUND IN: {len(target_column_matches)} SELECT statement(s) (UNION branches)")
    
    # Process each branch where the column appears
    for match_idx, match in enumerate(target_column_matches):
        col_info = match['col_info']
        branch_num = match['select_branch']
        
        llm_context_parts.append(f"\nUNION BRANCH {branch_num}:")
        llm_context_parts.append(f"EXPRESSION: {col_info['expression']}")
        llm_context_parts.append(f"TRANSFORMATION TYPE: {col_info['type']}")
        
        # Process resolved sources for this branch
        resolved_sources = col_info.get('resolved_source_columns', [])
        
        if resolved_sources:
            llm_context_parts.append(f"SOURCE ANALYSIS FOR BRANCH {branch_num}:")
            
            # Group resolved sources by table to detect multiple dependencies from same table/CTE
            sources_by_table = {}
            for source in resolved_sources:
                if len(source) >= 3:  # (table, alias, column)
                    table, alias, column = source[:3]
                    table_key = table
                    # For star selections, use the target column name instead of "*"
                    if column == "*":
                        column = target_column_name
                elif len(source) == 2:  # (table, column)
                    table, column = source
                    alias = table
                    table_key = table
                    # For star selections, use the target column name
                    if column == "*":
                        column = target_column_name
                else:
                    continue
                    
                if table_key not in sources_by_table:
                    sources_by_table[table_key] = []
                sources_by_table[table_key].append((table, alias, column))
            
            # Process each table's dependencies for this branch
            for table_key, table_sources in sources_by_table.items():
                for table, alias, column in table_sources:
                    # Check if this is a CTE reference
                    if table.startswith("CTE:"):
                        cte_name = table.replace("CTE:", "")
                        llm_context_parts.append(f"  └─ CTE REFERENCE: {alias}.{column} → {cte_name}.{column}")
                        
                        # Trace through the CTE
                        if cte_name.lower() in cte_registry:
                            cte_query = cte_registry[cte_name.lower()]
                            cte_sql = cte_query.sql(dialect="snowflake")
                            
                            # Recursively analyze the CTE with the current CTE registry
                            cte_trace = trace_column_lineage(cte_sql, column, cte_registry)
                            if "error" not in cte_trace:
                                # Add CTE transformation info
                                all_cte_transformations.append({
                                    "cte_name": cte_name,
                                    "column": column,
                                    "transformation_type": "intra_file_cte",
                                    "union_branch": branch_num,
                                    "details": cte_trace.get("llm_context", ""),
                                    "dependencies": cte_trace.get("next_columns_to_search", [])
                                })
                                
                                # Add CTE's dependencies to our next_columns with branch info
                                for cte_dep in cte_trace.get("next_columns_to_search", []):
                                    all_next_columns.append({
                                        "table": cte_dep["table"],
                                        "column": cte_dep["column"],
                                        "context": f"External source for {target_column_name} via CTE {cte_name} (branch {branch_num})",
                                        "level": "external_via_cte",
                                        "cte_intermediate": cte_name,
                                        "union_branch": branch_num
                                    })
                                
                                # Add CTE context to LLM output
                                llm_context_parts.append(f"    └─ TRACING CTE '{cte_name}' (INTRA-FILE TRANSFORMATION):")
                                cte_context_lines = cte_trace["llm_context"].split('\n')
                                for line in cte_context_lines:
                                    if line.strip():
                                        llm_context_parts.append(f"      {line}")
                            else:
                                llm_context_parts.append(f"    ERROR tracing CTE: {cte_trace['error']}")
                        else:
                            llm_context_parts.append(f"    WARNING: CTE '{cte_name}' not found in registry")
                            
                    else:
                        # Regular table reference - check if it's actually a CTE first
                        should_stop, reason = should_stop_tracing(table, cte_registry=cte_registry)
                        
                        if not should_stop and reason in ["is_cte_name", "single_name_is_cte"]:
                            # This is actually a CTE that we need to trace through
                            cte_name = table
                            llm_context_parts.append(f"  └─ NESTED CTE REFERENCE: {alias}.{column} → {cte_name}.{column}")
                            
                            if cte_name.lower() in cte_registry:
                                cte_query = cte_registry[cte_name.lower()]
                                cte_sql = cte_query.sql(dialect="snowflake")
                                
                                # Recursively analyze the nested CTE
                                cte_trace = trace_column_lineage(cte_sql, column, cte_registry)
                                if "error" not in cte_trace:
                                    # Add CTE transformation info
                                    all_cte_transformations.append({
                                        "cte_name": cte_name,
                                        "column": column,
                                        "transformation_type": "nested_cte",
                                        "union_branch": branch_num,
                                        "details": cte_trace.get("llm_context", ""),
                                        "dependencies": cte_trace.get("next_columns_to_search", [])
                                    })
                                    
                                    # Add CTE's dependencies to our next_columns
                                    for cte_dep in cte_trace.get("next_columns_to_search", []):
                                        all_next_columns.append({
                                            "table": cte_dep["table"],
                                            "column": cte_dep["column"],
                                            "context": f"External source for {target_column_name} via nested CTE {cte_name} (branch {branch_num})",
                                            "level": "external_via_cte",
                                            "cte_intermediate": cte_name,
                                            "union_branch": branch_num
                                        })
                        
                        elif should_stop:
                            # External table - add to dependencies
                            llm_context_parts.append(f"  └─ EXTERNAL TABLE: {table}.{column} (referenced as {alias}.{column}) - {reason}")
                            all_next_columns.append({
                                "table": table,
                                "column": column,
                                "context": f"External table dependency for {target_column_name} (branch {branch_num})",
                                "level": "external_table",
                                "source_reason": reason,
                                "union_branch": branch_num
                            })
                        else:
                            # Internal table - would need further tracing in full system
                            llm_context_parts.append(f"  └─ INTERNAL TABLE: {table}.{column} (referenced as {alias}.{column}) - {reason}")
                            all_next_columns.append({
                                "table": table,
                                "column": column,
                                "context": f"Internal table dependency for {target_column_name} (branch {branch_num})",
                                "level": "internal_table",
                                "trace_reason": reason,
                                "union_branch": branch_num
                            })
    
    # Show CTE transformations summary
    if all_cte_transformations:
        llm_context_parts.append(f"\nINTRA-FILE CTE TRANSFORMATIONS:")
        for cte_info in all_cte_transformations:
            branch_info = f" (branch {cte_info.get('union_branch', '?')})" if len(target_column_matches) > 1 else ""
            column_info = cte_info.get('column', 'unknown')
            llm_context_parts.append(f"  CTE '{cte_info['cte_name']}' transforms {column_info}{branch_info}")
            llm_context_parts.append(f"    └─ Type: {cte_info.get('transformation_type', 'unknown')}")
    
    # Show CTE definitions if relevant
    if cte_registry:
        llm_context_parts.append(f"\nAVAILABLE CTEs IN THIS FILE:")
        for cte_name in cte_registry.keys():
            llm_context_parts.append(f"  - {cte_name}")
    
    # Remove duplicates from all_next_columns but preserve branch information
    unique_next_columns = []
    seen = set()
    for col in all_next_columns:
        # Use table, column, and branch for uniqueness
        key = (col['table'], col['column'], col.get('level', 'unknown'), col.get('union_branch', 0))
        if key not in seen:
            seen.add(key)
            unique_next_columns.append(col)
    
    # Summary information
    total_dependencies = len(unique_next_columns)
    branches_with_deps = len(set(col.get('union_branch', 0) for col in unique_next_columns))
    
    llm_context_parts.append(f"\nUNION ANALYSIS SUMMARY:")
    llm_context_parts.append(f"  Total SELECT branches analyzed: {len(target_column_matches)}")
    llm_context_parts.append(f"  Total unique dependencies found: {total_dependencies}")
    llm_context_parts.append(f"  Branches with dependencies: {branches_with_deps}")
    
    return {
        "llm_context": "\n".join(llm_context_parts),
        "next_columns_to_search": unique_next_columns,
        "cte_transformations": all_cte_transformations,
        "full_lineage": target_column_matches[0]['col_info'] if target_column_matches else {},
        "union_branches_analyzed": len(target_column_matches),
        "total_unique_dependencies": total_dependencies
    }