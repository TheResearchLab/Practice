import sqlglot
from sqlglot import exp

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