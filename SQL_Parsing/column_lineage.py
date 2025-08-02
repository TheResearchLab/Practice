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
    def get_from_tables(select):
        """
        Returns a dict mapping alias (lowercase) -> (full_table_name, alias)
        """
        tables = {}
        
        from_expr = select.args.get("from")
        if from_expr:
            # Base table
            base = from_expr.args.get("this")
            if isinstance(base, exp.Table):
                db = base.catalog or ""
                schema = base.db or ""
                name = base.name
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
                    if db and schema:
                        full_name = f"{db}.{schema}.{name}"
                    elif schema:
                        full_name = f"{schema}.{name}"
                    else:
                        full_name = name
                    alias = join_table.alias or name
                    tables[alias.lower()] = (full_name, alias)
                        
        return tables
    
    selects = [node for node in parsed.walk() if isinstance(node, exp.Select)]
    all_columns = []
    
    for idx, select in enumerate(selects):
        select_columns = []
        from_tables = get_from_tables(select)
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


def analyze_column_transformations(sql_query):
    """
    Analyzes column transformations and lineage through CTEs and complex expressions.
    Returns detailed transformation information including data flow and control flow dependencies.
    """
    parsed = sqlglot.parse_one(sql_query, dialect="snowflake")
    
    # Step 1: Build CTE registry
    def build_cte_registry(parsed_query):
        """Map CTE names to their SELECT definitions"""
        cte_registry = {}
        
        # Find WITH clause
        with_clause = parsed_query.args.get("with")
        if with_clause:
            for cte in with_clause.expressions:
                cte_name = cte.alias
                cte_query = cte.this  # The SELECT part of the CTE
                cte_registry[cte_name.lower()] = cte_query
        
        return cte_registry
    
    # Step 2: Analyze expressions for complex transformations
    def analyze_expression(expr, available_tables):
        """
        Analyze an expression to determine its transformation type and dependencies
        """
        if isinstance(expr, exp.Column):
            # Simple column reference
            table_ref = expr.table if expr.table else ""
            return {
                "type": "direct",
                "expression": expr.sql(dialect="snowflake"),
                "data_flow_deps": [(table_ref, expr.name)],
                "control_flow_deps": [],
                "transformation_details": "Direct column reference"
            }
        
        elif isinstance(expr, exp.Binary):
            # Arithmetic operations: a + b, a - b, etc.
            left_analysis = analyze_expression(expr.left, available_tables)
            right_analysis = analyze_expression(expr.right, available_tables)
            
            return {
                "type": "calculated",
                "expression": expr.sql(dialect="snowflake"),
                "data_flow_deps": left_analysis["data_flow_deps"] + right_analysis["data_flow_deps"],
                "control_flow_deps": left_analysis["control_flow_deps"] + right_analysis["control_flow_deps"],
                "transformation_details": f"Binary operation: {expr.key}"
            }
        
        elif isinstance(expr, exp.Case):
            # CASE WHEN statements
            deps = []
            control_deps = []
            
            # Analyze all WHEN conditions and values
            for case in expr.args.get("ifs", []):
                condition_analysis = analyze_expression(case.this, available_tables)  # WHEN condition
                value_analysis = analyze_expression(case.expression, available_tables)  # THEN value
                
                control_deps.extend(condition_analysis["data_flow_deps"])  # Conditions are control flow
                deps.extend(value_analysis["data_flow_deps"])  # Values are data flow
            
            # Handle ELSE clause
            if expr.args.get("default"):
                else_analysis = analyze_expression(expr.args.get("default"), available_tables)
                deps.extend(else_analysis["data_flow_deps"])
            
            return {
                "type": "conditional",
                "expression": expr.sql(dialect="snowflake"),
                "data_flow_deps": deps,
                "control_flow_deps": control_deps,
                "transformation_details": "CASE WHEN conditional logic"
            }
        
        elif isinstance(expr, exp.Window):
            # Window functions: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
            func_analysis = analyze_expression(expr.this, available_tables) if expr.this else {"data_flow_deps": [], "control_flow_deps": []}
            
            partition_deps = []
            order_deps = []
            
            # PARTITION BY columns are control flow
            partition_by = expr.args.get("partition_by")
            if partition_by:
                for partition_col in partition_by:
                    part_analysis = analyze_expression(partition_col, available_tables)
                    partition_deps.extend(part_analysis["data_flow_deps"])
            
            # ORDER BY columns are control flow
            order_by = expr.args.get("order")
            if order_by:
                for order_col in order_by.expressions:
                    order_analysis = analyze_expression(order_col, available_tables)
                    order_deps.extend(order_analysis["data_flow_deps"])
            
            return {
                "type": "window_function",
                "expression": expr.sql(dialect="snowflake"),
                "data_flow_deps": func_analysis.get("data_flow_deps", []),
                "control_flow_deps": partition_deps + order_deps,
                "transformation_details": f"Window function with partitioning and ordering",
                "partition_by": partition_deps,
                "order_by": order_deps
            }
        
        elif isinstance(expr, exp.Func):
            # Aggregate functions: SUM, COUNT, etc.
            deps = []
            for arg in expr.expressions:
                arg_analysis = analyze_expression(arg, available_tables)
                deps.extend(arg_analysis["data_flow_deps"])
            
            return {
                "type": "aggregated",
                "expression": expr.sql(dialect="snowflake"),
                "data_flow_deps": deps,
                "control_flow_deps": [],
                "transformation_details": f"Aggregate function: {expr.key}"
            }
        
        else:
            # Fallback for other expression types
            deps = []
            for node in expr.walk():
                if isinstance(node, exp.Column):
                    table_ref = node.table if node.table else ""
                    deps.append((table_ref, node.name))
            
            return {
                "type": "complex",
                "expression": expr.sql(dialect="snowflake"),
                "data_flow_deps": deps,
                "control_flow_deps": [],
                "transformation_details": f"Complex expression: {type(expr).__name__}"
            }
    
    # Step 3: Resolve CTE references recursively
    def resolve_cte_lineage(table_ref, column_name, cte_registry, visited=None):
        """
        Recursively resolve a column reference through CTEs back to source tables
        """
        if visited is None:
            visited = set()
        
        if table_ref in visited:
            return [{"error": f"Circular reference detected in CTE: {table_ref}"}]
        
        if table_ref.lower() not in cte_registry:
            # This is a base table, not a CTE
            return [{
                "source_table": table_ref,
                "source_column": column_name,
                "transformation_type": "source",
                "step": "base_table"
            }]
        
        visited.add(table_ref)
        cte_query = cte_registry[table_ref.lower()]
        
        # Get column information from the CTE
        cte_columns = extract_snowflake_columns(cte_query.sql(dialect="snowflake"))
        
        lineage_chain = []
        
        # Find the column in the CTE's output
        for select_branch in cte_columns:
            for col_info in select_branch:
                if col_info["target_column"].lower() == column_name.lower():
                    # Found the column, now trace its sources
                    cte_step = {
                        "cte_name": table_ref,
                        "transformation_type": col_info["type"],
                        "expression": col_info["expression"],
                        "step": f"cte_{table_ref}"
                    }
                    
                    # Recursively resolve each resolved source column (not just source_columns)
                    resolved_sources = col_info.get("resolved_source_columns", [])
                    if resolved_sources:
                        for resolved_source in resolved_sources:
                            if len(resolved_source) >= 3:  # (full_table, alias, column)
                                source_table, alias, source_col = resolved_source[:3]
                                upstream_lineage = resolve_cte_lineage(source_table, source_col, cte_registry, visited.copy())
                                lineage_chain.extend(upstream_lineage)
                            elif len(resolved_source) == 2:  # (table, column)
                                source_table, source_col = resolved_source
                                upstream_lineage = resolve_cte_lineage(source_table, source_col, cte_registry, visited.copy())
                                lineage_chain.extend(upstream_lineage)
                    else:
                        # Fall back to basic source_columns
                        for source_table, source_col in col_info["source_columns"]:
                            if source_table:
                                upstream_lineage = resolve_cte_lineage(source_table, source_col, cte_registry, visited.copy())
                                lineage_chain.extend(upstream_lineage)
                    
                    lineage_chain.append(cte_step)
                    break
        
        return lineage_chain
    
    # Step 4: Main analysis
    cte_registry = build_cte_registry(parsed)
    base_columns = extract_snowflake_columns(sql_query)
    
    transformation_analysis = []
    
    for select_idx, select_columns in enumerate(base_columns):
        select_analysis = {
            "select_branch": select_idx + 1,
            "columns": []
        }
        
        for col_info in select_columns:
            try:
                # Get the expression analysis
                parsed_expr = sqlglot.parse_one(col_info["expression"], dialect="snowflake")
                
                # Build available tables context (this would need the from_tables info)
                available_tables = {}  # This should be populated with the table context
                
                expr_analysis = analyze_expression(parsed_expr, available_tables)
                
                # Build full lineage chain using resolved sources
                lineage_chains = []
                
                # Use resolved_source_columns if available, otherwise fall back to source_columns
                resolved_sources = col_info.get("resolved_source_columns", [])
                if resolved_sources:
                    for resolved_source in resolved_sources:
                        if len(resolved_source) >= 3:  # (full_table, alias, column)
                            source_table, alias, source_col = resolved_source[:3]
                            lineage = resolve_cte_lineage(source_table, source_col, cte_registry)
                            lineage_chains.append({
                                "source_reference": f"{source_table}.{source_col}",
                                "resolved_reference": f"{alias}.{source_col}" if alias != source_table else f"{source_table}.{source_col}",
                                "lineage_chain": lineage
                            })
                        elif len(resolved_source) == 2:  # (table, column)
                            source_table, source_col = resolved_source
                            lineage = resolve_cte_lineage(source_table, source_col, cte_registry)
                            lineage_chains.append({
                                "source_reference": f"{source_table}.{source_col}",
                                "resolved_reference": f"{source_table}.{source_col}",
                                "lineage_chain": lineage
                            })
                else:
                    # Fall back to basic source_columns
                    for source_table, source_col in col_info["source_columns"]:
                        if source_table:
                            lineage = resolve_cte_lineage(source_table, source_col, cte_registry)
                            lineage_chains.append({
                                "source_reference": f"{source_table}.{source_col}",
                                "resolved_reference": f"{source_table}.{source_col}",
                                "lineage_chain": lineage
                            })
                
                column_analysis = {
                    "target_column": col_info["target_column"],
                    "expression": col_info["expression"],
                    "basic_type": col_info["type"],
                    "transformation_analysis": expr_analysis,
                    "lineage_chains": lineage_chains,
                    "resolved_sources": col_info.get("resolved_source_columns", [])
                }
                
            except Exception as e:
                # Fallback for expressions we can't parse
                column_analysis = {
                    "target_column": col_info["target_column"],
                    "expression": col_info["expression"],
                    "basic_type": col_info["type"],
                    "transformation_analysis": {
                        "type": "parse_error",
                        "expression": col_info["expression"],
                        "data_flow_deps": col_info["source_columns"],
                        "control_flow_deps": [],
                        "transformation_details": f"Parse error: {str(e)}"
                    },
                    "lineage_chains": [],
                    "resolved_sources": col_info.get("resolved_source_columns", [])
                }
            
            select_analysis["columns"].append(column_analysis)
        
        transformation_analysis.append(select_analysis)
    
    return {
        "cte_registry": {name: cte.sql(dialect="snowflake") for name, cte in cte_registry.items()},
        "transformation_analysis": transformation_analysis
    }

def trace_column_lineage(sql_query, target_column_name):
    """
    Traces a specific column through all transformations and builds LLM-ready context.
    
    Args:
        sql_query: The SQL query to analyze
        target_column_name: Name of the final output column to trace
    
    Returns:
        {
            "llm_context": "Human-readable description of the column's journey",
            "next_columns_to_search": [{"table": "table_name", "column": "column_name"}],
            "full_lineage": {...}  # Detailed technical lineage
        }
    """
    
    # Get the transformation analysis
    analysis = analyze_column_transformations(sql_query)
    
    # Find the target column in the final output
    target_column_info = None
    target_select_branch = None
    
    for select_analysis in analysis['transformation_analysis']:
        for col_analysis in select_analysis['columns']:
            if col_analysis['target_column'].lower() == target_column_name.lower():
                target_column_info = col_analysis
                target_select_branch = select_analysis['select_branch']
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
    
    # Start with the target column description
    expr_analysis = target_column_info['transformation_analysis']
    llm_context_parts.append(f"COLUMN: {target_column_name}")
    llm_context_parts.append(f"EXPRESSION: {target_column_info['expression']}")
    llm_context_parts.append(f"TRANSFORMATION TYPE: {expr_analysis['type']}")
    llm_context_parts.append(f"TRANSFORMATION DETAILS: {expr_analysis['transformation_details']}")
    
    if target_select_branch:
        llm_context_parts.append(f"FOUND IN: SELECT branch {target_select_branch}")
    
    # Add data flow and control flow information
    if expr_analysis.get('data_flow_deps'):
        data_deps = [f"{table}.{col}" if table else col for table, col in expr_analysis['data_flow_deps']]
        llm_context_parts.append(f"DATA DEPENDENCIES: {', '.join(data_deps)}")
    
    if expr_analysis.get('control_flow_deps'):
        control_deps = [f"{table}.{col}" if table else col for table, col in expr_analysis['control_flow_deps']]
        llm_context_parts.append(f"CONTROL DEPENDENCIES: {', '.join(control_deps)}")
    
    # Add specific transformation context
    if expr_analysis['type'] == 'window_function':
        if expr_analysis.get('partition_by'):
            partition_cols = [f"{table}.{col}" if table else col for table, col in expr_analysis['partition_by']]
            llm_context_parts.append(f"PARTITIONED BY: {', '.join(partition_cols)}")
        if expr_analysis.get('order_by'):
            order_cols = [f"{table}.{col}" if table else col for table, col in expr_analysis['order_by']]
            llm_context_parts.append(f"ORDERED BY: {', '.join(order_cols)}")
    
    # Process lineage chains to build context and find next columns
    if target_column_info.get('lineage_chains'):
        llm_context_parts.append("\nLINEAGE TRACE:")
        
        for chain in target_column_info['lineage_chains']:
            llm_context_parts.append(f"\nSource: {chain['source_reference']}")
            if chain.get('resolved_reference') and chain['resolved_reference'] != chain['source_reference']:
                llm_context_parts.append(f"Resolved as: {chain['resolved_reference']}")
            
            # Process each step in the lineage chain
            for step_idx, step in enumerate(chain['lineage_chain']):
                if 'error' in step:
                    llm_context_parts.append(f"  ERROR: {step['error']}")
                elif step['step'] == 'base_table':
                    llm_context_parts.append(f"  └─ BASE TABLE: {step['source_table']}.{step['source_column']}")
                    # This is a source table - add to next_columns
                    next_columns.append({
                        "table": step['source_table'],
                        "column": step['source_column'],
                        "context": f"Ultimate source column for {target_column_name}",
                        "level": "base_table"
                    })
                else:
                    # This is a CTE step - also add these as intermediate dependencies to search
                    llm_context_parts.append(f"  └─ CTE '{step['cte_name']}': {step['expression']}")
                    llm_context_parts.append(f"     Transformation: {step['transformation_type']}")
                    
                    # Add CTE as an intermediate dependency to search
                    next_columns.append({
                        "table": step['cte_name'],
                        "column": chain['source_reference'].split('.')[-1],  # Extract column name
                        "context": f"Intermediate transformation for {target_column_name} via CTE {step['cte_name']}",
                        "level": "cte_intermediate"
                    })
    else:
        # No lineage chains - check resolved sources directly
        resolved_sources = target_column_info.get('resolved_sources', [])
        if resolved_sources:
            llm_context_parts.append("\nDIRECT SOURCES:")
            for source in resolved_sources:
                if len(source) >= 3:  # (table, alias, column)
                    table, alias, column = source[:3]
                    llm_context_parts.append(f"  └─ {table}.{column} (referenced as {alias}.{column})")
                    next_columns.append({
                        "table": table,
                        "column": column,
                        "context": f"Direct source for {target_column_name}",
                        "level": "direct"
                    })
                elif len(source) == 2:  # (table, column)
                    table, column = source
                    llm_context_parts.append(f"  └─ {table}.{column}")
                    next_columns.append({
                        "table": table,
                        "column": column,
                        "context": f"Direct source for {target_column_name}",
                        "level": "direct"
                    })
    
    # IMPORTANT: Also add all data flow and control flow dependencies as searchable columns
    all_dependencies = []
    all_dependencies.extend(expr_analysis.get('data_flow_deps', []))
    all_dependencies.extend(expr_analysis.get('control_flow_deps', []))
    
    for table_ref, col_name in all_dependencies:
        if table_ref:  # Skip empty table references
            # Try to resolve the table reference through resolved sources
            resolved_table = table_ref
            
            # Check if this is a CTE reference that we need to resolve
            resolved_sources = target_column_info.get('resolved_sources', [])
            for source in resolved_sources:
                if len(source) >= 3 and source[1] == table_ref:  # (full_table, alias, column)
                    resolved_table = source[0]
                    break
                elif len(source) == 2 and table_ref in source[0]:  # Partial match
                    resolved_table = source[0]
                    break
            
            next_columns.append({
                "table": resolved_table,
                "column": col_name,
                "context": f"Expression dependency for {target_column_name} (referenced as {table_ref}.{col_name})",
                "level": "expression_dependency"
            })
    
    # Add CTEs context if relevant
    cte_registry = analysis.get('cte_registry', {})
    if cte_registry:
        referenced_ctes = []
        for chain in target_column_info.get('lineage_chains', []):
            for step in chain['lineage_chain']:
                if step['step'] != 'base_table' and 'cte_name' in step:
                    referenced_ctes.append(step['cte_name'])
        
        if referenced_ctes:
            llm_context_parts.append(f"\nRELEVANT CTEs:")
            for cte_name in set(referenced_ctes):
                if cte_name.lower() in cte_registry:
                    llm_context_parts.append(f"\nCTE '{cte_name}':")
                    llm_context_parts.append(f"{cte_registry[cte_name.lower()]}")
    
    # Remove duplicates from next_columns but keep different levels
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
        "full_lineage": target_column_info
    }


def build_complete_column_context(sql_query, target_column_name, external_column_fetcher=None):
    """
    Builds complete LLM context by recursively following all source columns.
    
    Args:
        sql_query: The SQL query to analyze
        target_column_name: Name of the final output column to trace
        external_column_fetcher: Optional function(table, column) -> context_string for external data
    
    Returns:
        {
            "complete_context": "Full LLM-ready context including external sources",
            "source_tables_analyzed": ["list", "of", "tables"],
            "lineage_summary": "High-level summary of the data flow"
        }
    """
    
    initial_trace = trace_column_lineage(sql_query, target_column_name)
    
    if 'error' in initial_trace:
        return initial_trace
    
    context_parts = [initial_trace['llm_context']]
    next_to_search = initial_trace['next_columns_to_search']
    searched_tables = set()
    
    # If we have an external column fetcher, get additional context
    if external_column_fetcher and next_to_search:
        context_parts.append("\n" + "="*60)
        context_parts.append("EXTERNAL SOURCE COLUMN INFORMATION:")
        context_parts.append("="*60)
        
        # Group by level for better organization
        base_tables = [col for col in next_to_search if col.get('level') == 'base_table']
        cte_intermediates = [col for col in next_to_search if col.get('level') == 'cte_intermediate']
        expression_deps = [col for col in next_to_search if col.get('level') == 'expression_dependency']
        direct_sources = [col for col in next_to_search if col.get('level') == 'direct']
        
        # Process base tables first (ultimate sources)
        if base_tables:
            context_parts.append("\nULTIMATE SOURCE TABLES:")
            for col_info in base_tables:
                table = col_info['table']
                column = col_info['column']
                
                if (table, column) not in searched_tables:
                    searched_tables.add((table, column))
                    
                    try:
                        external_context = external_column_fetcher(table, column)
                        context_parts.append(f"\n  SOURCE: {table}.{column}")
                        context_parts.append(f"  CONTEXT: {col_info['context']}")
                        context_parts.append(f"  EXTERNAL INFO: {external_context}")
                    except Exception as e:
                        context_parts.append(f"\n  SOURCE: {table}.{column}")
                        context_parts.append(f"  CONTEXT: {col_info['context']}")
                        context_parts.append(f"  ERROR fetching external info: {str(e)}")
        
        # Process expression dependencies (intermediate transformations)
        if expression_deps:
            context_parts.append("\nEXPRESSION DEPENDENCIES:")
            for col_info in expression_deps:
                table = col_info['table']
                column = col_info['column']
                
                if (table, column) not in searched_tables:
                    searched_tables.add((table, column))
                    
                    try:
                        external_context = external_column_fetcher(table, column)
                        context_parts.append(f"\n  DEPENDENCY: {table}.{column}")
                        context_parts.append(f"  CONTEXT: {col_info['context']}")
                        context_parts.append(f"  EXTERNAL INFO: {external_context}")
                    except Exception as e:
                        context_parts.append(f"\n  DEPENDENCY: {table}.{column}")
                        context_parts.append(f"  CONTEXT: {col_info['context']}")
                        context_parts.append(f"  ERROR fetching external info: {str(e)}")
        
        # Process CTE intermediates and direct sources
        remaining_cols = cte_intermediates + direct_sources
        if remaining_cols:
            context_parts.append("\nOTHER DEPENDENCIES:")
            for col_info in remaining_cols:
                table = col_info['table']
                column = col_info['column']
                
                if (table, column) not in searched_tables:
                    searched_tables.add((table, column))
                    
                    try:
                        external_context = external_column_fetcher(table, column)
                        context_parts.append(f"\n  {col_info.get('level', 'other').upper()}: {table}.{column}")
                        context_parts.append(f"  CONTEXT: {col_info['context']}")
                        context_parts.append(f"  EXTERNAL INFO: {external_context}")
                    except Exception as e:
                        context_parts.append(f"\n  {col_info.get('level', 'other').upper()}: {table}.{column}")
                        context_parts.append(f"  CONTEXT: {col_info['context']}")
                        context_parts.append(f"  ERROR fetching external info: {str(e)}")
    
    # Build lineage summary with more detail
    all_base_sources = [col for col in next_to_search if col.get('level') == 'base_table']
    all_expression_deps = [col for col in next_to_search if col.get('level') == 'expression_dependency']
    
    base_tables = list(set([col['table'] for col in all_base_sources]))
    dep_tables = list(set([col['table'] for col in all_expression_deps]))
    
    if base_tables and dep_tables:
        lineage_summary = f"Column '{target_column_name}' is derived from {len(all_expression_deps)} intermediate column(s) which ultimately trace to {len(all_base_sources)} base column(s) in {len(base_tables)} table(s): {', '.join(base_tables)}"
    elif base_tables:
        lineage_summary = f"Column '{target_column_name}' is derived from {len(all_base_sources)} column(s) in {len(base_tables)} table(s): {', '.join(base_tables)}"
    else:
        source_tables = list(set([col['table'] for col in next_to_search]))
        lineage_summary = f"Column '{target_column_name}' is derived from {len(next_to_search)} column(s) across {len(source_tables)} sources: {', '.join(source_tables)}"
    
    return {
        "complete_context": "\n".join(context_parts),
        "source_tables_analyzed": base_tables if base_tables else list(set([col['table'] for col in next_to_search])),
        "lineage_summary": lineage_summary,
        "next_columns_to_search": next_to_search
    }