import re
import json
import datetime
import pkg_resources  # For getting the installed package version


def compare_schemas(schema1, schema2, schema1_name=None, schema2_name=None):
    """
    Compare two SQL schemas and generate a detailed comparison report in Great Expectations style.

    Args:
        schema1 (str): First schema DDL statements
        schema2 (str): Second schema DDL statements
        schema1_name (str, optional): Name of the first schema
        schema2_name (str, optional): Name of the second schema

    Returns:
        dict: Report of schema comparison
    """
    # Get the installed Great Expectations version
    try:
        ge_version = pkg_resources.get_distribution("great-expectations").version
    except:
        ge_version = "unknown"

    # Parse tables from each schema
    def parse_tables(schema):
        tables = {}
        table_matches = re.findall(r"CREATE TABLE ([^\s(]+)\s*\((.*?)\);", schema, re.DOTALL)

        for table_name, columns_str in table_matches:
            # Clean table name (remove schema prefix if exists)
            full_name = table_name
            clean_name = table_name.split('.')[-1]

            # Parse columns
            columns = {}
            column_defs = re.findall(r"([^,]+)", columns_str)
            for col_def in column_defs:
                col_def = col_def.strip()
                if col_def:
                    parts = col_def.split(None, 1)
                    if len(parts) >= 2:
                        col_name, col_type = parts
                        columns[col_name] = col_type

            tables[clean_name.lower()] = {
                "full_name": full_name,
                "columns": columns
            }

        return tables

    tables1 = parse_tables(schema1)
    tables2 = parse_tables(schema2)

    # If schema names aren't provided, default to schema1 and schema2
    schema1_name = schema1 if schema1_name is None else schema1_name
    schema2_name = schema2 if schema2_name is None else schema2_name

    # Build GE-style report
    report = {
        "success": True,
        "meta": {
            "great_expectations_version": ge_version,
            "source_schema": schema1_name,
            "destination_schema": schema2_name,
            "timestamp": datetime.datetime.now().isoformat()
        },
        "results": []
    }

    # Get all tables from both schemas
    all_tables = sorted(set(list(tables1.keys()) + list(tables2.keys())))

    # Evaluate each table
    for table in all_tables:
        expectation = {
            "expectation_type": "expect_table_schema_to_match",
            "kwargs": {
                "table": table
            },
            "meta": {
                "column_results": []
            }
        }

        # Check if table exists in both schemas
        if table in tables1 and table in tables2:
            expectation["success"] = True
            expectation["meta"]["status"] = "✅"

            # Compare columns
            all_columns = sorted(set(list(tables1[table]["columns"].keys()) + list(tables2[table]["columns"].keys())))

            for col in all_columns:
                if col in tables1[table]["columns"] and col in tables2[table]["columns"]:
                    datatype1 = tables1[table]["columns"][col]
                    datatype2 = tables2[table]["columns"][col]

                    if datatype1 == datatype2:
                        expectation["meta"]["column_results"].append(
                            f"✅ Column '{col}' matches with datatype: {datatype1}")
                    else:
                        expectation["success"] = False
                        expectation["meta"]["column_results"].append(
                            f"❌ Column '{col}' has different datatypes: {datatype1} vs {datatype2}")
                elif col in tables1[table]["columns"]:
                    expectation["meta"]["column_results"].append(f"❌ Column '{col}' is missing in {schema2_name}")
                elif col in tables2[table]["columns"]:
                    expectation["meta"]["column_results"].append(f"❌ Column '{col}' is missing in {schema1_name}")

        # Add to the report
        report["results"].append(expectation)

    return report
