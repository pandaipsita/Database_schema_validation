from docx import Document
import re
import pandas as pd


def extract_sql_schemas(docx_path):
    doc = Document(docx_path)
    full_text = "\n".join([para.text for para in doc.paragraphs])

    # Extract schema name from document filename
    import os
    schema_name = os.path.basename(docx_path).split('.')[0]

    # Split into individual CREATE TABLE statements
    schema_blocks = re.findall(r"(CREATE TABLE[\s\S]+?\);)", full_text, re.IGNORECASE)

    cleaned_schemas = []
    for schema in schema_blocks:
        cleaned = schema.strip()
        if cleaned:
            # Parse table name
            table_match = re.search(r"CREATE TABLE\s+(?:(\w+)\.)?(\w+)", cleaned)
            if table_match:
                schema_prefix = table_match.group(1) or schema_name
                table_name = table_match.group(2)

                cleaned_schemas.append({
                    "schema_name": schema_prefix,
                    "table_name": table_name,
                    "ddl": cleaned
                })

    return cleaned_schemas


def schemas_to_dataframe(schema_list):
    """Convert schema list to a pandas DataFrame for Great Expectations"""
    return pd.DataFrame(schema_list)