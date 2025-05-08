from docx import Document
import re
import pandas as pd
import os


def extract_sql_schemas(file_path):
    # Determine file type by extension
    file_ext = os.path.splitext(file_path)[1].lower()

    # Extract schema name from file name (common for all file types)
    schema_name = os.path.basename(file_path).split('.')[0]

    # Read file content based on file type
    if file_ext == '.docx':
        # Use Document for .docx files
        doc = Document(file_path)
        full_text = "\n".join([para.text for para in doc.paragraphs])
    elif file_ext in ['.txt', '.sql']:
        # Use standard file reading for .txt and .sql files
        with open(file_path, 'r') as file:
            full_text = file.read()
    else:
        print(f"Unsupported file type: {file_ext}")
        return []

    # Split into individual CREATE TABLE statements - common for all file types
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