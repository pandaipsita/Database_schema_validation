def chunk_tables(schema_list):
    """
    Process schema DDL statements into chunks with metadata.

    Args:
        schema_list: List of dictionaries containing schema information

    Returns:
        list: List of dictionaries with content and metadata
    """
    chunks = []

    for schema in schema_list:
        chunks.append({
            "content": schema["ddl"],
            "metadata": {
                "schema": schema["schema_name"],
                "table": schema["table_name"]
            }
        })

    # Count chunks per schema
    schema_counts = {}
    for chunk in chunks:
        schema_name = chunk["metadata"]["schema"]
        schema_counts[schema_name] = schema_counts.get(schema_name, 0) + 1

    print("Chunk counts by schema:")
    for schema, count in schema_counts.items():
        print(f"  {schema}: {count} chunks")

    return chunks