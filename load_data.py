from parsers.docx_schema_parser import extract_sql_schemas
from database.chroma_store import store_schemas
from utils.chunk_utils import chunk_tables
import os
import yaml

# Load configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

data_dir = config["data_directory"]
files = sorted([f for f in os.listdir(data_dir) if f.endswith(".docx")])

all_schemas = []

for file in files:
    file_path = os.path.join(data_dir, file)
    print(f"📄 Parsing file: {file_path}")
    schemas = extract_sql_schemas(file_path)
    all_schemas.extend(schemas)

print(f"🧠 Total schemas extracted: {len(all_schemas)}")

# Generate and count chunks
chunks = chunk_tables(all_schemas)

# Store schemas in ChromaDB
store_schemas(chunks, config)