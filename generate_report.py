import json
import yaml
import os
import great_expectations as ge
from parsers.docx_schema_parser import extract_sql_schemas, schemas_to_dataframe
from utils.chunk_utils import chunk_tables
from database.chroma_store import store_schemas
from validators.schema_comparator import compare_schemas
from langchain_ollama import OllamaLLM
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain


def main():
    # Load configuration
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Initialize Great Expectations context
    try:
        context = ge.data_context.DataContext(config.get("ge_dir", "./great_expectations"))
        print("✅ Great Expectations context loaded")
        use_ge = True
    except Exception as e:
        print(f"⚠️ Could not load Great Expectations context: {e}")
        print("Continuing without Great Expectations...")
        use_ge = False

    # Step 1: Load and parse schemas from docx files
    data_dir = config["data_directory"]
   # files = [f for f in os.listdir(data_dir) if f.endswith(".docx")]
    # In both instances of main() function where you have this line:
    files = [f for f in os.listdir(data_dir) if f.endswith((".docx", ".txt", ".sql"))]

    all_schemas = []
    dataframes = {}

    for file in files:
        file_path = os.path.join(data_dir, file)
        print(f"📄 Parsing file: {file_path}")
        schemas = extract_sql_schemas(file_path)
        all_schemas.extend(schemas)

        # Create dataframe for this schema
        schema_name = os.path.basename(file_path).split('.')[0]
        df = schemas_to_dataframe(schemas)
        dataframes[schema_name] = df

    print(f"🧠 Total schemas extracted: {len(all_schemas)}")

    # Load schema names from config
    schema_names = config.get("schemas", [])

    if len(schema_names) != 2:
        print(f"⚠️ Missing one or both of the required schemas in config.yaml")
        available_schemas = list(dataframes.keys())
        if len(available_schemas) >= 2:
            print(f"Using available schemas instead: {available_schemas[0]}, {available_schemas[1]}")
            schema_names = available_schemas[:2]
        else:
            print(f"⚠️ Need at least two schema files to generate comparison report")
            return

    schema1, schema2 = schema_names

    # Step 2: Generate and count chunks
    chunks = chunk_tables(all_schemas)

    # Step 3: Convert schemas to vector representations and store
    try:
        collection = store_schemas(chunks, config)
        print("✅ Schemas stored in vector database")
    except Exception as e:
        print(f"⚠️ Error storing schemas: {e}")
        return

    # Generate validation report
    if len(schema_names) >= 2:
        print(f"Comparing schemas: {schema1} (source) and {schema2} (destination)")

        df1 = dataframes[schema1]
        df2 = dataframes[schema2]

        # Extract DDL strings for comparison
        schema1_full = "\n".join([schema["ddl"] for schema in all_schemas if schema["schema_name"] == schema1])
        schema2_full = "\n".join([schema["ddl"] for schema in all_schemas if schema["schema_name"] == schema2])

        # Step 5: Generate detailed comparison report
        comparison_report = compare_schemas(
            schema1_full,
            schema2_full,
            schema1_name=schema1,
            schema2_name=schema2
        )

        # Generate a final report in JSON format
        report_path = f"validation_reports/validation_reports.json"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w") as f:
            json.dump(comparison_report, f, indent=2)

        print(f"✅ Comparison report saved to {report_path}")

        # Generate HTML report
        generate_html_report(comparison_report, f"validation_reports/validation_report.html")
        print(f"✅ HTML report saved to validation_reports/validation_report.html")

        # Use LLM to generate a natural language summary
        try:
            llm = OllamaLLM(model="llama3")
            prompt = PromptTemplate(
                template="""
                You are a database expert. Analyze this schema comparison report and provide a concise summary:

                {report}

                Provide a clear summary of:
                1. Major differences between the schemas
                2. Key similarities
                3. Potential issues or inconsistencies
                4. Recommendations for schema alignment
                """,
                input_variables=["report"]
            )

            chain = LLMChain(llm=llm, prompt=prompt)
            result = chain.run(report=json.dumps(comparison_report, indent=2))

            # Save the summary
            summary_path = f"validation_reports/validation_reports_summary.txt"
            with open(summary_path, "w") as f:
                f.write(result)

            print(f"✅ Summary report saved to {summary_path}")
        except Exception as e:
            print(f"⚠️ Error generating summary: {e}")

    else:
        print(f"⚠️ Need at least two schema files to generate comparison report")

    # ---- Q&A with Schema Information ----
    # Load the actual comparison report
    report_path = "validation_reports/validation_reports.json"

    if not os.path.exists(report_path):
        print(f"Error: Report file not found: {report_path}")
        exit(1)

    with open(report_path, "r") as f:
        actual_report = json.load(f)

    # Extract key information to provide as facts to the LLM
    source_schema = actual_report["meta"]["source_schema"]
    destination_schema = actual_report["meta"]["destination_schema"]

    # Extract detailed table mapping information
    schema1_tables = []
    schema2_tables = []
    table_mappings = []

    for result in actual_report["results"]:
        table_name = result["kwargs"]["table"]
        status = result.get("meta", {}).get("status", "")

        if isinstance(status, str):
            if f"missing in {destination_schema}" in status:
                schema1_tables.append(table_name)
            elif f"missing in {source_schema}" in status:
                schema2_tables.append(table_name)
            else:
                schema1_tables.append(table_name)
                schema2_tables.append(table_name)

                # For common tables, extract column differences
                if "column_results" in result.get("meta", {}):
                    column_differences = []
                    for col_result in result["meta"]["column_results"]:
                        if "missing" in col_result:
                            column_differences.append(col_result)

                    # Only add mapping for tables with differences
                    if column_differences:
                        table_mappings.append({
                            "table": table_name,
                            "differences": column_differences
                        })

    # Handle questions for Q&A
    llm = OllamaLLM(model="llama3", temperature=0.1)  # Lower temperature for more factual responses
    prompt = PromptTemplate(
        template="""You are a database expert with access to the following FACTS about two database schemas:

FACTS:
{facts}

Do not add any information beyond what is stated in the FACTS. If you don't know something based on the FACTS, say "I don't have that information."

User question: {question}

Answer:""",
        input_variables=["facts", "question"]
    )

    chain = LLMChain(llm=llm, prompt=prompt)

    # Interactive Q&A loop
    print("Database Schema Expert Ready! Ask questions about the compared schemas, or type 'exit' to quit.")
    print(f"Available schemas: {source_schema} database, {destination_schema} database")

    while True:
        question = input("\nQuestion: ")
        if question.lower() in ["exit", "quit"]:
            break

        # Check for schema-specific queries and extract the relevant tables accordingly
        if schema2.lower() in question.lower():
            result = chain.run(facts=json.dumps({"tables": schema2_tables}, indent=2), question=question)
        elif schema1.lower() in question.lower():
            result = chain.run(facts=json.dumps({"tables": schema1_tables}, indent=2), question=question)
        else:
            result = chain.run(
                facts=json.dumps({"tables": {source_schema: schema1_tables, destination_schema: schema2_tables}},
                                 indent=2),
                question=question
            )

        print("\nAnswer:", result)


def generate_html_report(report_data, output_path):
    """Generate an HTML report based on the JSON comparison report."""
    # Extract metadata
    meta_data = report_data["meta"]
    source_schema = meta_data["source_schema"]
    destination_schema = meta_data["destination_schema"]
    timestamp = meta_data["timestamp"]
    ge_version = meta_data["great_expectations_version"]

    # Start building HTML content
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Schema Validation Report</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      background-color: #fafafa;
      padding: 20px;
    }}
    h1 {{
      font-size: 24px;
    }}
    .meta-info {{
      background-color: #f0f0f0;
      padding: 15px;
      border-radius: 5px;
      margin-bottom: 20px;
    }}
    .meta-row {{
      margin-bottom: 5px;
    }}
    .meta-label {{
      font-weight: bold;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 20px;
    }}
    th {{
      background-color: #333;
      color: white;
      padding: 10px;
      text-align: left;
    }}
    td {{
      padding: 10px;
      vertical-align: top;
      background-color: #fff;
    }}
    .table-row td {{
      background-color: #fff7db;
    }}
    .failed td {{
      background-color: #f8d7da;
    }}
    .success-details {{
      color: green;
    }}
    .error-details {{
      color: red;
    }}
    .details-title {{
      font-weight: bold;
      margin-top: 8px;
    }}
    .status-success {{
      color: green;
    }}
    .status-failed {{
      color: red;
    }}
    .statistics {{
      margin: 20px 0;
      padding: 10px;
      background-color: #e9f7ef;
      border-radius: 5px;
    }}
  </style>
</head>
<body>

  <h1>Schema Validation Report</h1>

  <div class="meta-info">
    <div class="meta-row"><span class="meta-label">Great Expectations Version:</span> {ge_version}</div>
    <div class="meta-row"><span class="meta-label">Source Schema:</span> {source_schema}</div>
    <div class="meta-row"><span class="meta-label">Destination Schema:</span> {destination_schema}</div>
    <div class="meta-row"><span class="meta-label">Timestamp:</span> {timestamp}</div>
  </div>

  <table border="1">
    <thead>
      <tr>
        <th>Table</th>
        <th>Status</th>
        <th>Details</th>
      </tr>
    </thead>
    <tbody>
"""

    # Process each result
    for result in report_data["results"]:
        table_name = result["kwargs"]["table"]
        meta = result.get("meta", {})
        status = meta.get("status", "")
        success = result.get("success", False)

        # Determine the status display
        status_class = "status-success" if success else "status-failed"
        status_text = "✅ Success" if success else "❌ Failed"

        # Get column results if they exist
        column_results = meta.get("column_results", [])

        # Check if we have success and error details to split
        success_details = []
        error_details = []

        for col_result in column_results:
            if col_result.startswith("✅"):
                success_details.append(col_result)
            elif col_result.startswith("❌"):
                error_details.append(col_result)

        # If we have both success and error details
        if success_details and error_details:
            html_content += f"""
      <tr class="table-row">
        <td rowspan="2">{table_name}</td>
        <td rowspan="2" class="{status_class}">{status_text}</td>
        <td class="success-details">
          <div class="details-title">✅ Success:</div>
          {'<br>'.join(success_details)}
        </td>
      </tr>
      <tr class="failed">
        <td class="error-details">
          <div class="details-title">❌ Missing:</div>
          {'<br>'.join(error_details)}
        </td>
      </tr>
"""
        # If we only have success details
        elif success_details:
            html_content += f"""
      <tr class="table-row">
        <td>{table_name}</td>
        <td class="{status_class}">{status_text}</td>
        <td class="success-details">
          <div class="details-title">✅ Success:</div>
          {'<br>'.join(success_details)}
        </td>
      </tr>
"""
        # If we have no details or only error details (table missing case)
        else:
            html_content += f"""
      <tr class="failed">
        <td>{table_name}</td>
        <td class="{status_class}">{status_text}</td>
        <td class="error-details">❌ Table missing</td>
      </tr>
"""

    # Close HTML
    html_content += """
    </tbody>
  </table>

</body>
</html>"""

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write the HTML file
    with open(output_path, "w") as f:
        f.write(html_content)


if __name__ == "__main__":
    main()