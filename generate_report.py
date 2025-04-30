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
    files = [f for f in os.listdir(data_dir) if f.endswith(".docx")]

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

    # Explicitly define schema names
    schema1 = "employee_management"
    schema2 = "contractor_management"

    schema_names = [schema1, schema2]  # Use the variables for schema names

    # Ensure we have both schemas
    if len(schema_names) != 2:
        print(f"⚠️ Missing one or both of the required schemas ({schema1}, {schema2})")
        available_schemas = list(dataframes.keys())
        if len(available_schemas) >= 2:
            print(f"Using available schemas instead: {available_schemas[0]}, {available_schemas[1]}")
            schema_names = available_schemas[:2]
        else:
            print(f"⚠️ Need at least two schema files to generate comparison report")
            return

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
        print(f"Comparing schemas: {schema_names[0]} (source) and {schema_names[1]} (destination)")

        df1 = dataframes[schema_names[0]]
        df2 = dataframes[schema_names[1]]

        # Extract DDL strings for comparison
        schema1_full = "\n".join([schema["ddl"] for schema in all_schemas if schema["schema_name"] == schema_names[0]])
        schema2_full = "\n".join([schema["ddl"] for schema in all_schemas if schema["schema_name"] == schema_names[1]])

        # Step 5: Generate detailed comparison report
        comparison_report = compare_schemas(
            schema1_full,
            schema2_full,
            schema1_name=schema_names[0],
            schema2_name=schema_names[1]
        )

        # Generate a final report in JSON format
        report_path = f"validation_reports/validation_reports.json"
        with open(report_path, "w") as f:
            json.dump(comparison_report, f, indent=2)

        print(f"✅ Comparison report saved to {report_path}")

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
    print(f"Available schemas: {source_schema}, {destination_schema}")

    while True:
        question = input("\nQuestion: ")
        if question.lower() in ["exit", "quit"]:
            break

        # Check for schema-specific queries and extract the relevant tables accordingly
        if "contractor_management" in question.lower():
            result = chain.run(facts=json.dumps({"tables": schema2_tables}, indent=2), question=question)
        elif "employee_management" in question.lower():
            result = chain.run(facts=json.dumps({"tables": schema1_tables}, indent=2), question=question)
        else:
            result = chain.run(facts=json.dumps({"tables": {source_schema: schema1_tables, destination_schema: schema2_tables}}, indent=2), question=question)

        print("\nAnswer:", result)


if __name__ == "__main__":
    main()
