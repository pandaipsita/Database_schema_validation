import great_expectations as ge
import pandas as pd
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.validator.validator import Validator


def create_schema_expectations(df, context):
    """
    Create Great Expectations suite for schema validation

    Args:
        df: DataFrame containing schema information
        context: Great Expectations context

    Returns:
        suite: Expectation suite
    """
    # Create a new expectation suite
    expectation_suite_name = "schema_comparison_suite"
    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)

    # Create a validator
    batch = context.get_batch(
        {"table": df},
        expectation_suite_name=expectation_suite_name
    )

    # Add expectations
    batch.expect_column_to_exist("schema_name")
    batch.expect_column_to_exist("table_name")
    batch.expect_column_to_exist("ddl")
    batch.expect_column_values_to_not_be_null("ddl")

    # Add expectations for DDL format
    batch.expect_column_values_to_match_regex(
        "ddl",
        r"CREATE TABLE.*\(.*\);",
        mostly=1.0
    )

    # Save the expectation suite
    batch.save_expectation_suite(discard_failed_expectations=False)

    return context.get_expectation_suite(expectation_suite_name)


def validate_schemas(df1, df2, context):
    """
    Validate schemas against each other

    Args:
        df1: DataFrame for first schema
        df2: DataFrame for second schema
        context: Great Expectations context

    Returns:
        validation_result: Validation result
    """
    # Get the expectation suite
    expectation_suite = context.get_expectation_suite("schema_comparison_suite")

    # Create validation batches
    batch1 = context.get_batch(
        {"table": df1},
        expectation_suite_name="schema_comparison_suite"
    )

    batch2 = context.get_batch(
        {"table": df2},
        expectation_suite_name="schema_comparison_suite"
    )

    # Run validation
    result1 = batch1.validate()
    result2 = batch2.validate()

    # Combine results
    combined_result = {
        "schema1_validation": result1,
        "schema2_validation": result2,
        "success": result1.success and result2.success
    }

    return combined_result