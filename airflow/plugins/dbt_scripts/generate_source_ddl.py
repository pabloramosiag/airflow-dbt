""" Script to generate DDL statements for creating source tables in Snowflake from dbt manifest.json file.

IMPORTANT: Remember to compile the dbt project before running this script.
"""

import argparse
import json
from pathlib import Path
from typing import Union


def read_manifest_file(manifest_path: Union[str, Path]) -> dict:
    """
    Reads and parses the dbt manifest JSON file.

    Args:
        manifest_path (str or Path): The file path to the manifest.json.

    Returns:
        dict: The parsed manifest as a dictionary.

    Raises:
        FileNotFoundError: If the manifest file does not exist at the specified path.
        ValueError: If the JSON file cannot be decoded.
    """
    try:
        with open(manifest_path, "r") as fp:
            manifest_dict = json.load(fp)
            # Uncomment the following line if parsing with dbt_artifacts_parser is needed
            # parse_manifest_v10(manifest=manifest_dict)
    except FileNotFoundError:
        raise FileNotFoundError(
            "The manifest.json file was not found at the specified path: %s",
            manifest_path,
        )
    except json.JSONDecodeError:
        raise ValueError("Error decoding the JSON file at the path: %s", manifest_path)
    return manifest_dict


def generate_single_source_ddl_from_table_key(
    sources: dict, table_key: str, replace: bool = False
) -> tuple:
    """
    Generates the DDL statement for a single table based on the provided table key.

    Args:
        sources (dict): The sources section from the manifest file.
        table_key (str): The unique key identifying the table in the sources.
        replace (bool, optional): Whether to replace the table if it exists. Defaults to False.

    Returns:
        tuple: A tuple containing the DDL statement, database, schema, table name, description, and column count.

    Raises:
        ValueError: If no information is found for the table key or if a column lacks a data type.
    """
    table_info = sources.get(table_key, None)
    if table_info is None:
        raise ValueError("No information found for the table key: %s", table_key)

    # Extract table information
    database = table_info.get("database")
    schema = table_info.get("schema")
    table_name = table_info.get("name")
    description = table_info.get("description")
    columns = table_info.get("columns", {})

    # Generate the DDL columns definition
    ddl_columns = []
    for column_name, column_info in columns.items():
        column_type = column_info.get("data_type")
        if not column_type:
            raise ValueError(
                "No data type found for column %s in table %s", column_name, table_name
            )
        # column_description = column_info.get("description", "")
        ddl_column = f"{column_name} {column_type}" # COMMENT '{column_description}'"
        ddl_columns.append(ddl_column)

    # Decide on the CREATE statement based on the replace flag
    create_statement = (
        "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    )
    ddl_statement = f"{create_statement} {schema}.{table_name} (\n"
    ddl_statement += ",\n".join(ddl_columns)
    ddl_statement += f"\n)" # COMMENT='{description}'"

    return ddl_statement, database, schema, table_name, description, len(columns)


def generate_source_ddl_from_manifest(
    source_name: str,
    manifest_dict: dict,
    tables: list = None,
    **kwargs,
) -> str:
    """
    Generates the complete DDL for all specified tables from the manifest.

    Args:
        source_name (str): The name of the source as defined in dbt.
        manifest_dict (dict): The parsed manifest dictionary.
        tables (list, optional): A list of specific tables to include. If None, all tables are included.
        **kwargs: Additional keyword arguments to pass to the table DDL generator.

    Returns:
        str: The complete DDL statements for the specified tables.

    Raises:
        ValueError: If no tables are found for the given source.
    """
    # Access the 'sources' dictionary from the manifest
    sources = manifest_dict.get("sources", {})

    # Create the search key in the manifest based on the source name
    source_key = f"source.dbt_scripts.{source_name}"

    # Select table keys that contain the source_key
    table_keys = [key for key in sources.keys() if source_key in key]
    available_tables = [key.split(".")[-1] for key in table_keys]

    if not table_keys:
        raise ValueError("No tables found for the source: %s", source_name)

    # If specific tables are provided, filter the table_keys accordingly
    if tables:
        table_keys = [key for key in table_keys if key.split(".")[-1] in tables]

    complete_ddl = ""
    # Iterate over each table key to generate its DDL
    for table_key in table_keys:
        (
            ddl,
            database,
            schema,
            table,
            description,
            column_count,
        ) = generate_single_source_ddl_from_table_key(
            sources, table_key=table_key, **kwargs
        )
        # Logging table information
        print("Table information found")
        print(f"Database: {database}")
        print(f"Schema: {schema}")
        print(f"Table name: {table}")
        print(f"Description: {description}")
        print(f"Number of columns found: {column_count}")
        print("-" * 80)
        # Append the generated DDL to the complete DDL
        complete_ddl += ddl + ";\n\n"

    generated_tables = [table_key.split(".")[-1] for table_key in table_keys]
    missing_tables = set(tables or []) - set(generated_tables)
    print(
        "#" * 80,
        "Complete DDL generated.",
        f"Tables included ({len(table_keys)}): {generated_tables}",
        sep="\n",
    )
    if missing_tables:
        print(
            "#" * 80,
            "[WARNING] Tables missing in the manifest!",
            f"Missing tables ({len(missing_tables)}): {missing_tables}",
            "Please check if the sources file is correctly defined and deployed.",
            f"Available tables: {available_tables}",
            sep="\n",
        )
    print("#" * 80)

    return complete_ddl


def generate_source_ddl(
    source_name: str,
    manifest_path: Union[str, Path] = None,
    tables: str = None,
    **kwargs,
) -> str:
    """
    Orchestrates the generation of DDL for the specified source and tables.

    Args:
        source_name (str): The name of the source as defined in dbt.
        manifest_path (str or Path, optional): The file path to the manifest.json. Defaults to None.
        tables (str, optional): Space-separated list of tables to include. Defaults to None.
        **kwargs: Additional keyword arguments to pass to the DDL generator.

    Returns:
        str: The complete DDL statements.

    Raises:
        ValueError: If the source name is not provided.
    """
    if not source_name:
        raise ValueError("Source name must be provided")

    if not tables:
        print(
            "[WARNING] No specific tables provided. "
            f"All tables from source {source_name} will be generated."
        )
    else:
        # Split the tables string into a list
        tables = tables.split()

    # Read and parse the manifest file
    manifest_dict = read_manifest_file(manifest_path)

    # Generate the DDL from the manifest
    return generate_source_ddl_from_manifest(
        source_name, manifest_dict=manifest_dict, tables=tables, **kwargs
    )
