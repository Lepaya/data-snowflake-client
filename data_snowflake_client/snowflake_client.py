"""Client to interact with Snowflake."""

from __future__ import annotations

import json
from json import JSONDecodeError
from typing import List, Sequence, Union

import pandas as pd
import snowflake.connector
from data_slack_client.slack_client import SlackClient
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError
from snowflake.connector.pandas_tools import write_pandas

from .helpers.logging_helper import log, log_and_raise_error, log_and_update_slack
from .models.config_model import SnowflakeConfig


class SnowflakeClient:
    """Client used to interact with Snowflake."""

    def __init__(
        self, config: SnowflakeConfig, slack_client: SlackClient | None = None
    ):
        """
        Initialize the Snowflake Client.

        Args:
            config: Pydantic Snowflake config model.
            slack_client: SlackClient object [Optional]
        """
        self.account = config.account
        self.username = config.username
        self.password = config.password
        self.connection: SnowflakeConnection | None = None
        self.slack_client = slack_client

        self.test_database = "python_dev"
        self.test_schema = "ingest"

    def __enter__(self) -> SnowflakeClient:
        """
        Create a Snowflake Client in a context manager.

        Returns:
            The initialized Snowflake client.

        Raises:
            ValueError: Could not make connection to Snowflake.
        """
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.username,
                password=self.password,
            )
            log_and_update_slack(
                slack_client=self.slack_client,
                message="Successfully connected to SnowflakeDB.",
                temp=True,
            )
        except DatabaseError as e:
            log_and_raise_error(
                message=f"Failed to connect to SnowflakeDB. Error : {e}."
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        """
        Close the connection to Snowflake and exits context.

        Args:
            exc_type: N/a
            exc_val: N/a
            exc_tb: N/a
        """
        self.connection.close()  # type: ignore

    def fetch_table_data(
        self,
        database: str,
        schema: str,
        table: str,
        warehouse: str | None = None,
        role: str | None = None,
    ) -> pd.DataFrame:
        """Fetch data from a table in Snowflake as a pandas dataframe.

        Args:
            database: Name of the Snowflake database to fetch data.
            schema: Name of the Snowflake schema to fetch data.
            table: Name of the Snowflake table to fetch data.
            warehouse: Name of the Snowflake warehouse to run query [Optional].
            role: Name of the Snowflake role to run query [Optional].

        Returns:
            Pandas DataFrame with existing table data.
            None, if an error occurs or the table has no rows.

        Raises:
            ValueError: Could not fetch table data.
        """
        log_and_update_slack(
            slack_client=self.slack_client,
            message=f"Fetching data from {database}.{schema}.{table}",
            temp=True,
        )
        try:
            if self.connection is None:
                raise RuntimeError("No active connection to SnowflakeDB")
            if role is not None:
                self.connection.cursor().execute(f"USE ROLE {role}")
            if warehouse is not None:
                self.connection.cursor().execute(f"USE WAREHOUSE {warehouse}")
            self.connection.cursor().execute(f"USE DATABASE {database}")
            self.connection.cursor().execute(f"USE SCHEMA {schema}")
            cursor = self.connection.cursor().execute(f"SELECT * FROM {table}")
            if not cursor:
                raise ValueError("No valid cursor returned from Snowflake")
            dataframe = cursor.fetch_pandas_all()
        except (DatabaseError, RuntimeError, ValueError) as e:
            log_and_raise_error(
                f"Could not fetch data from {database}.{schema}.{table}" f"Error {e}."
            )
        else:
            log_and_update_slack(
                slack_client=self.slack_client,
                message=f"Successfully fetched {dataframe.shape[0]} rows from {database}.{schema}.{table}",
                temp=True,
            )
            return dataframe

    def load_dataframe(
        self,
        dataframe: pd.DataFrame,
        database: str,
        schema: str,
        table: str,
        overwrite: bool,
        warehouse: str | None = None,
        role: str | None = None,
        quote_identifiers: bool = False,
    ) -> tuple[
        bool,
        int,
        int,
        Sequence[
            tuple[
                str,
                str,
                int,
                int,
                int,
                int,
                str | None,
                int | None,
                int | None,
                str | None,
            ]
        ],
    ]:
        """Load a dataframe into a Snowflake table.

        If the table does not exist, a table is automatically created.
        Existing tables will be replaced with new tables.

        Args:
            dataframe: Pandas DataFrame with data to be loaded.
            database: Name of the Snowflake database to load data into.
            schema: Name of the Snowflake schema to load data into.
            table: Name of the Snowflake table to load data into.
            overwrite: Overwrite existing table.
            warehouse: Name of the Snowflake warehouse to load data [Optional].
            role: Name of the Snowflake role to load data [Optional].
            quote_identifiers: True if identifiers should be quoted, else False [Optional].

        Raises:
            ValueError: Could not load dataframe.

        Returns:
            Returns the COPY INTO command's results to verify ingestion in the form
            of a tuple of whether all chunks were ingested correctly, # of chunks,
            # of ingested rows, and ingest's output.
        """
        log_and_update_slack(
            slack_client=self.slack_client,
            message=f"Loading dataframe into {database}.{schema}.{table}",
            temp=True,
        )
        try:
            if self.connection is None:
                raise RuntimeError("No active connection to SnowflakeDB")
            if role is not None:
                self.connection.cursor().execute(f"USE ROLE {role}")
            if warehouse is not None:
                self.connection.cursor().execute(f"USE WAREHOUSE {warehouse}")

            # Load to temp and validate
            self.validate_schema(
                dataframe=dataframe,
                database=database,
                schema=schema,
                table=table,
                warehouse=warehouse,
                role=role,
            )

            # Load Data
            self.connection.cursor().execute(f"USE DATABASE {database}")
            self.connection.cursor().execute(f"USE SCHEMA {schema}")
            success, chunks, rows, output = write_pandas(
                conn=self.connection,
                table_name=table,
                df=dataframe,
                auto_create_table=True,
                overwrite=overwrite,
                quote_identifiers=quote_identifiers,
            )
        except (DatabaseError, RuntimeError) as e:
            log_and_raise_error(
                message=f"Failed to insert {dataframe.shape[0]} rows into {database}.{schema}.{table} "
                f"Error : {e}."
            )
        else:
            if success:
                log_and_update_slack(
                    slack_client=self.slack_client,
                    message=f"Successfully inserted {rows} rows in {chunks} chunks into {database}.{schema}.{table}",
                    temp=True,
                )
            return success, chunks, rows, output

    def run_query(
        self,
        query: str,
        table: str,
        schema: str,
        database: str,
        warehouse: str | None = None,
        role: str | None = None,
    ) -> Union[pd.DataFrame, List]:
        """Run an SQL query on a Snowflake table.

        Args:
            query: SQL query.
            database: Name of the Snowflake database to run query.
            schema: Name of the Snowflake schema to run query.
            table: Name of the Snowflake table to run query.
            warehouse: Name of the Snowflake warehouse to run query [Optional].
            role: Name of the Snowflake role to run query [Optional].

        Raises:
            ValueError: Could not run query.

        Returns:
            Pandas dataframe with the result of the query.
        """
        query_upper = query.upper()
        action = "Executing"
        if "SELECT" in query_upper:
            action = "Selecting"
        elif "INSERT" in query_upper:
            action = "Inserting"
        elif "UPDATE" in query_upper:
            action = "Updating"
        elif "DELETE" in query_upper:
            action = "Deleting"

        log_and_update_slack(
            slack_client=self.slack_client,
            message=f"{action} data in into {database}.{schema}.{table}",
            temp=True,
        )
        try:
            if self.connection is None:
                raise RuntimeError("No active connection to SnowflakeDB")
            if role is not None:
                self.connection.cursor().execute(f"USE ROLE {role}")
            if warehouse is not None:
                self.connection.cursor().execute(f"USE WAREHOUSE {warehouse}")
            self.connection.cursor().execute(f"USE DATABASE {database}")
            self.connection.cursor().execute(f"USE SCHEMA {schema}")
            cursor = self.connection.cursor().execute(query)
            if not cursor:
                raise ValueError("No valid cursor returned from Snowflake")
            dataframe = pd.DataFrame()
            try:
                if cursor.rowcount > 0:
                    if "SELECT" in query:
                        dataframe = cursor.fetch_pandas_all()
                    else:
                        dataframe = cursor.fetchall()
            except DatabaseError:
                log(message="Could not fetch any rows for this query.")
        except (ValueError, RuntimeError, DatabaseError) as e:
            log_and_raise_error(
                message=f"Failed to run query: {query} on {database}.{schema}.{table} "
                f"Error : {e}"
            )
        else:
            log_and_update_slack(
                slack_client=self.slack_client,
                message=f"Successfully retrieved {cursor.rowcount} rows.",
                temp=True,
            )
            return dataframe

    def validate_schema(
        self,
        dataframe: pd.DataFrame,
        database: str,
        schema: str,
        table: str,
        overwrite: bool = True,
        warehouse: Union[str, None] = None,
        role: Union[str, None] = None,
        quote_identifiers: bool = False,
    ) -> None:
        """
        Validate and update the schema of a Snowflake table based on a given DataFrame.

        Args:
            dataframe (pd.DataFrame): The DataFrame containing the new data to be validated.
            database (str): The name of the target database.
            schema (str): The name of the target schema.
            table (str): The name of the target table.
            overwrite (bool, optional): Whether to overwrite the existing table. Defaults to True.
            warehouse (Union[str, None], optional): The name of the warehouse to use. Defaults to None.
            role (Union[str, None], optional): The role to use. Defaults to None.
            quote_identifiers (bool, optional): Whether to quote identifiers. Defaults to False.

        Raises:
            DatabaseError: If there is an issue with the database operations.
            RuntimeError: If there is a runtime error during the validation.
            ValueError: If there is a value error during the schema validation.
            KeyError: If there is a key error during the schema validation.
            JSONDecodeError: If there is an error decoding JSON data.
            TypeError: If there is a type error during the schema validation.
            AttributeError: If there is an attribute error during the schema validation.
        """
        # Load to temp
        log_and_update_slack(
            slack_client=self.slack_client,
            message=(
                f"Loading data into temp table : {self.test_database}.{self.test_schema}.{table}."
            ),
            temp=True,
        )
        try:
            self.connection.cursor().execute(f"USE DATABASE {self.test_database}")
            self.connection.cursor().execute(f"USE SCHEMA {self.test_schema}")
            write_pandas(
                conn=self.connection,
                table_name=table,
                df=dataframe,
                auto_create_table=True,
                overwrite=overwrite,
                quote_identifiers=quote_identifiers,
            )
        except (DatabaseError, RuntimeError) as e:
            log_and_raise_error(
                message=(
                    f"Failed to create validation table and insert {dataframe.shape[0]} rows "
                    f"{self.test_database}.{self.test_schema}.{table}. Error: {e}."
                )
            )

        # Validate Schema
        log_and_update_slack(
            slack_client=self.slack_client,
            message=(
                f"Validating schema against temp table : {self.test_database}.{self.test_schema}.{table}."
            ),
            temp=True,
        )
        existing_columns = self.run_query(
            query=f"SHOW COLUMNS IN {database}.{schema}.{table}",
            table=table,
            schema=schema,
            database=database,
            warehouse=warehouse,
            role=role,
        )
        new_columns = self.run_query(
            query=f"SHOW COLUMNS IN {self.test_database}.{self.test_schema}.{table}",
            table=table,
            schema=self.test_schema,
            database=self.test_database,
            warehouse=warehouse,
            role=role,
        )

        try:
            existing_columns_df = pd.DataFrame(existing_columns)
            new_columns_df = pd.DataFrame(new_columns)
        except (AttributeError, TypeError, KeyError, ValueError) as e:
            log_and_raise_error(
                message=f"Could not convert columns list to dataframe. Error: {e}"
            )

        # Dictionary to map aliases to Snowflake data types
        data_type_mapping = {
            "FIXED": "NUMBER",
            "REAL": "FLOAT",
            "TEXT": "STRING",
            "CHARACTER": "CHAR",
            "VARCHAR": "VARCHAR",
            "BOOLEAN": "BOOL",
            "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
            "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
            "TIMESTAMP_TZ": "TIMESTAMP_TZ",
            "DATE": "DATE",
            "TIME": "TIME",
            "BINARY": "BINARY",
            "VARIANT": "VARIANT",
            "OBJECT": "OBJECT",
            "ARRAY": "ARRAY",
            "GEOGRAPHY": "GEOGRAPHY",
        }

        # Iterate over the new columns and add any that do not exist in the existing columns
        column_names_index = 2
        data_type_index = 3
        for _, row in new_columns_df.iterrows():
            try:
                if (
                    row[column_names_index]
                    not in existing_columns_df[column_names_index].tolist()
                ):
                    new_column = row[column_names_index]

                    data_type_info = json.loads(row[data_type_index])
                    column_type = data_type_info["type"]
                    nullable = data_type_info.get("nullable", True)
                    default_value = data_type_info.get(
                        "default", None
                    )  # Get default value if present

                    # Map the column type using the dictionary
                    column_type_mapped = data_type_mapping.get(column_type, column_type)

                    # Form the column definition based on type
                    if column_type_mapped == "NUMBER":
                        precision = data_type_info.get("precision", None)
                        scale = data_type_info.get("scale", None)
                        if precision is not None and scale is not None:
                            column_definition = f"NUMBER({precision}, {scale})"
                        elif precision is not None:
                            column_definition = f"NUMBER({precision})"
                        else:
                            column_definition = "NUMBER"
                    elif column_type_mapped in ["CHAR", "VARCHAR"]:
                        length = data_type_info.get("length", None)
                        if length is not None:
                            column_definition = f"{column_type_mapped}({length})"
                        else:
                            column_definition = column_type_mapped
                    else:
                        column_definition = column_type_mapped

                    # Add NULL or NOT NULL
                    column_definition += " NULL" if nullable else " NOT NULL"

                    # Add DEFAULT value if present, else set to NULL
                    if default_value is not None:
                        if isinstance(default_value, str):
                            default_value = f"'{default_value}'"
                        column_definition += f" DEFAULT {default_value}"
                    else:
                        column_definition += " DEFAULT NULL"

                    # Construct the ALTER TABLE statement
                    alter_table_stmt = f"""
                    ALTER TABLE {database}.{schema}.{table}
                    ADD COLUMN {new_column} {column_definition}
                    """

                    # Execute the ALTER TABLE statement
                    self.run_query(
                        query=alter_table_stmt,
                        table=table,
                        schema=schema,
                        database=database,
                        warehouse=warehouse,
                        role=role,
                    )
                    log_and_update_slack(
                        slack_client=self.slack_client,
                        message=(
                            f"Successfully added new column: {new_column} with data type: "
                            f"{column_definition} in {database}.{schema}.{table}."
                        ),
                        temp=False,
                    )
            except (
                ValueError,
                KeyError,
                JSONDecodeError,
                TypeError,
                AttributeError,
                RuntimeError,
            ) as e:
                log_and_raise_error(
                    message=(
                        f"Failed to alter table and add new column: {new_column} with data type: "
                        f"{column_definition} in {self.test_database}.{self.test_schema}.{table}. "
                        f"Error: {e}."
                    )
                )
