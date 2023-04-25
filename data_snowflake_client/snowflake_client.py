"""Client to interact with Snowflake."""
from __future__ import annotations

import pandas as pd
import structlog
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError
from snowflake.connector.pandas_tools import write_pandas

from data_slack_client.slack_client import SlackClient
from .helpers.logging_helper import log_and_raise_error, log_and_update_slack
from .models.config_model import SnowflakeConfig


class SnowflakeClient:
    """Client used to interact with Snowflake."""

    def __init__(self, config: SnowflakeConfig, slack_client: SlackClient | None = None):
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
            log_and_update_slack(slack_client=self.slack_client, message="Successfully connected to SnowflakeDB.",
                                 temp=True)
        except DatabaseError as e:
            log_and_raise_error(message=f"Failed to connect to SnowflakeDB. "
                                        f"Error : {e}.")
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

    def fetch_table_data(self, database: str, schema: str, table: str) -> pd.DataFrame:
        """Fetch data from a table in Snowflake as a pandas dataframe.

        Args:
            database: Name of the Snowflake database to fetch data.
            schema: Name of the Snowflake schema to fetch data.
            table: Name of the Snowflake table to fetch data.

        Returns:
            Pandas DataFrame with existing table data.
            None, if an error occurs or the table has no rows.

        Raises:
            ValueError: Could not fetch table data.
        """
        log_and_update_slack(
            slack_client=self.slack_client,
            message=f"Fetching data from SnowflakeDB. "
                    f"Table : {table}, Database: {database}, Schema: {schema}.",
            temp=True,
        )
        try:
            if self.connection is None:
                raise RuntimeError("No active connection to SnowflakeDB")
            self.connection.cursor().execute(f"USE DATABASE {database}")
            self.connection.cursor().execute(f"USE SCHEMA {schema}")
            cursor = self.connection.cursor().execute(f"SELECT * FROM {table}")
            if not cursor:
                raise ValueError("No valid cursor returned from Snowflake")
            dataframe = cursor.fetch_pandas_all()
        except (DatabaseError, RuntimeError, ValueError) as e:
            log_and_raise_error(f"Could not fetch data from SnowflakeDB. "
                                f"Table: {table}, Database: {database}, Schema: {schema}. "
                                f"Error {e}.")
        else:
            log_and_update_slack(
                slack_client=self.slack_client,
                message=f"Successfully fetched {dataframe.shape[0]} rows from SnowflakeDB. "
                        f"Table: {table}, Database: {database}, Schema: {schema}.",
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
    ) -> None:
        """Load a dataframe into a Snowflake table.

        If the table does not exist, a table is automatically created.
        Existing tables will be replaced with new tables.

        Args:
            dataframe: Pandas DataFrame with data to be loaded.
            database: Name of the Snowflake database to load data into.
            schema: Name of the Snowflake schema to load data into.
            table: Name of the Snowflake table to load data into.
            overwrite: Overwrite existing table.

        Raises:
            ValueError: Could not load dataframe.
        """
        log_and_update_slack(
            slack_client=self.slack_client,
            message=f"Loading data into SnowflakeDB. "
                    f"Table: {table}, Database: {database}, Schema: {schema}.",
            temp=True,
        )
        rows = 0
        chunks = 0
        try:
            if self.connection is None:
                raise RuntimeError("No active connection to SnowflakeDB")
            self.connection.cursor().execute(f"USE DATABASE {database}")
            self.connection.cursor().execute(f"USE SCHEMA {schema}")
            success, chunks, rows, _ = write_pandas(
                conn=self.connection,
                table_name=table,
                df=dataframe,
                auto_create_table=True,
                overwrite=overwrite,
                quote_identifiers=False,
            )
        except (DatabaseError, RuntimeError) as e:
            log_and_raise_error(message=f"Failed to insert {rows} rows into SnowflakeDB. "
                                        f"Table: {table}, Database: {database}, Schema: {schema}. "
                                        f"Error : {e}.")
        else:
            if success:
                log_and_update_slack(
                    slack_client=self.slack_client,
                    message=f"Successfully inserted {rows} rows in {chunks} chunks into SnowflakeDB. "
                            f"Table: {table}, Database: {database}, Schema: {schema}.",
                    temp=True,
                )

    def run_query(self, query: str, table: str, schema: str, database: str) -> None:
        """Run an SQL query on a Snowflake table.

        Args:
            query: SQL query.
            database: Name of the Snowflake database to run query.
            schema: Name of the Snowflake schema to run query.
            table: Name of the Snowflake table to run query.

        Raises:
            ValueError: Could not run query.
        """
        log_and_update_slack(
            slack_client=self.slack_client,
            message=f"Updating data in SnowflakeDB. "
                    f"Table: {table}, Database: {database}, Schema: {schema}.",
            temp=True,
        )
        try:
            if self.connection is None:
                raise RuntimeError("No active connection to SnowflakeDB")
            self.connection.cursor().execute(f"USE DATABASE {database}")
            self.connection.cursor().execute(f"USE SCHEMA {schema}")
            cursor = self.connection.cursor().execute(query)
            if not cursor:
                raise ValueError("No valid cursor returned from Snowflake")
        except (ValueError, RuntimeError, DatabaseError) as e:
            log_and_raise_error(message=f"Failed to run query: {query}. "
                                        f"Table: {table}, Database: {database}, Schema: {schema}."
                                        f"Error : {e}")
        else:
            log_and_update_slack(
                slack_client=self.slack_client,
                message=f"Successfully affected {cursor.rowcount} rows in SnowflakeDB. "
                        f"Table: {table}, Database: {database}, Schema: {schema}.",
                temp=True,
            )
