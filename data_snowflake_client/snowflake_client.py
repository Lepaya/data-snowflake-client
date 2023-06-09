"""Client to interact with Snowflake."""
from __future__ import annotations

import pandas as pd
import snowflake.connector
from data_slack_client.slack_client import SlackClient
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError
from snowflake.connector.pandas_tools import write_pandas

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

    def load_dataframe(
            self,
            dataframe: pd.DataFrame,
            database: str,
            schema: str,
            table: str,
            overwrite: bool,
            warehouse: str | None = None,
            role: str | None = None,
            quote_identifiers: bool = False
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
            warehouse: Name of the Snowflake warehouse to load data [Optional].
            role: Name of the Snowflake role to load data [Optional].
            quote_identifiers: True if identifiers should be quoted, else False [Optional].

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
            if role is not None:
                self.connection.cursor().execute(f"USE ROLE {role}")
            if warehouse is not None:
                self.connection.cursor().execute(f"USE WAREHOUSE {warehouse}")
            self.connection.cursor().execute(f"USE DATABASE {database}")
            self.connection.cursor().execute(f"USE SCHEMA {schema}")
            success, chunks, rows, _ = write_pandas(
                conn=self.connection,
                table_name=table,
                df=dataframe,
                auto_create_table=True,
                overwrite=overwrite,
                quote_identifiers=quote_identifiers,
            )
        except (DatabaseError, RuntimeError) as e:
            log_and_raise_error(message=f"Failed to insert {dataframe.shape[0]} rows into SnowflakeDB. "
                                        f"Error : {e}.")
        else:
            if success:
                log_and_update_slack(
                    slack_client=self.slack_client,
                    message=f"Successfully inserted {rows} rows in {chunks} chunks into SnowflakeDB. "
                            f"Table: {table}, Database: {database}, Schema: {schema}.",
                    temp=True,
                )

    def run_query(self, query: str, table: str, schema: str, database: str, warehouse: str | None = None,
                  role: str | None = None) -> pd.DataFrame:
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
        log_and_update_slack(
            slack_client=self.slack_client,
            message=f"Running query in SnowflakeDB. "
                    f"Table: {table}, Database: {database}, Schema: {schema}.",
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
            dataframe = cursor.fetch_pandas_all()
        except (ValueError, RuntimeError, DatabaseError) as e:
            log_and_raise_error(message=f"Failed to run query: {query}. "
                                        f"Error : {e}")
        else:
            log_and_update_slack(
                slack_client=self.slack_client,
                message=f"Successfully retrieved/affected {cursor.rowcount} rows in SnowflakeDB. "
                        f"Table: {table}, Database: {database}, Schema: {schema}.",
                temp=True,
            )
            return dataframe
