"""Client to interact with Snowflake."""
from __future__ import annotations

import pandas as pd
import snowflake.connector
import structlog
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import NotSupportedError, ProgrammingError
from snowflake.connector.pandas_tools import write_pandas

from lepaya_python_slackclient.slack_client import SlackClient
from helpers.logging_helper import log_and_raise_error, log_and_update_slack
from models.config_model import SnowflakeConfig

LOGGER = structlog.get_logger()


class SnowflakeClient:
    """Client used to interact with Snowflake."""

    def __init__(self, config: SnowflakeConfig, slack_client: SlackClient):
        """
        Initialize the Snowflake Client.

        Args:
            config: Pydantic Snowflake config model.
            slack_client: SlackClient object
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
        """
        self.connection = snowflake.connector.connect(
            account=self.account,
            user=self.username,
            password=self.password,
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
            RuntimeError: the client has no active connection.
            ValueError: if no valid cursor is returned from Snowflake.
        """
        if self.connection is None:
            raise RuntimeError

        LOGGER.info(
            f"Fetching data from SnowflakeDB. Table : {table},"
            f"Database: {database}, Schema: {schema}",
        )
        try:
            self.connection.cursor().execute(f"USE DATABASE {database}")
            self.connection.cursor().execute(f"USE SCHEMA {schema}")
            cursor = self.connection.cursor().execute(f"SELECT * FROM {table}")
            if not cursor:
                error_message = "No valid cursor returned from Snowflake"
                LOGGER.error(error_message)
                raise ValueError(error_message)
            dataframe = cursor.fetch_pandas_all()
        except (ProgrammingError, NotSupportedError) as e:
            error_message = (
                f"Could not fetch data from Table: {table},"
                f"Database: {database},Schema: {schema} "
                f" Error {e}"
            )
            self.slack_client.add_error_block(error_message=error_message)
            LOGGER.error(error_message)
            raise ValueError(error_message) from e
        else:
            log_and_update_slack(
                slack_client=self.slack_client,
                message=f"Successfully fetched {dataframe.shape[0]} rows "
                f"from Table: {table},Database: {database}, Schema: {schema}",
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
            RuntimeError: the client has no active connection.
        """
        if self.connection is None:
            raise RuntimeError

        LOGGER.info(
            f"Loading data into SnowflakeDB. Database: {database},"
            f"Schema: {schema}, Table: {table}",
        )
        rows = 0
        chunks = 0
        try:
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
        except (ValueError, ProgrammingError) as e:
            error_msg = (
                f"Failed to insert {rows} rows in {chunks} chunks into Table: {table}"
                f"Error : {e}"
            )
            self.slack_client.add_error_block(error_message=error_msg)
            log_and_raise_error(message=error_msg)
        else:
            if success:
                log_and_update_slack(
                    slack_client=self.slack_client,
                    message=f"Successfully inserted {rows} rows in"
                    f"{chunks} chunks into Table: {table}",
                    temp=True,
                )
