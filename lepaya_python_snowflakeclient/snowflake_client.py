"""Client to interact with Snowflake."""
from typing import Optional

import pandas
import snowflake.connector
import structlog
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas

from models.config_model import SnowflakeConfig

LOGGER = structlog.get_logger()


class SnowflakeClient:
    """Client used to interact with Snowflake."""

    def __init__(self, config: SnowflakeConfig):
        """
        Initialize the Snowflake Client.

        Args:
            config: Pydantic Snowflake config model.
        """
        self.account = config.account
        self.username = config.username
        self.password = config.password

        self.connection: Optional[SnowflakeConnection] = None

    def __enter__(self):
        """
        Create a Snowflake Client in a context manager.

        Returns:
            The initialized Snowflake client.
        """
        self.connection = snowflake.connector.connect(
            account=self.account, user=self.username, password=self.password
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the connection to Snowflake and exits context.

        Args:
            exc_type: N/a
            exc_val: N/a
            exc_tb: N/a
        """
        self.connection.close()

    def load_dataframe(
        self, dataframe: pandas.DataFrame, database: str, schema: str, table: str
    ) -> str:
        """
        Load a dataframe into a Snowflake table.
        If the table does not exist, a table is automatically created.

        Args:
            dataframe: Pandas DataFrame with data to be loaded.
            database: Name of the Snowflake database to load data into.
            schema: Name of the Snowflake schema to load data into.
            table: Name of the Snowflake table to load data into.
        Returns:
            None.
        Raises:
            RuntimeError: the client has no active connection.
        """
        if self.connection is None:
            raise RuntimeError

        self.connection.cursor().execute(f"USE DATABASE {database}")
        self.connection.cursor().execute(f"USE SCHEMA {schema}")

        LOGGER.info(
            f"Loading data into SnowflakeDB. Database: {database}, schema: {schema}"
        )

        success, chunks, rows, _ = write_pandas(
            conn=self.connection, table_name=table, df=dataframe, auto_create_table=True
        )

        if success:
            success_msg = f"Successfully inserted {rows} rows in {chunks} chunks into table: {table}."
            LOGGER.info(success_msg)
            return success_msg

        fail_msg = f"Failed to insert {rows} rows into table: {table}."
        LOGGER.warning(fail_msg)
        return fail_msg
