"""Client to interact with Snowflake."""
from typing import Optional

import pandas
import snowflake.connector
import structlog
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError
from snowflake.connector.pandas_tools import write_pandas

from lepaya_python_slackclient.slack_client import SlackClient
from models.config_model import SnowflakeConfig

LOGGER = structlog.get_logger()


class SnowflakeClient:
    """Client used to interact with Snowflake."""

    def __init__(self, config: SnowflakeConfig, slack_client: SlackClient):
        """
        Initialize the Snowflake Client.

        Args:
            config: Pydantic Snowflake config model.
        """
        self.account = config.account
        self.username = config.username
        self.password = config.password
        self.connection: Optional[SnowflakeConnection] | None = None
        self.slack_client = slack_client

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

    def fetch_table_data(self, database: str, schema: str, table: str):
        """Fetch existing data from a table in Snowflake.

        Args:
            database: Name of the Snowflake database to fetch data.
            schema: Name of the Snowflake schema to fetch data.
            table: Name of the Snowflake table to fetch datao.
        Returns:
            Pandas DataFrame with existing table data.
            None, if the given table does not exist.
        """
        if self.connection is None:
            raise RuntimeError

        self.connection.cursor().execute(f"USE DATABASE {database}")
        self.connection.cursor().execute(f"USE SCHEMA {schema}")

        dataframe = None
        try:
            cursor = self.connection.cursor().execute(f'SELECT * FROM "{table}"')
            LOGGER.info(
                f"Fetching existing data from table : {table}. Database: {database}, schema: {schema}"
            )
            dataframe = cursor.fetch_pandas_all()  # type: ignore
        except ProgrammingError as e:
            fail_msg = (
                f"{table} does not exist in Database: {database}, schema: {schema}"
            )
            LOGGER.warning(fail_msg)
            raise ValueError(fail_msg) from e
        finally:
            return dataframe

    def load_dataframe(
        self,
        dataframe: pandas.DataFrame,
        database: str,
        schema: str,
        table: str,
        overwrite: bool,
    ):
        """Load a dataframe into a Snowflake table.

        If the table does not exist, a table is automatically created.
        Existing tables will be replaced with new tables.
        Args:
            dataframe: str : Pandas DataFrame with data to be loaded.
            database: str : Name of the Snowflake database to load data into.
            schema: str : Name of the Snowflake schema to load data into.
            table: str : Name of the Snowflake table to load data into.
            overwrite: bool : Overwrite existing table
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
        rows = 0
        chunks = 0
        try:
            success, chunks, rows, _ = write_pandas(
                conn=self.connection,
                table_name=table,
                df=dataframe,
                auto_create_table=True,
                overwrite=overwrite,
                quote_identifiers=False,
            )
        except (ValueError, ProgrammingError) as e:
            fail_msg = f"Failed to insert {dataframe.shape[0]} rows into table: {table}. Error : {e}"
            LOGGER.warning(fail_msg)
            raise ValueError(fail_msg)
        if success:
            success_msg = f"Successfully inserted {rows} rows in {chunks} chunks into table: {table}."
            LOGGER.info(success_msg)
            self.slack_client.add_message_block(
                message=f" :snowflake-db: {success_msg}", temp=True
            )
