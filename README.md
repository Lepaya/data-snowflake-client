# Lepaya Snowflake Client
The Lepaya Snowflake Client is a Python library that allows you to interact with the Snowflake API and load/extract data and run queries on SnowflakeDB. This library was developed by Humaid Mollah for Lepaya.

This client logs events internally using structlog = "~=22.3" and (optionally) Lepaya's Python SlackClient - https://github.com/Lepaya/lepaya-python-slackclient

## Installation
To install this python package, run the following command:
``pipenv install -e "git+https://github.com/Lepaya/data-snowflake-client@release-1#egg=data-slack-client``

## Usage

### Initialization

from lepaya_python_snowflakeclient.models.config_model import SnowflakeConfig
from lepaya_python_snowflakeclient.snowflake_client import SnowflakeClient

````
class SnowflakeConfig(
    account = 'YOUR SNOWFLAKE ACCOUNT : <AccountID.RegionName>',
    username = 'YOUR SNWOFLAKE USERNAME',
    private_key: 'YOUR BASE64 ENCODED SNOWFLAKE RSA PRIVATE KEY',
)
````

````
with SnowflakeClient(config = SnowflakeConfig) as client:
    dataframe = client.fetch_table_data(database="ingest", schema="git", table="deployments")
````

### Functions

The following methods are available for use:
- ``__init__(self, config: SnowflakeConfig, slack_client: SlackClient)``: Initializes the SnowflakeClient object with a configuration object and a Slack client object.
- ``__enter__(self) -> SnowflakeClient``: Establishes a connection to Snowflake within a context manager.
- ``__exit__(self, exc_type, exc_val, exc_tb)`` -> None: Closes the Snowflake connection and exits the context.
- ``fetch_table_data(self, database: str, schema: str, table: str, warehouse: str | None = None,
                         role: str | None = None) -> pd.DataFrame:``: Fetch data from a Snowflake Table and return the result as pandas dataframe.
- ``load_dataframe(
            self,
            dataframe: pd.DataFrame,
            database: str,
            schema: str,
            table: str,
            overwrite: bool,
            warehouse: str | None = None,
            role: str | None = None,
            quote_identifiers: bool = False
    ) -> success, chunks, rows, output``: Loads a Pandas DataFrame into a Snowflake table. If the table does not exist, a new table is created, and existing tables are replaced with new tables.
- ``run_query(self, query: str, table: str, schema: str, database: str, warehouse: str | None = None,
                  role: str | None = None) -> pd.DataFrame``: Run an SQL query on a given table in Snowflake and get the result of a query as a pandas dataframe.
