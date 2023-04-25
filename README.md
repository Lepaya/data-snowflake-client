# Lepaya Snowflake Client v1.1.2
The Lepaya Snowflake Client is a Python library that allows you to interact with the Snowflake API and load/extract data and run queries on SnowflakeDB. This library was developed by Humaid Mollah for Lepaya.

This client logs events internally using structlog = "~=22.3" and Lepaya's Python SlackClient - https://github.com/Lepaya/lepaya-python-slackclient release-1.2.

## Installation
To install the project dependencies, use the following commands:
1. Clone the repository 
2. Navigate to the directory of the cloned repository using the cd command.
3. ``pipenv sync`` (to download all dependencies from the Pipfile)
4. ``pipenv shell`` (to create a new virtual environment and activate it)

## Usage

### Initialization

from lepaya_python_snowflakeclient.models.config_model import SnowflakeConfig
from lepaya_python_snowflakeclient.snowflake_client import SnowflakeClient

````
class SnowflakeConfig(

    account = 'YOUR SNOWFLAKE ACCOUNT : <AccountID.RegionName>',
    username = 'YOUR SNWOFLAKE USERNAME',
    password: 'YOUR SNOWFLAKE PASSWORD',
    to_database_name = 'NAME OF DATABASE TO LOAD DATA INTO',
    to_schema_name = 'NAME OF SCHEMA TO LOAD DATA INTO',
    to_table_name = 'NAME OF TABLE TO LOAD DATA INTO',
)
````

````
with SnowflakeClient(config=configs.snowflake, slack_client=slack) as snowflake:
    #code here
````

### Functions

The following methods are available for use:
- ``__init__(self, config: SnowflakeConfig, slack_client: SlackClient)``: Initializes the SnowflakeClient object with a configuration object and a Slack client object.
- ``__enter__(self) -> SnowflakeClient``: Establishes a connection to Snowflake within a context manager.
- ``__exit__(self, exc_type, exc_val, exc_tb)`` -> None: Closes the Snowflake connection and exits the context.
- ``fetch_table_data(self, database: str, schema: str, table: str) -> pd.DataFrame``: Fetches data from a table in Snowflake and returns a Pandas DataFrame.
- ``load_dataframe(self, dataframe: pd.DataFrame, database: str, schema: str, table: str, overwrite: bool) -> None``: Loads a Pandas DataFrame into a Snowflake table. If the table does not exist, a new table is created, and existing tables are replaced with new tables.
- ``run_query(self, query: str, table: str, schema: str, database: str) -> None:``: Run an SQL query on a given table in Snowflake.
