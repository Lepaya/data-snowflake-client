# Lepaya Snowflake Client
The Lepaya Snowflake Client is a Python library that allows you to interact with the Snowflake API and load/extract data from SnowflakeDB. This library was developed by Humaid Mollah for Lepaya.

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
    database_name = 'NAME OF DATABASE TO LOAD DATA INTO',
    schema_name = 'NAME OF SCHEMA TO LOAD DATA INTO',
)
````

````
with SnowflakeClient(config=configs.snowflake, slack_client=slack) as snowflake:
    #code here
````

### Functions

The following methods are available for use:
- ``__init__(self, config: SnowflakeConfig, slack_client: SlackClient)``: Initializes the Snowflake client with the given configuration and a Slack client.

- ``__enter__(self)``: Creates a Snowflake client in a context manager.

- ``__exit__(self, exc_type, exc_val, exc_tb)``: Closes the connection to Snowflake and exits the context.

- ``fetch_table_data(self, database: str, schema: str, table: str)``: Fetches existing data from a table in Snowflake and returns a Pandas DataFrame. If the table does not exist, returns None.

- ``load_dataframe(self, dataframe: pandas.DataFrame, database: str, schema: str, table: str, overwrite: bool)``: Loads a Pandas DataFrame into a Snowflake table. If the table does not exist, it will be created. Existing tables will be replaced with new tables. Returns None. If the client has no active connection, raises a RuntimeError.



