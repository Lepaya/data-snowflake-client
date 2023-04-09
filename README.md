# Lepaya Snowflake Client v1.0
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
    table_name = 'NAME OF TABLE TO LOAD DATA INTO',
)
````

````
with SnowflakeClient(config=configs.snowflake, slack_client=slack) as snowflake:
    #code here
````

### Functions

The following methods are available for use:
- ``__init__(self, config: SnowflakeConfig)``: Initializes the SnowflakeClient object with a Pydantic Snowflake config model. Sets the Snowflake account, username, and password.

- ``__enter__(self)``: Creates a Snowflake client in a context manager. Connects to the Snowflake account using the account, username, and password.

- ``__exit__(self, exc_type, exc_val, exc_tb)``: Closes the connection to Snowflake and exits context.

- ``load_dataframe(self, dataframe: pandas.DataFrame, database: str, schema: str, table: str)`` -> str: Loads a Pandas DataFrame into a Snowflake table. If the table does not exist, a table is automatically created. Takes in the dataframe, database name, schema name, and table name as arguments. Returns a success or failure message depending on whether the load was successful. If the SnowflakeClient object has no active connection, a RuntimeError is raised.
