import unittest
from pathlib import Path

import pandas as pd
import snowflake.connector

from data_snowflake_client.snowflake_client import SnowflakeClient
from tests.integration.configs.config_loader import load_config

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
CONFIG_FILE_PATH = f"{PROJECT_ROOT}/tests/integration/configs/config.yml"


class TestSnowflakeClient(unittest.TestCase):

    @classmethod
    def setUp(cls):
        configs = load_config(CONFIG_FILE_PATH)
        cls.config = configs.snowflake
        cls.database = "python_dev"
        cls.schema = "test_schema"
        cls.table = "test_table"

    def test_connection(self):
        # Test Snowflake connection
        try:
            with SnowflakeClient(self.config) as client:
                self.assertIsInstance(client, SnowflakeClient)
        except snowflake.connector.errors.DatabaseError as e:
            self.fail(f"Failed to connect to Snowflake: {e}")

    def test_fetch_table_data(self):

        # Assuming the table exists in Snowflake with test data
        with SnowflakeClient(self.config) as client:
            dataframe = client.fetch_table_data(self.database, self.schema, self.table)

        # Assertions
        self.assertIsInstance(dataframe, pd.DataFrame)
        self.assertTrue(dataframe.shape[0] > 0)

    def test_load_dataframe(self):
        dataframe = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})

        with SnowflakeClient(self.config) as client:
            client.load_dataframe(dataframe, self.database, self.schema, self.table, overwrite=True)

    def test_run_query(self):

        query = f"SELECT * FROM {self.table}"

        # Run a query on Snowflake
        with SnowflakeClient(self.config) as client:
            result_dataframe = client.run_query(query, self.table, self.schema, self.database)

        # Assertions
        self.assertIsInstance(result_dataframe, pd.DataFrame)
        self.assertTrue(result_dataframe.shape[0] >= 0)

    def test_invalid_location(self):

        invalid_table = "Invalid"
        with self.assertRaises(ValueError):
            with SnowflakeClient(self.config) as client:
                client.fetch_table_data(self.database, self.schema, invalid_table)


if __name__ == '__main__':
    unittest.main()
