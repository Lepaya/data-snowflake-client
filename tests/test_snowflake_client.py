import unittest
import snowflake.connector
import pandas as pd

from data_snowflake_client.models.config_model import SnowflakeConfig
from data_snowflake_client.snowflake_client import SnowflakeClient


class TestSnowflakeClient(unittest.TestCase):
    def setUp(self):

        self.config = SnowflakeConfig(
            account="",
            password="",
            username=""
        )
        self.database = "python_dev"
        self.schema = "test_schema"
        self.table = "test_table"

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


if __name__ == '__main__':
    unittest.main()
