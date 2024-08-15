import random
import string
import unittest
from pathlib import Path

import numpy as np
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
        dataframe = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"], "col3": ["C", "D"], "num": [1, 2]})

        with SnowflakeClient(self.config) as client:
            client.load_dataframe(dataframe, self.database, self.schema, self.table, overwrite=True)

    def test_load_dataframe_additional_column(self):
        dataframe = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"], "random_col": [3, 4]})

        letters = string.ascii_lowercase
        random_column_name = "".join(random.choice(letters) for _ in range(5))
        random_values = np.random.randint(1, 10, size=len(dataframe))
        dataframe[random_column_name] = random_values

        with SnowflakeClient(self.config) as client:
            client.load_dataframe(
                dataframe,
                self.database,
                self.schema,
                f"{self.table}_add_column",
                overwrite=False,
            )

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

    def test_table_exists(self):

        with SnowflakeClient(self.config) as client:
            existing_table = client.check_if_table_exists(self.database, self.schema, self.table)
            self.assertTrue(existing_table)
            non_existing_table = client.check_if_table_exists(self.database, self.schema, "non_existing_table")
            self.assertFalse(non_existing_table)


if __name__ == "__main__":
    unittest.main()
