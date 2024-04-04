import unittest
from unittest.mock import MagicMock, patch

from data_snowflake_client.models.config_model import SnowflakeConfig
from data_snowflake_client.snowflake_client import SnowflakeClient


class TestSnowflakeClient(unittest.TestCase):

    @classmethod
    def setUp(cls):
        # Patch the Snowflake connector and pandas
        cls.patcher = patch('data_snowflake_client.snowflake_client.snowflake.connector.connect')
        cls.mock_snowflake_connector = cls.patcher.start()

        # Create instance of SnowflakeClient with mocked dependencies
        cls.config = SnowflakeConfig(account='your_account', username='your_username', password='your_password')
        cls.snowflake_client = SnowflakeClient(config=cls.config, slack_client=None)

    @classmethod
    def tearDown(cls):
        cls.patcher.stop()

    def test_enter_and_exit(self):
        """Test __enter__ and __exit__ context manager functionality."""
        # Mock successful connection
        self.mock_snowflake_connector.return_value = MagicMock()

        # Use SnowflakeClient in a context manager
        with self.snowflake_client as client:
            self.assertIsNotNone(client.connection)

        # Assert connection was closed on exit
        client.connection.close.assert_called_once()
