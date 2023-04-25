"""Logging helper functions."""

from __future__ import annotations

import structlog
from data_slack_client.slack_client import SlackClient

LOGGER = structlog.get_logger()
PACKAGE_NAMES = ["requests", "urllib3", "snowflake.connector", "slack_sdk"]


def log_and_update_slack(slack_client: SlackClient, message: str, temp: bool) -> None:
    """
    Log a response and update slack.

    Args:
        slack_client: SlackClient object.
        message: plain text message.
        temp: True if the Slack message should be temporary.
    """
    LOGGER.info(message)
    if slack_client is not None:
        slack_client.add_message_block(message=message, temp=temp)


def log_and_raise_error(message: str):
    """Log a response and raise an error.

    Args:
         message: plain error text message.

    Raises:
        ValueError: Reraise with the error message.
    """
    LOGGER.info(message)
    raise ValueError(message)
