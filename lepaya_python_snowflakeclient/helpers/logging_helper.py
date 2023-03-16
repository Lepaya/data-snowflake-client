"""Logging helper functions."""

from __future__ import annotations

import logging
import os

import structlog
from lepaya_python_slackclient.slack_client import SlackClient

LOGGER = structlog.get_logger()
PACKAGE_NAMES = ["requests", "urllib3", "snowflake.connector", "slack_sdk"]


def set_logger_level(log_level: int) -> None:
    """
    Set a logger level for the project.

        - Set the logging level for the project.
        - Skip unnecessary warnings.

    Args:
         log_level: logging level for the project.
    """
    for name in PACKAGE_NAMES:
        logging.getLogger(name).setLevel(log_level)

    # Skip unnecessary warnings
    os.environ["SKIP_SLACK_SDK_WARNING"] = "True"


def log_and_update_slack(slack_client: SlackClient, message: str, temp: bool) -> None:
    """
    Log a response and update slack.

    Args:
        slack_client: SlackClient object.
        message: plain text message.
        temp: True if the Slack message should be temporary.
    """
    LOGGER.info(message)
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
