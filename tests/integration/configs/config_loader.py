"""Config loader."""

import os
import re
from typing import Dict

import yaml
from pydantic import ValidationError
from structlog import get_logger

from data_snowflake_client.models.config_model import ConfigModel

LOGGER = get_logger()


def parse_config(path: str = None, data: str = None, tag: str = "!ENV"):
    """
    Load a yaml configuration file and resolve any environment variables.

    The environment variables must have !ENV before them and be in this format
    to be parsed: ${VAR_NAME}.

    Args:
        path: the path to the yaml file.
        data: the yaml data itself as a stream.
        tag: the tag to look for.

    Returns:
        The dict configuration.

    Raises:
        ValueError: No file path or data provided.

    """
    # pattern for global vars: look for ${word}
    pattern = re.compile(".*?\${(\w+)}.*?")  # noqa
    loader = yaml.SafeLoader

    # the tag will be used to mark where to start searching for the pattern
    # e.g. somekey: !ENV somestring${MYENVVAR}blah blah blah
    loader.add_implicit_resolver(tag, pattern, None)

    def constructor_env_variables(loader: yaml.Loader, node):
        """
        Extract the environment variable from the node's value.

        Args:
            loader: the yaml loader.
            node: the current node in the yaml.

        Returns:
            the parsed string that contains the value of the environment variable.

        """
        value = str(loader.construct_scalar(node))
        if match := pattern.findall(value):
            full_value = value
            for g in match:
                os_env = os.environ
                os_env_var = os_env[g]
                full_value = full_value.replace(f"${{{g}}}", os_env_var)
            return full_value
        return value

    loader.add_constructor(tag, constructor_env_variables)

    if path:
        with open(path) as config_data:
            return yaml.load(config_data, Loader=loader)
    elif data:
        return yaml.load(data, Loader=loader)
    else:
        raise ValueError("Either a path or data should be defined as input")


def load_config(file_path: str) -> ConfigModel:
    """Load a config YAML file into a Pydantic config model.

    Args:
        file_path: Path to configuration file.

    Returns:
        A Pydantic validated ConfigModel instance.

    Raises:
        ValidationError: Pydantic validation error.

    """
    LOGGER.info("Loading configuration file.")

    try:
        config: Dict = parse_config(path=file_path)
        return ConfigModel(**config)
    except ValidationError as e:
        LOGGER.warning("Unable to validate config file")
        raise e from e
