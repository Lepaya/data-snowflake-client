"""Config model."""

from pydantic import BaseModel, constr


class SnowflakeConfig(BaseModel):
    """Snowflake config data model."""

    account: constr(min_length=1)  # type: ignore
    username: constr(min_length=1)  # type: ignore
    private_key: constr(min_length=1)  # type: ignore


class ConfigModel(BaseModel):
    """Config data model."""

    snowflake: SnowflakeConfig
