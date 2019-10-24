# -*- coding: utf-8 -*-

""" Application-wide configuration module

This module contains functions to load a JSON configuration file and validate it
against a JSON schema hardcoded within the module under `config_schema_default`.

Attributes:
    config_schema_default (dict): The JSON configuration schema defining the
    structure and content of a valid JSON configuration file.
"""

from __future__ import unicode_literals

import os

import ujson
import validictory
import attrdict

from mp_ingester import excs

# JSON schema for the incoming configuration.
config_schema_default = {
    "type": "object",
    "required": [
        # General Settings.
        "logger_level",
        # SQL Server Configuration Settings.
        "sql_host",
        "sql_port",
        "sql_username",
        "sql_password",
        "sql_db",
    ],
    "properties": {
        "logger_level": {
            "type": "string",
            "description": (
                "The minimum level of `logging` messages that will "
                "be emitted"
            ),
            "enum": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        },
        # SQL Server Configuration Settings.
        "sql_host": {"type": "string", "description": "SQL server host."},
        "sql_port": {"type": "integer", "description": "SQL server port."},
        "sql_username": {
            "type": "string",
            "description": "SQL server username.",
        },
        "sql_password": {
            "type": "string",
            "description": "SQL server password.",
        },
        "sql_db": {
            "type": "string",
            "description": "SQL server database name.",
        },
    },
}


def load_config_file(fname_config_file):
    """ Loads a JSON configuration file and returns its contents as a dict.

    Args:
        fname_config_file (str, unicode): The path to the JSON configuration
            file.

    Returns:
        dict: The loaded configuration dictionary.
    """

    # Ensure the provided path is a valid existing file.
    if not os.path.exists(fname_config_file) or not os.path.isfile(
        fname_config_file
    ):
        msg = "Config file '{0}' not found or not a file."
        msg_fmt = msg.format(fname_config_file)
        raise excs.ConfigFileNotFound(msg_fmt)

    # Read the JSON file.
    with open(fname_config_file, str("r")) as finp:
        config = ujson.load(finp)

    return config


def validate_config(config_instance, config_schema=None):
    """ Validates a configuration dict against a JSON schema.

    Args:
        config_instance (str, unicode): The configuration dictionary instance.
        config_schema (dict, optional): The configuration JSON schema in the
            form of a `dict`. Defaults to `None` in which case the
            `config_schema_default` is used.

    Returns:
        bool: `True` if `config_instance` validates against the schema.
    """

    # Use `config_schema_default` if no schema was provided.
    if config_schema is None:
        config_schema = config_schema_default

    # Perform the validation of the provided configuration against the schema
    # and raise an exception if it fails.
    try:
        # Perform the validation.
        validictory.validate(
            data=config_instance,
            schema=config_schema,
            required_by_default=False,
            blank_by_default=True,
        )
    # catch any exception resulting from the validation and wrap it in the
    # custom `excs.ConfigFileInvalid` exception.
    except Exception as exc:
        raise excs.ConfigFileInvalid(exc.message)

    return True


def import_config(fname_config_file):
    """ Loads and validates a JSON configuration file.

    This method uses the `load_config_file` and `validate_config` functions to
    load a JSON configuration file and validate against the
    `config_schema_default` returning the validates configuration as an
    `attrdict.AttrDict`.

    Args:
        fname_config_file (str, unicode): The path to the JSON configuration
            file.

    Returns:
        attrdict.AttrDict: The imported configuration `AttrDict`.
    """

    # Load the JSON configuration file.
    config = load_config_file(fname_config_file=fname_config_file)

    # Validate the loaded `dict` against the `config_schema_default` JSON
    # schema.
    validate_config(config_instance=config, config_schema=config_schema_default)

    return attrdict.AttrDict(config)
