# -*- coding: utf-8 -*-

""" Custom application-wide exception classes.

This module contains custom exception classes that can be used to wrap other
exception and allow for consistent error-handling across the application.
"""


class ConfigFileNotFound(Exception):
    """Exception raised when a JSON configuration file is missing."""

    def __init__(self, message, *args):
        super(ConfigFileNotFound, self).__init__(message, *args)


class ConfigFileInvalid(Exception):
    """Exception raised when a JSON configuration file is invalid."""

    def __init__(self, message, *args):
        super(ConfigFileInvalid, self).__init__(message, *args)
