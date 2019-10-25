# -*- coding: utf-8 -*-

""" Application-wide logger-factory module

This module contains a function that creates and customizes `logging.Logger`
objects for use across the entire-application.
"""

import sys
import logging
import logging.handlers

import colorlog


def create_logger(
    logger_name,
    logger_level="DEBUG",
    project_name="mp-ingester",
    do_log_stdout=True,
    do_log_syslog=True,
    do_color_logs=True,
):
    """ Creates and customizes a logger

    This method creates a new `logging.Logger` object with custom name, format,
    and handlers. Loggers created through this method are meant to be used
    across the entire application to provide consistent logging across different
    modules and classes.

    Other features include logging to standard-out, syslog, a very verbose
    format used in the handlers to aid in debugging, and colorful messages via
    the `colorlog` package.

    Note:
        Due to the `logging` design, loggers are uniquely identified by their
        name. As such, should this method be called with the name of an existing
        logger the only change that can be applied to that logger would be the
        logging-level.

    Args:
        logger_name (str): Uniquely identifying name of the logger.
        logger_level (str): Logging level as defined in the `logging` package.
        project_name (str): Name of the project under which the logger will be
            created. This will appear in the logging messages and helps when
            grep'ing for messages emitted by the given project.
        do_log_stdout (bool, optional): Whether to create a 'standard-out'
            logging handler. Defaults to `True`.
        do_log_syslog (bool, optional): Whether to create a 'syslog'
            logging handler. Defaults to `True`. Note that this feature is not
            available on OSX systems. Defaults to `True`.
        do_color_logs (bool, optional): Whether to emitted colorful log
            messages.

    Returns:
        logging.Logger: The created logger.
    """

    # Create the logger with the appropriate name.
    logger = logging.getLogger(name=logger_name)

    # Set logger's log-level.
    logger.setLevel(logger_level)

    # If the logger has already been configured (module reloads) then don't
    # configure it.
    if logger.handlers:
        return logger

    # Assemble the logging format.
    fmt_tmpl = (
        "{0}: %(process)d %(processName)s %(asctime)-15s "
        "%(levelname)-8s %(name)-10s %(funcName)s %(message)s"
    )
    fmt = fmt_tmpl.format(project_name)

    # Create a colourful formatter (should one be needed).
    formatter_w_color = colorlog.ColoredFormatter(
        fmt="%(log_color)s" + fmt,
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        reset=True,
        log_colors={
            "DEBUG": "blue",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
        secondary_log_colors={},
        style="%",
    )

    # Create a formatter without colours.
    formatter_wo_color = logging.Formatter(fmt=fmt)

    # Create an 'stdout' logging handler, set its output format, and add to the
    # logger (if enabled).
    if do_log_stdout:
        handler_stdout = logging.StreamHandler(sys.stdout)
        handler_stdout.setFormatter(
            formatter_w_color if do_color_logs else formatter_wo_color
        )
        logger.addHandler(handler_stdout)

    # Create a 'syslog' logging handler, set its output format, and add to the
    # logger (if enabled).
    if do_log_syslog and ("darwin" not in sys.platform):
        handler_syslog = logging.handlers.SysLogHandler(address="/dev/log")
        # Syslog does not like colours and displays the colour-codes in a mess
        # so we're using the colour-less formatter instead.
        handler_syslog.setFormatter(formatter_wo_color)
        logger.addHandler(handler_syslog)

    return logger
