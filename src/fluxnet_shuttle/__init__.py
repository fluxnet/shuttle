"""
:module: fluxnet_shuttle
:synopsis: Main library package for FLUXNET Shuttle operations with plugin system
:module author: Gilberto Pastorello <gzpastorello@lbl.gov>
:module author: Dario Papale <darpap@unitus.it>
:platform: Unix, Windows
:created: 2024-10-31
:updated: 2025-12-09

.. currentmodule:: fluxnet_shuttle


FLUXNET Shuttle Library provides functionality for discovering and downloading
FLUXNET data from multiple data hubs, including AmeriFlux and ICOS.

The library offers both synchronous and asynchronous Python APIs with a plugin-based
architecture for extending to new FLUXNET data hubs.

*Features*

* Plugin-based architecture for easy extensibility
* Both sync and async APIs using decorators to reduce duplication
* Error collection and isolation across plugins
* Unified configuration system
* Type-safe API with Pydantic models
* Comprehensive logging and error handling

*License*
See LICENSE file.

.. rubric:: Submodules
.. autosummary::
    :toctree: generated/

    core
    models
    plugins
    shuttle


------------------------------------------------------
"""

import logging
import sys
import traceback
import warnings
from typing import Any, Optional, Tuple

# get logger for this module
_log = logging.getLogger(__name__)

# 'no-op' handler in case no logging setup is done
_log.addHandler(logging.NullHandler())


# customize showwarning to get py.warnings to be logged instead of printed and
# to avoid new line characters in log
def format_warning(message: Any, category: Any, filename: Any, lineno: Any, file: Any = None, line: Any = None) -> None:
    logger_pywarnings = logging.getLogger("py.warnings")
    if not logger_pywarnings.handlers:
        logger_pywarnings.addHandler(logging.NullHandler())
    msg = warnings.formatwarning(message, category, filename, lineno, line).replace("\n", " ").replace("\r", " ")
    logger_pywarnings.warning(msg)


warnings.showwarning = format_warning

# logging default formats
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_FMT = "%(asctime)s.%(msecs)03d [%(levelname)-8s] %(message)s [%(name)s]"

# logging levels
LOG_LEVELS = {
    50: "CRITICAL",
    40: "ERROR",
    30: "WARNING",
    20: "INFO",
    10: "DEBUG",
    0: "NOTSET",
}


class FLUXNETShuttleError(Exception):
    """
    Base error/exception class for FLUXNET Shuttle
    """


def log_config(
    level: int = logging.DEBUG,
    filename: Optional[str] = None,
    filename_level: Optional[int] = None,
    std: bool = True,
    std_level: Optional[int] = None,
    log_fmt: str = LOG_FMT,
    log_datefmt: str = LOG_DATEFMT,
) -> None:
    """
    Setup root logger and handlers for log file and STDOUT

    :param level: logging level (from logging library)
    :type level: int
    :param filename: name of log file
    :type filename: str
    :param filename_level: logging level for file log (same as level if None)
    :type filename_level: int
    :param std: if True, sys.stderr will show log messages
    :type std: boolean
    :param std_level: logging level for std log (same as level if None)
    :type std_level: int
    :param log_fmt: log output formatting
    :type log_fmt: str
    :param log_datefmt: log date-time output formatting
    :type log_datefmt: str
    """

    # check and reset log levels
    reset_level = False
    if not isinstance(level, int):
        level = logging.DEBUG
        reset_level = True

    reset_filename_level = False
    if not isinstance(filename_level, int):
        if filename_level is not None:
            reset_filename_level = True
        filename_level = level

    reset_std_level = False
    if not isinstance(std_level, int):
        if std_level is not None:
            reset_std_level = True
        std_level = level

    # setup root logger
    logger_root = logging.getLogger()
    logger_root.setLevel(level)

    # setup formatter
    formatter = logging.Formatter(fmt=log_fmt, datefmt=log_datefmt)

    # setup file handler
    if filename is not None:
        handler_file = logging.FileHandler(filename)
        handler_file.setLevel(filename_level)
        handler_file.setFormatter(formatter)
        logger_root.addHandler(handler_file)

    # setup std handler
    if std:
        handler_console = logging.StreamHandler()
        handler_console.setLevel(std_level)
        handler_console.setFormatter(formatter)
        logger_root.addHandler(handler_console)

    # initialization message
    logger_root.info("Logging started")

    # registers results from housekeeping checks
    if reset_level:
        logger_root.warning("Invalid logging level, reset to DEBUG")
    if reset_filename_level:
        logger_root.warning("Invalid file logging level, reset to {l}".format(l=LOG_LEVELS.get(level, level)))
    if reset_std_level:
        logger_root.warning("Invalid std logging level, reset to {l}".format(l=LOG_LEVELS.get(level, level)))
    if filename is None:
        logger_root.info("No log file will be saved")
    if not std:
        logger_root.info("No log entries shown on console")


def add_file_log(
    filename: str, level: int = logging.DEBUG, log_fmt: str = LOG_FMT, log_datefmt: str = LOG_DATEFMT
) -> Tuple[logging.Logger, Optional[logging.FileHandler]]:
    """
    Setup root logger and handlers for log file and STDOUT

    :param filename: name of log file
    :type filename: str
    :param level: logging level (from logging library)
    :type level: int
    :param log_fmt: log output formatting
    :type log_fmt: str
    :param log_datefmt: log date-time output formatting
    :type log_datefmt: str
    :rtype: logging.FileHandler
    """

    # check and reset log levels
    reset_level = False
    if not isinstance(level, int):
        level = logging.DEBUG
        reset_level = True

    # setup logger logger
    logger_root = logging.getLogger()
    logger_root.setLevel(level)

    # setup formatter
    formatter = logging.Formatter(fmt=log_fmt, datefmt=log_datefmt)

    # setup file handler
    handler_file: Optional[logging.FileHandler] = None
    if filename is not None:
        handler_file = logging.FileHandler(filename)
        handler_file.setLevel(level)
        handler_file.setFormatter(formatter)
        logger_root.addHandler(handler_file)

    # initialization message
    logger_root.info("Pipeline logging started")

    # registers results from housekeeping checks
    if reset_level:
        logger_root.warning("Pipeline invalid logging level, reset to DEBUG")

    return logger_root, handler_file


def log_trace(exception: Exception, level: int = logging.ERROR, log: Any = _log, output_fmt: str = "std") -> str:
    """
    Logs exception including stack traceback into log,
    formatting trace as single line

    :param exception: exception object to be handled
    :type exception: Exception
    :param level: logging severity level
    :type level: int
    :param log: logger to use for logging trace
    :type log: logging.Logger
    :param output_fmt: output format: std (like Python traceback) or
                                      alt (';'-separated single line)
    :type output_fmt: str

    >>> # N.B.: careful when catching Exception class,
    >>> #       this can mask virtually any error in Python
    >>> try:
    >>>     raise Exception('Test exception')
    >>> except Exception as e:
    >>>     msg = log_trace(exception=e, level=logging.CRITICAL)
    >>>     sys.exit(msg)
    """

    # check logger parameter
    if not isinstance(log, logging.Logger):
        # get this function name
        func_name = sys._getframe().f_code.co_name
        msg = "{n} expected <class 'logging.Logger'> object, got {t} instead; " "using default".format(
            n=func_name, t=type(log)
        )
        log = _log
        log.error(msg)

    # protect trace retrieval
    message: str = ""
    try:
        # get exc_type, exc_value, exc_traceback
        _, _, exc_traceback = sys.exc_info()
        # format trace
        if output_fmt == "std":
            # use standard Python formatting (log list, return str)
            _message = traceback.format_exception(exception.__class__, exception, exc_traceback)
            log.log(level=level, msg=message)
            message = "".join(_message)
        elif output_fmt == "alt":
            trace = traceback.extract_tb(exc_traceback)
            message = "Trace for '{e}': ".format(e=str(exception))
            # go through all stack entries
            for t in trace:
                # items are: (filename, line number, function name, text)
                message += "{f}:{p}:{n} '{c}'; ".format(f=t[0], n=t[1], p=t[2], c=t[3])
            log.log(level=level, msg=message)

    # error while trying to retrieve/format trace
    except Exception as e:
        message = "Trace not generated for: '{x}'; ERROR: '{r}'".format(x=str(exception), r=str(e))
        log.error(message)

    return message


# Import the new plugin-based architecture
# Import plugins to ensure they're registered
from . import core, plugins  # noqa: F401
from .shuttle import download, listall  # noqa: F401

__all__ = [
    "plugins",
    "core",
    "download",
    "listall",
    "FLUXNETShuttleError",
    "log_config",
    "add_file_log",
    "log_trace",
]


from .main import main  # noqa: F401

__all__.append("main")
