"""
:module: fluxnet_shuttle_lib
:synopsis: Main library package for FLUXNET Shuttle operations with plugin system
:module author: Gilberto Pastorello <gzpastorello@lbl.gov>
:module author: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2024-10-31
:updated: 2025-01-09

.. currentmodule:: fluxnet_shuttle_lib

.. autosummary::
   :toctree: generated/

   add_file_log
   log_config
   log_trace
   download
   listall
   core
   plugins
   shuttle

   FLUXNETShuttleError

FLUXNET Shuttle Library provides functionality for discovering, downloading, and cataloging
FLUXNET data from multiple networks including AmeriFlux, ICOS, and FLUXNET2015.

The library offers both synchronous and asynchronous Python APIs with a plugin-based
architecture for extending to new FLUXNET networks.

*Features*

* Plugin-based architecture for easy extensibility
* Both sync and async APIs using decorators to reduce duplication
* Error collection and isolation across plugins
* Unified configuration system
* Type-safe API with Pydantic models
* Comprehensive logging and error handling


*License*

Copyright (c) 2023-2025, The Regents of the University of California,
through Lawrence Berkeley National Laboratory (subject to receipt
of any required approvals from the U.S. Dept. of Energy).
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
(1) Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer. (2) Redistributions
in binary form must reproduce the above copyright notice, this list of
conditions and the following disclaimer in the documentation and/or other
materials provided with the distribution. (3) Neither the name of the
University of California, Lawrence Berkeley National Laboratory, U.S.
Dept. of Energy nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

You are under no obligation whatsoever to provide any bug fixes, patches,
or upgrades to the features, functionality or performance of the source
code ("Enhancements") to anyone; however, if you choose to make your
Enhancements available either publicly, or directly to Lawrence Berkeley
National Laboratory, without imposing a separate written license agreement
for such Enhancements, then you hereby grant the following license:
a non-exclusive, royalty-free perpetual license to install, use, modify,
prepare derivative works, incorporate into other computer software,
distribute, and sublicense such enhancements or derivative works thereof,
in binary and source code form.


*Version History*

.. versionadded:: 0.1.0
   Initial release with AmeriFlux and ICOS support.

------------------------------------------------------
"""

import logging
import sys
import traceback
import warnings
from typing import Optional, Tuple

VERSION = "0.1.0"

__author__ = "Gilberto Pastorello"
__copyright__ = (
    "Copyright 2023-2024, The Regents of the University of California, " "through Lawrence Berkeley National Laboratory"
)
__credits__ = [
    "Gilberto Pastorello <gzpastorello@lbl.gov>",
    "Dario Papale <darpap@unitus.it>",
]
__maintainer__ = "Gilberto Pastorello"
__email__ = "gzpastorello@lbl.gov"
__institution__ = "Lawrence Berkeley National Laboratory (www.lbl.gov)"
__license__ = "BSD"
__status__ = "Development"
__version__ = VERSION

# get logger for this module
_log = logging.getLogger(__name__)

# 'no-op' handler in case no logging setup is done
_log.addHandler(logging.NullHandler())


# customize showwarning to get py.warnings to be logged instead of printed and
# to avoid new line characters in log
def format_warning(message, category, filename, lineno, file=None, line=None):
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
    level=logging.DEBUG,
    filename=None,
    filename_level=None,
    std=True,
    std_level=None,
    log_fmt=LOG_FMT,
    log_datefmt=LOG_DATEFMT,
):
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
    filename, level=logging.DEBUG, log_fmt=LOG_FMT, log_datefmt=LOG_DATEFMT
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


def log_trace(exception, level=logging.ERROR, log=_log, output_fmt="std") -> str:
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
