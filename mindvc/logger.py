"""Manages logging configuration for DVC repo."""

import logging.config
import logging.handlers

from .progress import Tqdm


def addLoggingLevel(levelName, levelNum, methodName=None):
    """
    Adds a new logging level to the `logging` module and the
    currently configured logging class.

    Uses the existing numeric levelNum if already defined.

    Based on https://stackoverflow.com/questions/2183233
    """
    if methodName is None:
        methodName = levelName.lower()

    # If the level name is already defined as a top-level `logging`
    # constant, then adopt the existing numeric level.
    if hasattr(logging, levelName):
        existingLevelNum = getattr(logging, levelName)
        assert isinstance(existingLevelNum, int)
        levelNum = existingLevelNum

    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            # pylint: disable=protected-access
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    # getLevelName resolves the numeric log level if already defined,
    # otherwise returns a string
    if not isinstance(logging.getLevelName(levelName), int):
        logging.addLevelName(levelNum, levelName)

    if not hasattr(logging, levelName):
        setattr(logging, levelName, levelNum)

    if not hasattr(logging.getLoggerClass(), methodName):
        setattr(logging.getLoggerClass(), methodName, logForLevel)

    if not hasattr(logging, methodName):
        setattr(logging, methodName, logToRoot)


class LoggingException(Exception):
    def __init__(self, record):
        msg = f"failed to log {str(record)}"
        super().__init__(msg)


def excludeFilter(level):
    class ExcludeLevelFilter(logging.Filter):
        def filter(self, record):
            return record.levelno < level

    return ExcludeLevelFilter


class LoggerHandler(logging.StreamHandler):
    def handleError(self, record):
        super().handleError(record)
        raise LoggingException(record)

    def emit_pretty_exception(self, exc, verbose: bool = False):
        return exc.__pretty_exc__(verbose=verbose)

    def emit(self, record):
        """Write to Tqdm's stream so as to not break progress-bars"""
        try:
            if record.exc_info:
                _, exc, *_ = record.exc_info
                if hasattr(exc, "__pretty_exc__"):
                    try:
                        self.emit_pretty_exception(exc, verbose=_is_verbose())
                        if not _is_verbose():
                            return
                    except Exception:  # noqa, pylint: disable=broad-except
                        pass

            msg = self.format(record)
            Tqdm.write(
                msg, file=self.stream, end=getattr(self, "terminator", "\n")
            )
            self.flush()
        except (BrokenPipeError, RecursionError):
            raise
        except Exception:  # noqa, pylint: disable=broad-except
            self.handleError(record)


def _is_verbose():
    return (
        logging.NOTSET
        < logging.getLogger("mindvc").getEffectiveLevel()
        <= logging.DEBUG
    )


def disable_other_loggers():
    logging.captureWarnings(True)
    loggerDict = logging.root.manager.loggerDict  # pylint: disable=no-member
    for logger_name, logger in loggerDict.items():
        if logger_name != "mindvc" and not logger_name.startswith("mindvc."):
            logger.disabled = True


def set_loggers_level(level: int = logging.INFO) -> None:
    for name in ["mindvc", "mindvc.objects", "mindvc.data"]:
        logging.getLogger(name).setLevel(level)


def setup(level: int = logging.INFO) -> None:
    if level >= logging.DEBUG:
        # Unclosed session errors for asyncio/aiohttp are only available
        # on the tracing mode for extensive debug purposes. They are really
        # noisy, and this is potentially somewhere in the client library
        # not managing their own session. Even though it is the best practice
        # for them to do so, we can be assured that these errors raised when
        # the object is getting deallocated, so no need to take any extensive
        # action.
        logging.getLogger("asyncio").setLevel(logging.CRITICAL)
        logging.getLogger("aiohttp").setLevel(logging.CRITICAL)

    addLoggingLevel("TRACE", logging.DEBUG - 5)
    logging.config.dictConfig(
        {
            "version": 1,
            "filters": {
                "exclude_errors": {"()": excludeFilter(logging.WARNING)},
                "exclude_info": {"()": excludeFilter(logging.INFO)},
                "exclude_debug": {"()": excludeFilter(logging.DEBUG)},
            },
            "handlers": {
                "console_info": {
                    "class": "mindvc.logger.LoggerHandler",
                    "level": "INFO",
                    "stream": "ext://sys.stdout",
                    "filters": ["exclude_errors"],
                },
                "console_debug": {
                    "class": "mindvc.logger.LoggerHandler",
                    "level": "DEBUG",
                    "stream": "ext://sys.stdout",
                    "filters": ["exclude_info"],
                },
                "console_trace": {
                    "class": "mindvc.logger.LoggerHandler",
                    "level": "TRACE",
                    "stream": "ext://sys.stdout",
                    "filters": ["exclude_debug"],
                },
                "console_errors": {
                    "class": "mindvc.logger.LoggerHandler",
                    "level": "WARNING",
                    "stream": "ext://sys.stderr",
                },
            },
            "loggers": {
                "mindvc": {
                    "level": level,
                    "handlers": [
                        "console_info",
                        "console_debug",
                        "console_trace",
                        "console_errors",
                    ],
                },
            },
            "disable_existing_loggers": False,
        }
    )
