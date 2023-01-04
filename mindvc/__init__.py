"""
DVC
----
Make your data science projects reproducible and shareable.
"""
from . import logger
from .version import __version__, version_tuple  # noqa: F401

logger.setup()
