from .datafunctions import datafunction, ArgumentError, ReturnError

try:
    from .version import __version__
except ImportError:  # pragma: no cover
    # version.py is auto-generated with the git tag when building
    __version__ = "???"
