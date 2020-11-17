"""
Helpers shared between our storage service scripts.
"""

from .secrets import read_secret, write_secret

__all__ = [
    "read_secret",
    "write_secret",
]
