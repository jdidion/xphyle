# -*- coding: utf-8 -*-
"""Type checking support. Defines commonly used types.
"""
# pylint: disable=wildcard-import, unused-wildcard-import, import-error
import typing.io
from typing import *
from typing.re import *

# pylint: disable=invalid-name
FileLike = typing.io.IO
"""File-like object (alias for typing.io.IO)"""

PathOrFile = Union[str, FileLike]
"""Either a string or FileLike"""

Url = Tuple[str, str, str, str, str, str]
"""URL tuple (result of urllib.parse.urlparse)"""

Range = Tuple[int, int]
"""Two-integer tuple representing a range"""

FilesArg = Iterable[Union[str, Tuple[Any, PathOrFile]]]
"""Multiple files: an iterable over either strings or (key, PathOrFile)"""

CharMode = TypeVar('CharMode', str, bytes)
"""Type representing a file encoding mode (text or binary)"""

TextMode = 't'
"""Text mode"""

BinMode = b'b'
"""Binary mode"""
