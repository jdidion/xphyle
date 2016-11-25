# -*- coding: utf-8 -*-
"""Type checking support. Defines commonly used types.
"""
# pylint: disable=wildcard-import, unused-wildcard-import
import typing.io
from typing import *
from typing.re import Pattern

# pylint: disable=invalid-name
FileLike = typing.io.IO
PathOrFile = Union[str, FileLike]
Url = Tuple[str, str, str, str, str, str]
Range = Tuple[int, int]
