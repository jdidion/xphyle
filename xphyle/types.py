# -*- coding: utf-8 -*-
"""Type checking support. Defines commonly used types.
"""
# pylint: disable=wildcard-import, unused-wildcard-import, import-error, invalid-name
import collections
from enum import Enum
import os
import stat
import sys
import typing.io
from typing import *
from typing.re import *

def is_iterable(obj: Any, include_str: bool = False) -> bool:
    """Test whether an object is iterable.
    
    Args:
        x: The object to test.
        include_str: Whether a string should be considered an iterable
            (default: False).
    
    Returns:
        True if the object is iterable.
    """
    return (isinstance(obj, collections.Iterable) and
            (include_str or not isinstance(obj, str)))

class ModeAccess(Enum):
    """Enumeration of the access modes allowed when opening files.
    
    See Also:
        https://docs.python.org/3/library/functions.html#open
    """
    READ = 'r'
    """Read from file."""
    WRITE = 'w'
    """Write to file, overwriting any existing file."""
    READWRITE = 'r+'
    """Open file for reading and writing."""
    TRUNCATE_READWRITE = 'w+'
    """Open file for reading and writing, first truncating the file to 0."""
    APPEND = 'a'
    """Create file if it doesn't exist, else append to existing file."""
    EXCLUSIVE = 'x'
    """Exclusive write (fails if file already exists)."""
    
    @property
    def readable(self):
        """Whether this is readable mode.
        """
        return any(char in self.value for char in ('r', '+'))
    
    @property
    def writable(self):
        """Whether this is writable mode.
        """
        return any(char in self.value for char in ('w', '+', 'a', 'x'))

ModeAccessArg = Union[str, ModeAccess]

class ModeCoding(Enum):
    """Enumeration of file open modes (text or binary).
    
    See Also:
        https://docs.python.org/3/library/functions.html#open
    """
    TEXT = 't'
    """Text mode."""
    BINARY = 'b'
    """Binary mode."""

ModeCodingArg = Union[str, ModeCoding]

class FileMode(object):
    """Definition of a file mode as composed of a :class:`ModeAccess` and a
    :class:`ModeCoding`.
    
    Args:
        mode: Specify the mode as a string; mutually exclusive with `access`
            and `coding`.
        access: The file access mode (default: :attribute:`ModeAccess.READ`).
        coding: The file open mode (default: :attribute:`ModeCoding.TEXT`).
    """
    def __init__(
            self, mode: str = None, access: ModeAccessArg = None,
            coding: ModeCodingArg = None):
        if mode:
            for a in ModeAccess:
                if a.value in mode:
                    access = a
                    break
            for e in ModeCoding:
                if e.value in mode:
                    coding = e
                    break
        else:
            if isinstance(access, str):
                access = ModeAccess(access)
            if isinstance(coding, str):
                coding = ModeCoding(coding)
        
        self.access = access or ModeAccess.READ
        self.coding = coding or ModeCoding.TEXT
        self.value = '{}{}'.format(self.access.value, self.coding.value)
        
        if mode:
            diff = set(mode) - set(str(self) + 'U')
            if diff:
                raise ValueError("Invalid characters in mode string: {}".format(
                    ''.join(diff)))
    
    @property
    def readable(self):
        """Whether this is readable mode.
        """
        return self.access.readable
    
    @property
    def writable(self):
        """Whether this is writable mode.
        """
        return self.access.writable
    
    @property
    def binary(self):
        """Whether this is binary mode.
        """
        return self.coding == ModeCoding.BINARY
    
    @property
    def text(self):
        """Whether this is text mode.
        """
        return self.coding == ModeCoding.TEXT
    
    def __contains__(self, value: Union[str, ModeAccess, ModeCoding]) -> bool:
        if isinstance(value, ModeAccess):
            return self.access == value
        elif isinstance(value, ModeCoding):
            return self.coding == value
        else:
            for v in value:
                if v not in self.access.value and v not in self.coding.value:
                    return False
            return True
    
    def __repr__(self):
        return self.value

OS_ALIASES = dict(
    r=os.R_OK,
    w=os.W_OK,
    x=os.X_OK,
    t=0
)
"""Dictionary mapping mode characters to :module:`os` flags"""

STAT_ALIASES = dict(
    r=stat.S_IREAD,
    w=stat.S_IWRITE,
    x=stat.S_IEXEC,
    t=stat.S_ISVTX,
    f=stat.S_IFREG,
    d=stat.S_IFDIR,
    fifo=stat.S_IFIFO
)
"""Dictionary mapping mode characters to :module:`stat` flags"""

class Permission(Enum):
    """Enumeration of file permission flags ('r', 'w', 'x', 't'). Note that
    this isn't a full enumeration of all flags, just those pertaining to the
    permissions of the current user.
    """
    READ = 'r'
    """Read; alias of :attribute:`stat.S_IREAD` and :attribute:`os.R_OK`."""
    WRITE = 'w'
    """Write; alias of :attribute:`stat.S_IWRITE and :attribute:`os.W_OK``."""
    EXECUTE = 'x'
    """Execute; alias of :attribute:`stat.S_IEXEC` and :attribute:`os.X_OK`."""
    STICKY = 't'
    """The sticky bit, alias of :attribute:`stat.S_ISVTX`."""
    
    @property
    def stat_flag(self):
        """Returns the :module:`stat` flag.
        """
        return STAT_ALIASES[self.value]
    
    @property
    def os_flag(self):
        """Returns the :module:`os` flag.
        """
        return OS_ALIASES[self.value]

PermissionArg = Union[str, int, Permission, ModeAccess]
"""Types from which an Permission can be inferred."""

class PermissionSet(object):
    """A set of :class:`Permission`s.
    
    Args:
        flags: Sequence of flags as string ('r', 'w', 'x'), int,
            :class:`ModeAccess`, or :class:`Permission`.
    """
    def __init__(
            self, flags: Union[PermissionArg, Iterable[PermissionArg]] = None):
        self.flags = set()
        if flags:
            if isinstance(flags, str) or is_iterable(flags):
                self.update(flags)
            else:
                self.add(flags)
    
    def add(self, flag: PermissionArg) -> None:
        """Add a permission.
        
        Args:
            flag: Permission to add.
        """
        if isinstance(flag, str):
            self.flags.add(Permission(flag))
        elif isinstance(flag, int):
            for f in Permission:
                if (f.stat_flag & flag) or (f.os_flag & flag):
                    self.flags.add(f)
        elif isinstance(flag, ModeAccess):
            if flag.readable:
                self.add(Permission.READ)
            if flag.writable:
                self.add(Permission.WRITE)
        else:
            self.flags.add(flag)
    
    def update(
            self, flags: Union['PermissionSet', Sequence[PermissionArg]]
            ) -> None:
        """Add all flags in `flags` to this `PermissionSet`.
        
        Args:
            flags: Flags to add.
        """
        for flag in flags:
            self.add(flag)
    
    @property
    def stat_flags(self) -> int:
        """Returns the binary OR of the :module:`stat` flags corresponding to
        the flags in this `PermissionSet`.
        """
        flags = 0
        for f in self.flags:
            flags |= f.stat_flag
        return flags
    
    @property
    def os_flags(self) -> int:
        """Returns the binary OR of the :module:`os` flags corresponding to
        the flags in this `PermissionSet`.
        """
        flags = 0
        for f in self.flags:
            flags |= f.os_flag
        return flags
    
    def __iter__(self) -> Iterable[Permission]:
        """Iterate over flags in the same order they appear in
        :class:`Permission`.
        """
        for f in Permission:
            if f in self.flags:
                yield f
    
    def __eq__(self, other):
        return isinstance(other, PermissionSet) and self.flags == other.flags
    
    def __contains__(self, access_flag: PermissionArg) -> bool:
        if isinstance(access_flag, str):
            access_flag = Permission(access_flag)
        return access_flag in self.flags
    
    def __repr__(self) -> str:
        return ''.join(f.value for f in Permission if f in self.flags)

class FileType(Enum):
    """Enumeration of types of files that can be opened by
    :method:`xphyle.xopen`.
    """
    STDIO = 'std'
    """One of stdin/stdout/stderr."""
    LOCAL = 'local'
    """A file on the local computer."""
    URL = 'url'
    """A URL; schema must be recognized by :module:`urllib`."""
    PROCESS = 'ps'
    """A system command to be executed in a subprocess."""
    FILELIKE = 'filelike'
    """An object that implements the methods in
    :class:`xphyle.types.FileLikeInterface`."""

class EventType(Enum):
    """Enumeration of event types that can be registered on a
    :class:`FileLikeWrapper`.
    """
    CLOSE = 'close'

class FileLikeInterface(object):
    """This is a marker interface for classes that implement method (listed
    below) to make them behave like python file objects.
    
    See Also:
        https://docs.python.org/3/tutorial/inputoutput.html#methods-of-file-objects
    """
    pass
    #def flush(self): pass
    #def close(self): pass
    #def __enter__(self): pass
    #def __exit__(self,exc_type,exc_val,exc_tb): pass
    #def next(self): pass
    #def __iter__(self): pass
    #def truncate(self, size=None): pass
    #def seek(self,offset,whence=0): pass
    #def seekable(self): pass
    #def tell(self): pass
    #def read(self, size=-1): pass
    #def readable(self): pass
    #def readline(self, size=-1): pass
    #def readlines(self, sizehint=-1): pass
    #def writable(self): pass
    #def write(self, string): pass
    #def writelines(self, seq): pass

class PathType(Enum):
    """Enumeration of supported path types (file, directory, FIFO).
    """
    FILE = 'f'
    """Path represents a file."""
    DIR = 'd'
    """Path represents a directory."""
    FIFO = '|'
    """Path represents a FIFO."""

FileLike = Union[typing.io.IO, FileLikeInterface]
"""File-like object; either a subclass of :class:`io.IOBase` or a
:class:`FileLikeInterface`.
"""

# pragma: no-cover
if sys.version_info >= (3, 6):
    PathLikeClass = os.PathLike # pylint: disable=no-member
else:
    import pathlib
    PathLikeClass = pathlib.PurePath

PathLike = Union[str, PathLikeClass]
"""Either a string path or a path-like object. In
python >= 3.6, path-like means is a subclass of os.PathLike, otherwise means
is a subclass of pathlib.PurePath.
"""

PathOrFile = Union[str, FileLike]
"""Either a string or FileLike."""

Url = Tuple[str, str, str, str, str, str]
"""URL tuple (result of urllib.parse.urlparse)."""

Range = Tuple[int, int]
"""Two-integer tuple representing a range."""

Regexp = Union[str, Pattern]
"""A regular expression string or compiled :class:`re`."""

CharMode = TypeVar('CharMode', str, bytes)
"""Type representing how data should be handled when read from a file.
If the value is bytes (:attribute:`BinMode`), raw bytes are returned. If the
value is a string (:attribute:`TextMode`), bytes are decoded using the system
default encoding.
"""

BinMode = b'b'
"""Value representing binary mode to use for an argument of type CharMode."""

TextMode = 't'
"""Value representing text mode to use for an argument of type CharMode."""

# Aliases for commonly used compound argument types

PermissionSetArg = Union[PermissionSet, Sequence[PermissionArg]]
"""Sequence of stat flags (string, int, or :class:`Permission`)."""

ModeArg = Union[str, FileMode]
"""A file mode; string, or :class:`FileMode`."""

PathTypeArg = Union[str, PathType]
"""A path type string or :class:`PathType`."""

EventTypeArg = Union[str, EventType]
"""An event type name or :class:`EventType`."""

FilesArg = Iterable[Union[str, Tuple[Any, PathOrFile]]]
"""Multiple files: an iterable over either strings or (key, PathOrFile)."""

CompressionArg = Union[bool, str]
"""Compression can be True, False, or the name of a compression format."""
