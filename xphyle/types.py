# -*- coding: utf-8 -*-
"""Type checking support. Defines commonly used types.
"""
# pylint: disable=wildcard-import, unused-wildcard-import, import-error, invalid-name
from abc import ABCMeta, abstractmethod
import collections
from enum import Enum
import os
import stat
import sys
from types import ModuleType
# HACK: critical features were introduced in python 3.5.2, so we force use
# of backports.typing even in 3.5
if sys.version_info < (3, 6):
    from backports.typing import *
    from backports.typing import IO
    from backports.typing.re import *
else:
    from typing import *
    # ISSUE: Not sure why I have to import IO separately
    from typing import IO
    from typing.re import *

# ISSUE: there are several mypy errors due to incomplete enum support.
# enums should be fully suported in the next version of mypy (>0.501).

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

FILE_MODE_CACHE = {} # type: Dict[Tuple[str, ModeAccessArg, ModeCodingArg], FileMode]
"""Cache of FileMode objects."""

class FileMode(object):
    """Definition of a file mode as composed of a :class:`ModeAccess` and a
    :class:`ModeCoding`.
    
    Args:
        mode: Specify the mode as a string; mutually exclusive with `access`
            and `coding`.
        access: The file access mode (default: :attribute:`ModeAccess.READ`).
        coding: The file open mode (default: :attribute:`ModeCoding.TEXT`).
    """
    def __new__(
            cls, mode: str = None, access: ModeAccessArg = None,
            coding: ModeCodingArg = None) -> 'FileMode':
        key = (mode, access, coding)
        if not key in FILE_MODE_CACHE:
            FILE_MODE_CACHE[key] = super().__new__(cls)
        return FILE_MODE_CACHE[key]
    
    def __init__(
            self, mode: str = None, access: ModeAccessArg = None,
            coding: ModeCodingArg = None) -> None:
        if mode:
            access_val = None
            for a in ModeAccess:
                if a.value in mode:
                    access_val = a
                    break
            coding_val = None
            for e in ModeCoding:
                if e.value in mode:
                    coding_val = e
                    break
        else:
            if isinstance(access, str):
                access_val = ModeAccess(access)
            else:
                access_val = cast(ModeAccess, access)
            if isinstance(coding, str):
                coding_val = ModeCoding(coding)
            else:
                coding_val = cast(ModeCoding, coding)
        
        self.access = access_val or ModeAccess.READ
        self.coding = coding_val or ModeCoding.TEXT
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
    
    def __eq__(self, other):
        return (isinstance(other, FileMode) and
            self.access == other.access and
            self.coding == other.coding)
    
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

PERMISSION_SET_CACHE = {} # type: Dict[Union[PermissionArg, Iterable[PermissionArg]], PermissionSet]

class PermissionSet(object):
    """A set of :class:`Permission`s.
    
    Args:
        flags: Sequence of flags as string ('r', 'w', 'x'), int,
            :class:`ModeAccess`, or :class:`Permission`.
    """
    def __new__(
            cls, flags: Union[PermissionArg, Iterable[PermissionArg]] = None
            ) -> 'PermissionSet':
        if not flags in PERMISSION_SET_CACHE:
            PERMISSION_SET_CACHE[flags] = super().__new__(cls)
        return PERMISSION_SET_CACHE[flags]
    
    def __init__(
            self, flags: Union[PermissionArg, Iterable[PermissionArg]] = None
            ) -> None:
        self.flags = set() # type: Set[Permission]
        if flags:
            if isinstance(flags, str) or is_iterable(flags):
                self.update(cast(Iterable[PermissionArg], flags))
            else:
                self.add(cast(Union[int, Permission, ModeAccess], flags))
    
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
            self, flags: Union['PermissionSet', Iterable[PermissionArg]]
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
    BUFFER = 'buffer'
    """A StringIO or BytesIO."""

class EventType(Enum):
    """Enumeration of event types that can be registered on an
    :class:`EventManager`.
    """
    CLOSE = 'close'

AnyChar = Union[bytes, Text]
"""Similar to AnyStr, but specifies that strings must be unicode."""

class FileLikeInterface(IO, Iterable[AnyChar], metaclass=ABCMeta):
    """This is a marker interface for classes that implement methods (listed
    below) to make them behave like python file objects. Provides a subset of
    methods from typing.io.IO, plus next() and __iter__.
    
    See Also:
        https://docs.python.org/3/tutorial/inputoutput.html#methods-of-file-objects
    """
    @abstractmethod
    def next(self) -> AnyChar:
        raise NotImplementedError()


class FileLikeBase(FileLikeInterface):
    def readable(self) -> bool:
        return False
    
    def read(self, n: int = -1) -> AnyChar:
        raise NotImplementedError()
    
    def readline(self, hint: int = -1) -> AnyChar:
        raise NotImplementedError()
    
    def readlines(self, sizehint: int = -1) -> List[AnyChar]:
        raise NotImplementedError()
    
    def writable(self) -> bool:
        return False
    
    def write(self, string: AnyChar) -> int:
        raise NotImplementedError()
    
    def writelines(self, lines: Iterable[AnyChar]) -> None:
        raise NotImplementedError()
    
    def seek(self, offset, whence: int = 0) -> int:
        raise NotImplementedError()
    
    def seekable(self) -> bool:
        return False
    
    def tell(self) -> int:
        raise NotImplementedError()
    
    def isatty(self) -> bool:
        return False
    
    def fileno(self) -> int:
        return -1
    
    def truncate(self, size: int = None) -> int:
        raise NotImplementedError()
    
    def __enter__(self) -> Any:
        return self
    
    def __exit__(self, exception_type, exception_value, traceback) -> bool:
        self.close()
        return False
    
    def __iter__(self) -> Iterator[AnyChar]:
        raise NotImplementedError()
    
    def __next__(self) -> AnyChar:
        raise NotImplementedError()
    
    def next(self) -> AnyChar:
        return self.__next__()


class PathType(Enum):
    """Enumeration of supported path types (file, directory, FIFO).
    """
    FILE = 'f'
    """Path represents a file."""
    DIR = 'd'
    """Path represents a directory."""
    FIFO = '|'
    """Path represents a FIFO."""

FileLike = Union[IO, FileLikeInterface]
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

PathOrFile = Union[PathLike, FileLike]
"""Either a string or FileLike."""

Url = Tuple[str, str, str, str, str, str]
"""URL tuple (result of urllib.parse.urlparse)."""

Range = Tuple[int, int]
"""Two-integer tuple representing a range."""

Regexp = Union[str, Pattern]
"""A regular expression string or compiled :class:`re`."""

CharMode = TypeVar('CharMode', bytes, Text)
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

FilesArg = Iterable[Union[PathLike, Tuple[Any, PathOrFile]]]
"""Multiple files: an iterable over either strings or (key, PathOrFile)."""

CompressionArg = Union[bool, str]
"""Compression can be True, False, or the name of a compression format."""
