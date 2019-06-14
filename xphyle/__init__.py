# -*- coding: utf-8 -*-
"""The main xphyle methods -- xopen, popen, and open_.
"""
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
import io
import os
from pathlib import Path, PurePath
import shlex
import signal
from subprocess import Popen, PIPE, TimeoutExpired
import sys
import warnings
from typing import (
    Callable,
    Container,
    Iterable,
    Iterator,
    Union,
    Sequence,
    List,
    Tuple,
    Dict,
    Any,
    Generic,
    TypeVar,
    Generator,
    IO,
    Optional,
    Type,
    cast,
)

from xphyle.formats import FORMATS, THREADS
from xphyle.paths import (
    STDIN,
    STDOUT,
    STDERR,
    EXECUTABLE_CACHE,
    check_readable_file,
    check_writable_file,
    safe_check_readable_file,
    deprecated_str_to_path,
    convert_std_placeholder
)
from xphyle.progress import ITERABLE_PROGRESS, PROCESS_PROGRESS
from xphyle.types import (
    FileType,
    FileLikeInterface,
    FileLike,
    FileMode,
    ModeArg,
    ModeAccess,
    ModeCoding,
    CompressionArg,
    EventType,
    EventTypeArg,
    PathOrFile,
    FileLikeBase,
    AnyChar,
)
from xphyle.urls import parse_url, open_url, get_url_file_name


# pylint: disable=protected-access
# noinspection PyProtectedMember
from xphyle._version import get_versions

__version__ = get_versions()["version"]
del get_versions


# Classes


E = TypeVar("E", bound="EventManager")


class EventListener(Generic[E], metaclass=ABCMeta):
    """Base class for listener events that can be registered on a
    FileLikeWrapper.

    Args:
        kwargs: keyword arguments to pass through to ``execute``
    """

    def __init__(self, **kwargs) -> None:
        self.create_args = kwargs

    def __call__(self, wrapper: E, **call_args) -> None:
        """Called by :method:`FileLikeWrapper._fire_listeners`.

        Args:
            wrapper: The :class:`EventManager` on which this event was
                registered.
            call_args: Additional keyword arguments, which are merged with
                :attribute:`create_args`.
        """
        kwargs = dict(self.create_args)
        if call_args:
            kwargs.update(call_args)
        self.execute(wrapper, **kwargs)

    @abstractmethod
    def execute(self, wrapper: E, **kwargs) -> None:
        """Handle an event. This method must be implemented by subclasses.

        Args:
            wrapper: The :class:`EventManager` on which this event was
                registered.
            kwargs: A union of the keyword arguments passed to the constructor
                and the __call__ method.
        """
        raise NotImplementedError()


class EventManager:
    """Mixin type for classes that allow registering event listners.
    """

    def __init__(self) -> None:
        self._listeners: Dict[EventType, List[EventListener]] = defaultdict(lambda: [])

    def register_listener(self, event: EventTypeArg, listener: EventListener) -> None:
        """Register an event listener.

        Args:
            event: Event name (currently, only 'close' is recognized)
            listener: A listener object, which must be callable with a
                single argument -- this file wrapper.
        """
        # if not hasattr(self, '_listeners'):
        #    object.__setattr__(self, '_listeners', defaultdict(lambda: []))
        if isinstance(event, str):
            event = EventType(event)
        self._listeners[event].append(listener)

    def _fire_listeners(self, event: EventType, **kwargs) -> None:
        """Fire :class:`FileEventListener`s associated with `event`.

        Args:
            event: The event type.
            kwargs: Additional arguments to pass to the listener.
        """
        if event in self._listeners:
            for listener in self._listeners[event]:
                listener(self, **kwargs)


class FileLikeWrapper(EventManager, FileLikeBase):
    """Base class for wrappers around file-like objects. By default, method
    calls are forwarded to the file object. Adds the following:

    1. A simple event system by which registered listeners can respond to
    file events. Currently, 'close' is the only supported event
    2. Wraps file iterators in a progress bar (if configured)

    Args:
        fileobj: The file-like object to wrap.
        compression: Whether the wrapped file is compressed.
        close_fileobj: Whether to close the wrapped file object when closing
            this wrapper.

    """

    def __init__(
        self,
        fileobj: FileLike,
        compression: CompressionArg = False,
        close_fileobj: bool = True,
    ) -> None:
        EventManager.__init__(self)
        self._fileobj = fileobj
        self._iterator: Optional[Iterator] = None
        self.compression = compression
        self.close_fileobj = close_fileobj

    def __next__(self) -> bytes:
        return next(iter(self))

    def __iter__(self) -> Iterator:
        if self._iterator is None:
            self._iterator = iter(ITERABLE_PROGRESS.wrap(self._fileobj, desc=self.name))
        return self._iterator

    def __enter__(self) -> "FileLikeWrapper":
        if self.closed:
            raise IOError("I/O operation on closed file.")
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> bool:
        self.close()
        return False

    def peek(self, size: int = 1) -> AnyChar:
        """Return bytes/characters from the stream without advancing the
        position. At most one single read on the raw stream is done to satisfy
        the call.

        Args:
            size: The max number of bytes/characters to return.

        Returns:
            At most `size` bytes/characters. Unlike io.BufferedReader.peek(),
            will never return more than `size` bytes/characters.

        Notes:
            If the file uses multi-byte encoding and N characters are desired,
            it is up to the caller to request `size=2N`.
        """
        if not FileMode(self._fileobj.mode).readable:
            raise IOError("Can only call peek() on a readable file")

        if hasattr(self._fileobj, "peek"):
            # The underlying file has a peek() method
            peek = getattr(self._fileobj, "peek")(size)
            # I don't think the following is a valid state
            # if 't' in self._fileobj.mode:
            #     if isinstance(peek, 'bytes'):
            #         if hasattr(self._fileobj, 'encoding'):
            #             peek = peek_bytes.decode(self._fileobj.encoding)
            #         else:
            #             peek = peek_bytes.decode()
            if len(peek) > size:
                peek = peek[:size]
        elif hasattr(self._fileobj, "seek"):
            # The underlying file has a seek() method
            curpos = self._fileobj.tell()
            try:
                peek = self._fileobj.read(size)
            finally:
                self._fileobj.seek(curpos)
        else:  # pragma: no-cover
            # TODO I don't think it's possible to get here, but leaving for now
            raise IOError("Unpeekable file: {}".format(self.name))
        return peek

    def close(self) -> None:
        """Close the file, close an open iterator, and fire 'close' events to
        any listeners.
        """
        self._close()
        if hasattr(self, "_iterator"):
            delattr(self, "_iterator")
        self._fire_listeners(EventType.CLOSE)

    def _close(self) -> None:
        if self.close_fileobj:
            self._fileobj.close()

    # Pass-through methods

    @property
    def name(self) -> str:  # pragma: no-cover
        return self._fileobj.name

    @property
    def mode(self) -> str:  # pragma: no-cover
        return self._fileobj.mode

    @property
    def closed(self) -> bool:  # pragma: no-cover
        return self._fileobj.closed

    def readable(self) -> bool:  # pragma: no-cover
        return self._fileobj.readable()

    def read(self, size: int = -1) -> bytes:  # pragma: no-cover
        return self._fileobj.read(size)

    def readline(self, size: int = -1) -> AnyChar:  # pragma: no-cover
        return self._fileobj.readline(size)

    def readlines(self, hint: int = -1) -> List[AnyChar]:  # pragma: no-cover
        return self._fileobj.readlines(hint)

    def writable(self) -> bool:  # pragma: no-cover
        return self._fileobj.writable()

    def write(self, string: AnyChar) -> int:  # pragma: no-cover
        return self._fileobj.write(string)

    def writelines(self, lines: Iterable[AnyChar]) -> None:  # pragma: no-cover
        self._fileobj.writelines(lines)

    def flush(self) -> None:  # pragma: no-cover
        self._fileobj.flush()

    def seekable(self) -> bool:  # pragma: no-cover
        return self._fileobj.seekable()

    def seek(self, offset, whence: int = 0) -> int:  # pragma: no-cover
        return self._fileobj.seek(offset, whence=whence)

    def tell(self) -> int:  # pragma: no-cover
        return self._fileobj.tell()

    def isatty(self) -> bool:  # pragma: no-cover
        return self._fileobj.isatty()

    def fileno(self) -> int:  # pragma: no-cover
        return self._fileobj.fileno()

    def truncate(self, size: int = None) -> int:  # pragma: no-cover
        return self._fileobj.truncate(size=size)


class FileWrapper(FileLikeWrapper):
    """Wrapper around a file object.

    Args:
        source: Path or file object.
        mode: File open mode.
        compression: Compression type.
        name: Use an alternative name for the file.
        kwargs: Additional arguments to pass to xopen.
    """

    @deprecated_str_to_path(1, "source")
    def __init__(
        self,
        source: PathOrFile,
        mode: ModeArg = "w",
        compression: CompressionArg = False,
        name: Union[str, PurePath] = None,
        close_fileobj: bool = True,
        **kwargs,
    ) -> None:
        if isinstance(source, Path):
            self._path = source
            source_fileobj = xopen(source, mode=mode, compression=compression, **kwargs)
        else:
            source_fileobj = cast(FileLike, source)
            if name is None and hasattr(source_fileobj, "name"):
                name = str(getattr(source_fileobj, "name"))
            self._path = Path(name) if name else None
        super().__init__(
            source_fileobj, compression=compression, close_fileobj=close_fileobj
        )
        self._name = str(name)
        if mode is None and hasattr(source, "mode"):
            self._mode = getattr(source_fileobj, "mode")
        if mode:
            self._mode = str(mode)
        else:
            self._mode = None

    @property
    def name(self) -> str:
        if hasattr(self, "_name"):
            return getattr(self, "_name")
        return super().name

    @property
    def path(self) -> PurePath:
        """The source path.
        """
        return getattr(self, "_path", None)


class BufferWrapper(FileWrapper):
    """Wrapper around a string/bytes buffer.

    Args:
        fileobj: The fileobj to wrap (the raw or wrapped buffer).
        buffer: The raw buffer.
        compression: Compression type.
        close_fileobj: Whether to close the buffer when closing this wrapper.
    """

    def __init__(
        self,
        fileobj: PathOrFile,
        buffer: Union[io.StringIO, io.BytesIO],
        compression: CompressionArg = False,
        name: str = None,
        **kwargs,
    ) -> None:
        super().__init__(fileobj, compression=compression, name=name, **kwargs)
        self.buffer = buffer

    def getvalue(self) -> AnyChar:
        """Returns the contents of the buffer.
        """
        if hasattr(self, "_value"):
            return getattr(self, "_value")
        else:
            return self.buffer.getvalue()

    def _close(self):
        if self.compression:
            self._fileobj.close()
            setattr(self, "_value", self.buffer.getvalue())
        elif self.close_fileobj:
            setattr(self, "_value", self.buffer.getvalue())
            self._fileobj.close()


class StdWrapper(FileLikeWrapper):
    """Wrapper around stdin/stdout/stderr.

    Args:
        stream: The stream to wrap.
        compression: Compression type.
    """

    def __init__(self, stream: FileLike, compression: CompressionArg = False) -> None:
        super().__init__(stream, compression=compression)
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def _close(self):
        self._fileobj.flush()
        self._closed = True


PopenStdArg = Union[PathOrFile, int]  # pylint: disable=invalid-name


# noinspection PyAbstractClass
class Process(Popen, EventManager, FileLikeBase, Iterable):
    """Subclass of :class:`subprocess.Popen` with the following additions:

    * Provides :method:`Process.wrap_pipes` for wrapping stdin/stdout/stderr
    (e.g. to send compressed data to a process' stdin or read compressed data
    from its stdout/stderr).
    * Provides :method:`Process.close` for properly closing stdin/stdout/stderr
    streams and terminating the process.
    * Implements required methods to make objects 'file-like'.

    Args:
        args: Positional arguments, passed to :class:`subprocess.Popen`
            constructor.
        stdin, stdout, stderr: Identical to the same arguments to
            :class:`subprocess.Popen`.
        kwargs: Keyword arguments, passed to :class:`subprocess.Popen`
            constructor.
    """

    def __init__(
        self,
        args,
        stdin: PopenStdArg = None,
        stdout: PopenStdArg = None,
        stderr: PopenStdArg = None,
        **kwargs,
    ) -> None:
        Popen.__init__(
            cast(Popen, self), args, stdin=stdin, stdout=stdout, stderr=stderr, **kwargs
        )
        EventManager.__init__(self)
        # Construct a dict of name=(stream, wrapper, is_pipe) for std streams
        self._name = " ".join(args)
        self._std = dict(
            (name, [stream, None, desc == PIPE])
            for name, desc, stream in zip(
                ("stdin", "stdout", "stderr"),
                (stdin, stdout, stderr),
                (self.stdin, self.stdout, self.stderr),
            )
        )
        self._iterator: Optional[Iterator[str]] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def mode(self) -> str:
        if self.writable():
            mode = self.get_writer().mode
            if self.readable() and ("b" in mode) == ("b" in self.get_reader().mode):
                mode += "r"
            return mode
        elif self.readable():
            return self.get_reader().mode
        else:
            raise TypeError("Process is not readable or writable")

    def wrap_pipes(self, **kwargs) -> None:
        """Wrap stdin/stdout/stderr PIPE streams using xopen.

        Args:
            kwargs: for each of 'stdin', 'stdout', 'stderr', a dict providing
                arguments to xopen describing how the stream should be wrapped.
        """
        for name, args in kwargs.items():
            if name not in self._std:
                raise ValueError("Invalid stream name: {}".format(name))
            std = self._std[name]
            if not std[2]:
                raise IOError("Only PIPE streams can be wrapped")
            args["validate"] = False
            std[1] = xopen(std[0], **args)

    def is_wrapped(self, name: str) -> bool:
        """Returns True if the stream corresponding to `name` is wrapped.

        Args:
            name: One of 'stdin', 'stdout', 'stderr'
        """
        if name not in self._std:
            raise ValueError("Invalid stream name: {}".format(name))
        return self._std[name][1] is not None

    def writable(self) -> bool:
        """Returns True if this Popen has stdin, otherwise False.
        """
        return self.get_writer() is not None

    def write(self, data: AnyChar) -> int:
        """Write `data` to stdin.

        Args:
            data: The data to write; must be bytes if stdin is a byte stream or
                string if stdin is a text stream.

        Returns:
            Number of bytes/characters written
        """
        return self.get_writer().write(data)

    def get_writer(self) -> FileLike:
        """Returns the stream for writing to stdin.
        """
        stdin = self._std["stdin"]
        return stdin[1] or stdin[0]

    def readable(self) -> bool:
        """Returns True if this Popen has stdout and/or stderr, otherwise False.
        """
        return self.get_reader() is not None

    def read(self, size: int = -1, which: str = None) -> bytes:
        """Read `size` bytes/characters from stdout or stderr.

        Args:
            size: Number of bytes/characters to read.
            which: Which stream to read from, 'stdout' or 'stderr'. If None,
                stdout is used if it exists, otherwise stderr.

        Returns:
            The bytes/characters read from the specified stream.
        """
        return self.get_reader(which).read(size)

    def get_reader(self, which: str = None) -> FileLike:
        """Returns the stream for reading data from stdout/stderr.

        Args:
            which: Which stream to read from, 'stdout' or 'stderr'. If None,
                stdout is used if it exists, otherwise stderr.

        Returns:
            The specified stream, or None if the stream doesn't exist.
        """
        if which in ("stdout", None):
            std = self._std["stdout"]
        else:
            std = self._std["stderr"]
        return std[1] or std[0]

    def get_readers(self):
        """Returns (stdout, stderr) tuple.
        """
        return tuple(self.get_reader(std) for std in ("stdout", "stderr"))

    # ISSUE: No idea why mypy says the type of `inp` is incompatible with
    # super class.
    def communicate(self, inp: AnyChar = None, timeout: float = None) -> Tuple[IO, IO]:
        """Send input to stdin, wait for process to terminate, return
        results.

        Args:
            inp: Input to send to stdin.
            timeout: Time to wait for process to finish.

        Returns:
            Tuple of (stdout, stderr).
        """
        if inp:
            self.write(inp)
        self.close1(timeout, True, True)
        return self.stdout, self.stderr

    def flush(self) -> None:
        """Flushes stdin if there is one.
        """
        if self.writable():
            self.get_writer().flush()

    def __next__(self) -> AnyChar:
        """Returns the next line from the iterator.
        """
        return next(iter(self))

    def __iter__(self) -> Iterator[AnyChar]:
        """Returns the currently open iterator. If one isn't open,
        :method:`_reader(which=None)` is used to create one.
        """
        if not self._iterator:
            self._iterator = iter(
                ITERABLE_PROGRESS.wrap(
                    cast(Iterable[AnyChar], self.get_reader()), desc=str(self)
                )
            )
        return self._iterator

    def __enter__(self) -> "Process":
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> bool:
        """On exit from a context manager, calls
        :method:`close(raise_on_error=True, record_output=True)`.
        """
        if not self.closed:
            self.close1(raise_on_error=True, record_output=True)
        return False

    def __del__(self, _maxsize=sys.maxsize, _warn=warnings.warn) -> None:
        if not self.closed:
            try:
                self.close1(1, False, False, True)
            except IOError:  # pragma: no-cover
                pass
        super().__del__(_maxsize=_maxsize, _warn=_warn)

    @property
    def closed(self):
        """Whether the Process has been closed.
        """
        return self._std is None

    def close(self) -> None:
        self.close1()

    def close1(
        self,
        timeout: float = None,
        raise_on_error: bool = False,
        record_output: bool = False,
        terminate: bool = False,
    ) -> Optional[int]:
        """Close stdin/stdout/stderr streams, wait for process to finish, and
        return the process return code.

        Args:
            timeout: time in seconds to wait for stream to close; negative
                value or None waits indefinitely.
            raise_on_error: Whether to raise an exception if the process returns
                an error.
            record_output: Whether to store contents of stdout and stderr in
                place of the actual streams after closing them.
            terminate: If True and `timeout` is a positive integer, the process
                is terminated if it doesn't finish within `timeout` seconds.

        Notes:
            If :attribute:`record_output` is True, and if stdout/stderr is a
            PIPE, any contents are read and stored as the value of
            :attribute:`stdout`/:attribute:`stderr`. Otherwise the data is lost.

        Returns:
            The process returncode.

        Raises:
            IOError if `raise_on_error` is True and the process returns an
                error code.
        """
        if self.closed:
            if raise_on_error:
                raise IOError("Process already closed")
            else:
                return None

        stdin = self._std["stdin"]
        if stdin and stdin[0]:
            if stdin[1]:
                stdin[1].close()
            try:
                stdin[0].close()
            except IOError:  # pragma: no-cover
                pass
            self._std["stdin"] = None

        try:
            self.wait(timeout)
        except TimeoutExpired:
            if terminate:
                self.terminate()
            else:
                raise

        def _close_reader(name):
            std = self._std[name]
            data = None
            if std and std[0]:
                if record_output:
                    reader = std[1] or std[0]
                    data = reader.read()
                if std[1]:
                    std[1].close()
                std[0].close()
            self._std[name] = None
            return data

        self.stdout = _close_reader("stdout")
        self.stderr = _close_reader("stderr")
        self._iterator = None
        self._std = None

        if raise_on_error:
            self.check_valid_returncode()

        self._fire_listeners(EventType.CLOSE, returncode=self.returncode)

        return self.returncode

    def check_valid_returncode(
        self, valid: Container[int] = (0, None, signal.SIGPIPE, signal.SIGPIPE + 128)
    ):
        """Check that the returncodes does not have a value associated with
        an error state.

        Raises:
            IOError if :attribute:`returncode` is associated with an error
            state.
        """
        if self.returncode not in valid:
            raise IOError("Process existed with return code {}".format(self.returncode))

    def readline(self, hint: int = -1, which: str = None) -> AnyChar:
        return self.get_reader(which).readline(hint)

    def readlines(self, sizehint: int = -1, which: str = None) -> List[AnyChar]:
        return self.get_reader(which).readlines(sizehint)

    def writelines(self, lines: Iterable[AnyChar]) -> None:
        self.get_writer().writelines(lines)


# Methods


DEFAULTS: Dict[str, Any] = dict(xopen_context_wrapper=False)


# noinspection PyShadowingNames
def configure(
    default_xopen_context_wrapper: Optional[bool] = None,
    progress: Optional[bool] = None,
    progress_wrapper: Optional[Callable[..., Iterable]] = None,
    system_progress: Optional[bool] = None,
    system_progress_wrapper: Optional[Union[str, Sequence[str]]] = None,
    threads: Optional[Union[int, bool]] = None,
    executable_path: Optional[Union[PurePath, Sequence[PurePath]]] = None,
) -> None:
    """Conifgure xphyle.

    Args:
        default_xopen_context_wrapper: Whether to wrap files opened by
            :method:`xopen` in :class:`FileLikeWrapper`s by default (when
            `xopen`'s context_wrapper parameter is `None`.
        progress: Whether to wrap long-running operations with a progress bar
        progress_wrapper: Specify a non-default progress wrapper
        system_progress: Whether to use progress bars for system-level
        system_progress_wrapper: Specify a non-default system progress wrapper
        threads: The number of threads that can be used by compression formats
            that support parallel compression/decompression. Set to None or a
            number < 1 to automatically initalize to the number of cores on
            the local machine.
        executable_path: List of paths where xphyle should look for system
            executables. These will be searched before the default system path.
    """
    if default_xopen_context_wrapper is not None:
        # ISSUE: mypy doesn't recognize valid generator statement
        DEFAULTS.update(xopen_context_wrapper=default_xopen_context_wrapper)
    if progress is not None:
        ITERABLE_PROGRESS.update(progress, progress_wrapper)
    if system_progress is not None:
        PROCESS_PROGRESS.update(system_progress, system_progress_wrapper)
    if threads is not None:
        THREADS.update(threads)
    if executable_path:
        EXECUTABLE_CACHE.add_search_path(executable_path)


# The following doesn't work due to a known bug
# https://github.com/python/typing/issues/266
OpenArg = Union[PathOrFile, bytes, str, Type[Union[bytes, str]]]


# TODO: interesting idea from a reddit user - have open_ return an object
# that overloads the | operator.


@contextmanager
def open_(
    target: OpenArg,
    mode: ModeArg = None,
    errors: bool = True,
    wrap_fileobj: bool = True,
    **kwargs,
) -> Generator[FileLike, None, None]:
    """Context manager that frees you from checking if an argument is a path
    or a file object. Calls ``xopen`` to open files.

    Args:
        target: A relative or absolute path, a URL, a system command, a
            file-like object, or :class:`bytes` or :class:`str` to indicate a
            writeable byte/string buffer.
        mode: The file open mode.
        errors: Whether to raise an error if there is a problem opening the
            file. If False, yields None when there is an error.
        wrap_fileobj: If path_or_file is a file-likek object, this parameter
            determines whether it will be passed to xopen for wrapping (True)
            or returned directly (False). If False, any `kwargs` are ignored.
        kwargs: Additional args to pass through to xopen (if ``f`` is a path).

    Yields:
        A file-like object, or None if ``errors`` is False and there is a
        problem opening the file.

    Examples:
        with open_('myfile') as infile:
            print(next(infile))

        fileobj = open('myfile')
        with open_(fileobj) as infile:
            print(next(infile))
    """
    if target is None:
        if errors:
            raise ValueError("'target' cannot be None")
        else:
            yield None
    else:
        is_fileobj = not (
            isinstance(target, str)
            or isinstance(target, PurePath)
            or target in (str, bytes)
        )
        if not wrap_fileobj:
            if is_fileobj:
                yield target
            else:
                raise ValueError(
                    "'wrap_fileobj must be True if 'path' is not file-like"
                )
        else:
            kwargs["context_wrapper"] = True
            try:
                with xopen(target, mode, **kwargs) as fileobj:
                    yield fileobj
            except IOError:
                if errors:
                    raise
                else:
                    yield None


def xopen(
    target: OpenArg,
    mode: ModeArg = None,
    compression: CompressionArg = None,
    use_system: bool = True,
    allow_subprocesses: bool = True,
    context_wrapper: bool = None,
    file_type: FileType = None,
    validate: bool = True,
    overwrite: bool = True,
    close_fileobj: bool = True,
    **kwargs,
) -> FileLike:
    """
    Replacement for the builtin `open` function that can also open URLs and
    subprocessess, and automatically handles compressed files.

    Args:
        target: A relative or absolute path, a URL, a system command, a
            file-like object, or :class:`bytes` or :class:`str` to
            indicate a writeable byte/string buffer.
        mode: Some combination of the access mode ('r', 'w', 'a', or 'x')
            and the open mode ('b' or 't'). If the later is not given, 't'
            is used by default.
        compression: If None or True, compression type (if any) will be
            determined automatically. If False, no attempt will be made to
            determine compression type. Otherwise this must specify the
            compression type (e.g. 'gz'). See `xphyle.compression` for
            details. Note that compression will *not* be guessed for
            '-' (stdin).
        use_system: Whether to attempt to use system-level compression
            programs.
        allow_subprocesses: Whether to allow `path` to be a subprocess (e.g.
            '|cat'). There are security risks associated with allowing
            users to run arbitrary system commands.
        context_wrapper: If True, the file is wrapped in a `FileLikeWrapper`
            subclass before returning (`FileWrapper` for files/URLs,
            `StdWrapper` for STDIN/STDOUT/STDERR). If None, the default value
            (set using :method:`configure`) is used.
        file_type: a FileType; explicitly specify the file type. By default the
            file type is detected, but auto-detection might make mistakes, e.g.
            a local file contains a colon (':') in the name.
        validate: Ensure that the user-specified compression format matches the
            format guessed from the file extension or magic bytes.
        overwrite: For files opened in write mode, whether to overwrite
            existing files (True).
        close_fileobj: When `path` is a file-like object / `file_type` is
            FileType.FILELIKE, and `context_wrapper` is True, whether to close
            the underlying file when closing the wrapper.
        kwargs: Additional keyword arguments to pass to ``open``.

    `path` is interpreted as follows:
        * If starts with '|', it is assumed to be a system command
        * If a file-like object, it is used as-is
        * If one of STDIN, STDOUT, STDERR, the appropriate `sys` stream is used
        * If parseable by `xphyle.urls.parse_url()`, it is assumed to be a URL
        * If file_type == FileType.BUFFER and path is a string or bytes and
          mode is readable, a new StringIO/BytesIO is created with 'path' passed
          to its constructor.
        * Otherwise it is assumed to be a local file

    If `use_system` is True and the file is compressed, the file is opened with
    a pipe to the system-level compression program (e.g. ``gzip`` for '.gz'
    files) if possible, otherwise the corresponding python library is used.

    Returns:
        A Process if `file_type` is PROCESS, or if `file_type` is None and
        `path` starts with '|'. Otherwise, an opened file-like object. If
        `context_wrapper` is True, this will be a subclass of `FileLikeWrapper`.

    Raises:
        ValueError if:
            * ``compression`` is True and compression format cannot be
            determined
            * the specified compression format is invalid
            * ``validate`` is True and the specified compression format is not
                the acutal format of the file
            * the path or mode are invalid
    """
    if compression and isinstance(compression, str):
        cannonical_fmt_name = FORMATS.get_compression_format_name(compression)
        if cannonical_fmt_name is None:
            raise ValueError("Invalid compression format: {}".format(compression))
        else:
            compression = cannonical_fmt_name

    # Convert placeholder strings ("-", "_") to paths
    target = convert_std_placeholder(target)

    # Whether the file object is stdin/stdout/stderr
    is_std = target in (STDIN, STDOUT, STDERR)
    # Whether 'target' is currently a file-like object in binary mode
    is_bin = False
    # Whether target is a string
    is_str = isinstance(target, str)
    # Whether target is a Path
    is_path = not is_std and isinstance(target, PurePath)
    # Whether target is a class indicating a buffer type
    is_buffer = target in (str, bytes)

    if not file_type:
        if is_path:
            file_type = FileType.LOCAL
        elif is_std:
            file_type = FileType.STDIO
        elif is_buffer:
            file_type = FileType.BUFFER
        elif not is_str:
            file_type = FileType.FILELIKE
        elif target.startswith("|"):
            file_type = FileType.PROCESS
    elif file_type == FileType.BUFFER and (
        is_str or is_path or isinstance(target, bytes)
    ):
        if not mode:
            mode = FileMode(access="r", coding="t" if is_str else "b")
        is_buffer = True
    elif (
        (is_str or is_path or is_buffer) == (file_type is FileType.FILELIKE)
        or is_std != (file_type is FileType.STDIO)
        or is_buffer != (file_type is FileType.BUFFER)
    ):
        raise ValueError(f"file_type = {file_type} does not match target {target}")

    url_parts = None
    if file_type in (FileType.URL, None):
        url_parts = parse_url(target)
        if not file_type:
            file_type = FileType.URL if url_parts else FileType.LOCAL
        elif not url_parts:
            raise ValueError(f"{target} is not a valid URL")

    if not mode:
        # set to default
        if not is_buffer:
            mode = FileMode()
        elif target == str:
            mode = FileMode("wt")
        else:
            mode = FileMode("wb")
    elif isinstance(mode, str):
        if "U" in mode and "newline" in kwargs and kwargs["newline"] is not None:
            raise ValueError(
                "newline={} not compatible with universal newlines ('U') "
                "mode".format(kwargs["newline"])
            )
        mode = FileMode(mode)

    if context_wrapper is None:
        context_wrapper = DEFAULTS["xopen_context_wrapper"]

    # Return early if opening a process
    if file_type is FileType.PROCESS:
        if not allow_subprocesses:
            raise ValueError("Subprocesses are disallowed")
        if target.startswith("|"):
            target = target[1:]
        popen_args = dict(kwargs)
        for std in ("stdin", "stdout", "stderr"):
            popen_args[std] = PIPE
        if mode.writable:
            if compression is True:
                raise ValueError(
                    "Can determine compression automatically when writing to "
                    "process stdin"
                )
            elif compression is None:
                compression = False
            outstream = "stdin"
        else:
            outstream = "stdout"
        popen_args[outstream] = dict(
            mode=mode,
            compression=compression,
            validate=validate,
            context_wrapper=context_wrapper,
        )
        return popen(target, **popen_args)

    buffer = None

    if file_type is FileType.BUFFER:
        if target == str:
            target = io.StringIO()
        elif target == bytes:
            target = io.BytesIO()
            is_bin = True
        elif is_str or isinstance(target, bytes):
            if not mode.readable:
                raise ValueError(
                    "'mode' must be readable when 'file_type' == BUFFER "
                    "and 'target' is string or bytes."
                )
            if is_str:
                if mode.coding != ModeCoding.TEXT:
                    raise ValueError("Must use text mode with a string buffer")
                target = io.StringIO(target)
            else:
                if mode.coding != ModeCoding.BINARY:
                    raise ValueError("Must use binary mode with a bytes buffer")
                target = io.BytesIO(target)
                is_bin = True
        if context_wrapper:
            buffer = target
        if not mode.readable:
            if compression is True:
                raise ValueError("Cannot guess compression for a write-only buffer")
            elif compression is None:
                compression = False
            validate = False

    # The file handle we will open
    # TODO: figure out the right type
    fileobj: Any = None
    # The name to use for the file
    name = None
    # Guessed compression type, if compression in (None, True)
    guess = None
    # Whether to try and guess file format
    guess_format = compression in (None, True)
    # Whether to validate that the actual compression format matches expected
    validate = validate and bool(compression) and not guess_format

    if file_type is FileType.STDIO:
        use_system = False
        if target == STDERR:
            if not mode.writable:
                raise ValueError("Mode must be writable for stderr")
            stdobj = sys.stderr
        else:
            stdobj = sys.stdin if mode.readable else sys.stdout

        # whether we need the underlying byte stream regardless of the mode
        check_readable = mode.readable and (validate or guess_format)

        if mode.binary or compression or check_readable:
            # get the underlying binary stream
            fileobj = stdobj.buffer
            is_bin = True
        else:
            fileobj = stdobj

        if check_readable:
            if not hasattr(fileobj, "peek"):
                fileobj = io.BufferedReader(fileobj)
            guess = FORMATS.guess_format_from_buffer(fileobj)
        else:
            validate = False
    elif file_type in (FileType.FILELIKE, FileType.BUFFER):
        fileobj = target
        use_system = False

        # determine mode of fileobj
        if hasattr(fileobj, "mode"):
            fileobj_mode = FileMode(target.mode)
        elif hasattr(fileobj, "readable"):
            access = ModeAccess.READWRITE
            # if fileobj.readable and fileobj.writable:
            #     access = ModeAccess.READWRITE
            # elif fileobj.writable:
            #     access = ModeAccess.WRITE
            # else:
            #     access = ModeAccess.READ
            fileobj_mode = FileMode(
                access=access, coding="t" if hasattr(fileobj, "encoding") else "b"
            )
        else:  # pragma: no-cover
            # TODO I don't think we can actually get here, but leaving for now.
            raise ValueError("Cannot determine file mode")

        # make sure modes are compatible
        if not (
            (mode.readable and fileobj_mode.readable)
            or (mode.writable and fileobj_mode.writable)
        ):
            raise ValueError(
                "mode {} and file mode {} are not compatible".format(mode, fileobj_mode)
            )

        # compression/decompression only possible for binary files
        is_bin = fileobj_mode.binary
        if not is_bin:
            if compression:
                raise ValueError(
                    "Cannot compress to/decompress from a text-mode " "file/buffer"
                )
            else:
                # noinspection PyUnusedLocal
                guess_format = False
        elif validate or guess_format:
            if mode.readable:
                if not hasattr(fileobj, "peek"):
                    fileobj = io.BufferedReader(fileobj)
                guess = FORMATS.guess_format_from_buffer(fileobj)
            elif hasattr(fileobj, "name") and isinstance(fileobj.name, str):
                guess = FORMATS.guess_compression_format(fileobj.name)
            else:
                raise ValueError(
                    "Could not guess compression format from {}".format(target)
                )
    elif file_type is FileType.URL:
        if not mode.readable:
            raise ValueError("URLs can only be opened in read mode")

        fileobj = open_url(target)
        if not fileobj:
            raise ValueError("Could not open URL {}".format(target))

        use_system = False
        name = get_url_file_name(fileobj, url_parts)

        # Get compression format if not specified
        if validate or guess_format:
            guess = FORMATS.guess_format_from_buffer(fileobj)
            # The following code is never used, unless there is some
            # scenario in which the file type cannot be guessed from
            # the header bytes. I'll leave this here for now but keep
            # it commented out until someone provides an example of
            # why it's necessary.
            # if guess is None and guess_format:
            #     # Check if the MIME type indicates that the file is
            #     # compressed
            #     mime = get_url_mime_type(fileobj)
            #     if mime:
            # TODO: look at this https://github.com/dbtsai/python-mimeparse
            # or similar for mime parsing
            #         guess = get_format_for_mime_type(mime)
            #     # Try to guess from the file name
            #     if not guess and name:
            #         guess = guess_file_format(name)
    elif file_type is FileType.LOCAL:
        if is_str:
            target = Path(target)
        if mode.readable:
            target = check_readable_file(target)
            if validate or guess_format:
                guess = FORMATS.guess_format_from_file_header(target)
        else:
            target = check_writable_file(target)
            # If overwrite=False, check that the file doesn't already exist
            if not overwrite and os.path.exists(target):
                raise ValueError("File already exists: {}".format(target))
            if validate or guess_format:
                guess = FORMATS.guess_compression_format(target)

    if validate and guess != compression:
        # TODO: this is to handle the case where the same extension can be used for
        # multiple compression formats, and we're writing a file so the format cannot
        # be detected from the header. Formats currently does not support an extension
        # being used with multiple formats. Currently bgzip is the only format that has
        # this issue.
        if not mode.readable and FORMATS.has_compatible_extension(compression, guess):
            pass
        else:
            raise ValueError(
                "Acutal compression format {} is not compatible with expected "
                "format {}".format(guess, compression)
            )
    elif guess:
        compression = guess
    elif compression is True:
        raise ValueError(f"Could not guess compression format from {target}")

    if compression:
        fmt = FORMATS.get_compression_format(str(compression))
        compression = fmt.name
        fileobj = fmt.open_file(
            fileobj or target, mode, use_system=use_system, **kwargs
        )
        is_std = False
    elif not fileobj:
        fileobj = open(target, mode.value, **kwargs)
    elif mode.text and is_bin and (is_std or file_type is FileType.FILELIKE):
        fileobj = io.TextIOWrapper(fileobj)
        fileobj.mode = mode.value

    if context_wrapper:
        if is_std:
            fileobj = StdWrapper(fileobj, compression=compression)
        elif file_type == FileType.BUFFER:
            fileobj = BufferWrapper(
                fileobj, buffer, compression=compression, close_fileobj=close_fileobj
            )
        else:
            fileobj = FileWrapper(
                fileobj,
                name=name,
                mode=mode,
                compression=compression,
                close_fileobj=close_fileobj,
            )

    return fileobj


@deprecated_str_to_path(0, "path")
def guess_file_format(path: PurePath) -> str:
    """Try to guess the file format, first from the extension, and then
    from the header bytes.

    Args:
        path: The path to the file

    Returns:
        The v format, or None if one could not be determined
    """
    fmt = FORMATS.guess_compression_format(path)
    if fmt is None and safe_check_readable_file(path):
        fmt = FORMATS.guess_format_from_file_header(path)
    return fmt


PopenStdParamsArg = Union[
    PopenStdArg, dict, Tuple[PopenStdArg, Union[ModeArg, dict]]
]  # pylint: disable=invalid-name


def popen(
    args: Union[str, Iterable],
    stdin: PopenStdParamsArg = None,
    stdout: PopenStdParamsArg = None,
    stderr: PopenStdParamsArg = None,
    shell: bool = False,
    **kwargs,
) -> Process:
    """Opens a subprocess, using xopen to open input/output streams.

    Args:
        args: argument string or tuple of arguments.
        stdin:
        stdout:
        stderr: file to use as stdin, PIPE to open a pipe, a
            dict to pass xopen args for a PIPE, a tuple of (path, mode) or a
            tuple of (path, dict), where the dict contains parameters to pass
            to xopen.
        shell: The 'shell' arg from `subprocess.Popen`.
        kwargs: additional arguments to `subprocess.Popen`.

    Returns:
        A Process object, which is a subclass of `subprocess.Popen`.
    """
    is_str = isinstance(args, str)
    if not is_str:
        args = [str(a) for a in args]
    if shell and not is_str:
        args = " ".join(args)
    elif not shell and is_str:
        args = shlex.split(str(args))
    std_args = {}

    # Open non-PIPE streams
    for name, arg, default_mode in zip(
        ("stdin", "stdout", "stderr"), (stdin, stdout, stderr), ("rb", "wb", "wb")
    ):
        if arg is None:
            kwargs[name] = None
            continue
        if isinstance(arg, tuple):
            path, path_args = arg
            if not isinstance(path_args, dict):
                path_args = dict(mdoe=path_args)
        elif isinstance(arg, dict):
            path = PIPE
            path_args = arg
        else:
            path = arg
            path_args = {}
        if path == PIPE and path_args:
            path_args["file_type"] = FileType.FILELIKE
            std_args[name] = path_args
        elif path not in (PIPE, None):
            path_args["use_system"] = False
            path_args["context_wrapper"] = True
            if "mode" not in path_args:
                path_args["mode"] = default_mode
            path = xopen(path, **path_args)
        kwargs[name] = path

    # add defaults for some Popen args
    kwargs["shell"] = shell
    if "executable" not in kwargs:
        kwargs["executable"] = os.environ.get("SHELL") if shell else None
    if "preexec_fn" not in kwargs:
        kwargs["preexec_fn"] = _prefunc

    # create process
    process = Process(args, **kwargs)

    # Wrap PIPE streams
    if std_args:
        process.wrap_pipes(**std_args)

    return process


def _prefunc():  # pragma: no-cover
    """Handle a SIGPIPE error in Popen (happens when calling a command that has
    pipes).
    """
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
