# -*- coding: utf-8 -*-
"""The main xphyle methods -- xopen, popen, and open_.
"""
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
import io
import os
import shlex
import signal
from subprocess import Popen, PIPE, TimeoutExpired
import sys
from xphyle.formats import FORMATS, THREADS
from xphyle.paths import (
    STDIN, STDOUT, STDERR, EXECUTABLE_CACHE,
    check_readable_file, check_writable_file, safe_check_readable_file)
from xphyle.progress import ITERABLE_PROGRESS, PROCESS_PROGRESS
from xphyle.types import (
    FileType, FileLikeInterface, FileLike, FileMode, ModeArg, ModeAccess,
    CompressionArg, EventType, EventTypeArg, PathOrFile, Callable, Container,
    Iterable, Iterator, Union, Sequence, Tuple, AnyStr, Any)
from xphyle.urls import parse_url, open_url, get_url_file_name

# pylint: disable=protected-access
from xphyle._version import get_versions
__version__ = get_versions()['version']
del get_versions

# Classes

class EventListener(metaclass=ABCMeta):
    """Base class for listener events that can be registered on a
    FileLikeWrapper.
    
    Args:
        kwargs: keyword arguments to pass through to ``execute``
    """
    def __init__(self, **kwargs):
        self.create_args = kwargs
    
    def __call__(self, wrapper: 'EventManager', **call_args) -> None:
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
    def execute(self, wrapper: 'EventManager', **kwargs) -> None:
        """Handle an event. This method must be implemented by subclasses.
        
        Args:
            wrapper: The :class:`EventManager` on which this event was
                registered.
            kwargs: A union of the keyword arguments passed to the constructor
                and the __call__ method.
        """
        pass


class EventManager(object):
    """Mixin type for classes that allow registering event listners.
    """
    def register_listener(
            self, event: EventTypeArg, listener: EventListener) -> None:
        """Register an event listener.
        
        Args:
            event: Event name (currently, only 'close' is recognized)
            listener: A listener object, which must be callable with a
                single argument -- this file wrapper.
        """
        if not hasattr(self, '_listeners'):
            object.__setattr__(self, '_listeners', defaultdict(lambda: []))
        if isinstance(event, str):
            event = EventType(event)
        self._listeners[event].append(listener)
    
    def _fire_listeners(self, event: EventType, **kwargs) -> None:
        """Fire :class:`FileEventListener`s associated with `event`.
        
        Args:
            event: The event type.
            kwargs: Additional arguments to pass to the listener.
        """
        if hasattr(self, '_listeners') and event in self._listeners:
            for listener in self._listeners[event]:
                listener(self, **kwargs)


class FileLikeWrapper(FileLikeInterface, EventManager):
    """Base class for wrappers around file-like objects. By default, method
    calls are forwarded to the file object. Adds the following:
    
    1. A simple event system by which registered listeners can respond to
    file events. Currently, 'close' is the only supported event
    2. Wraps file iterators in a progress bar (if configured)
    
    Args:
        fileobj: The file-like object to wrap.
        compression: Whether the wrapped file is compressed.
    """
    def __init__(self, fileobj: FileLike, compression: bool = False):
        object.__setattr__(self, '_fileobj', fileobj)
        object.__setattr__(self, 'compression', compression)
    
    def __getattr__(self, name: str) -> Any:
        return getattr(self._fileobj, name)
    
    def __next__(self) -> AnyStr:
        return next(iter(self))
    
    def __iter__(self) -> Iterator:
        if not hasattr(self, '_iterator'):
            itr = iter(ITERABLE_PROGRESS.wrap(self._fileobj, desc=self.name))
            setattr(self, '_iterator', itr)
        return self._iterator
    
    def __enter__(self) -> 'FileLikeWrapper':
        if self.closed:
            raise IOError("I/O operation on closed file.")
        return self
    
    def __exit__(self, exception_type, exception_value, traceback) -> None:
        self.close()
    
    def peek(self, size: int = 1) -> AnyStr:
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
        
        if hasattr(self._fileobj, 'peek'):
            # The underlying file has a peek() method
            peek = self._fileobj.peek(size)
            # I don't think the following is a valid state
            # if 't' in self._fileobj.mode:
            #     if isinstance(peek, 'bytes'):
            #         if hasattr(self._fileobj, 'encoding'):
            #             peek = peek_bytes.decode(self._fileobj.encoding)
            #         else:
            #             peek = peek_bytes.decode()
            if len(peek) > size:
                peek = peek[:size]
        elif hasattr(self._fileobj, 'seek'):
            # The underlying file has a seek() method
            curpos = self._fileobj.tell()
            try:
                peek = self._fileobj.read(size)
            finally:
                self._fileobj.seek(curpos)
        else:
            # TODO I don't think it's possible to get here, but leaving for now
            # pragma: no-cover
            raise IOError("Unpeekable file: {}".format(self.name))
        return peek
    
    def close(self) -> None:
        """Close the file, close an open iterator, and fire 'close' events to
        any listeners.
        """
        self._close()
        if hasattr(self, '_iterator'):
            delattr(self, '_iterator')
        self._fire_listeners(EventType.CLOSE)
    
    def _close(self) -> None:
        self._fileobj.close()


class FileWrapper(FileLikeWrapper):
    """Wrapper around a file object.
    
    Args:
        source: Path or file object.
        mode: File open mode.
        compression: Compression type.
        name: Use an alternative name for the file.
        kwargs: Additional arguments to pass to xopen.
    """
    def __init__(
            self, source: PathOrFile, mode: ModeArg = 'w',
            compression: CompressionArg = False, name: str = None,
            **kwargs):
        if isinstance(source, str):
            path = source
            source = xopen(source, mode=mode, compression=compression, **kwargs)
        elif name or not hasattr(source, 'name'):
            path = name
        else:
            path = source.name
        super().__init__(source, compression=compression)
        object.__setattr__(self, '_path', path)
        if name:
            object.__setattr__(self, 'name', name)
    
    @property
    def path(self):
        """The source path.
        """
        return getattr(self, '_path', None)

class BufferWrapper(FileWrapper):
    """Wrapper around a string/bytes buffer.
    
    Args:
        fileobj: The fileobj to wrap (the raw or wrapped buffer).
        buffer: The raw buffer.
        compression: Compression type.
    """
    def __init__(
            self, fileobj: PathOrFile, buffer: FileLike,
            compression: CompressionArg = False, name: str = None):
        super().__init__(fileobj, compression=compression, name=name)
        object.__setattr__(self, 'buffer', buffer)
    
    def getvalue(self) -> AnyStr:
        """Returns the contents of the buffer.
        """
        if hasattr(self, '_value'):
            return self._value
        else:
            return self.buffer.getvalue()
    
    def _close(self):
        if self.compression:
            self._fileobj.close()
            value = self.getvalue()
        else:
            value = self.getvalue()
            self._fileobj.close()
        object.__setattr__(self, '_value', value)


class StdWrapper(FileLikeWrapper):
    """Wrapper around stdin/stdout/stderr.
    
    Args:
        stream: The stream to wrap.
        compression: Compression type.
    """
    def __init__(self, stream: FileLike, compression: CompressionArg = False):
        super().__init__(stream, compression=compression)
        object.__setattr__(self, 'closed', False)
    
    def _close(self):
        self._fileobj.flush()
        self.closed = True # pylint: disable=attribute-defined-outside-init


PopenStdArg = Union[PathOrFile, int] # pylint: disable=invalid-name

class Process(Popen, FileLikeInterface, EventManager):
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
            self, *args, stdin: PopenStdArg = None, stdout: PopenStdArg = None,
            stderr: PopenStdArg = None, **kwargs):
        super().__init__(
            *args, stdin=stdin, stdout=stdout, stderr=stderr, **kwargs)
        # Construct a dict of name=(stream, wrapper, is_pipe) for std streams
        self._std = dict(
            (name, [stream, None, desc == PIPE])
            for name, desc, stream in zip(
                ('stdin', 'stdout', 'stderr'),
                (stdin, stdout, stderr),
                (self.stdin, self.stdout, self.stderr)))
        self._iterator = None
    
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
            args['validate'] = False
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
    
    def write(self, data: AnyStr) -> int:
        """Write `data` to stdin.
        
        Args:
            data: The data to write; must be bytes if stdin is a byte stream or
                string if stdin is a text stream.
        
        Returns:
            Number of bytes/characters written
        """
        self.get_writer().write(data)
    
    def get_writer(self) -> FileLike:
        """Returns the stream for writing to stdin.
        """
        stdin = self._std['stdin']
        return stdin[1] or stdin[0]
    
    def readable(self) -> bool:
        """Returns True if this Popen has stdout and/or stderr, otherwise False.
        """
        return self.get_reader() is not None
    
    def read(self, size: int = -1, which: str = None) -> AnyStr:
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
        if which in ('stdout', None):
            std = self._std['stdout']
        else:
            std = self._std['stderr']
        return std[1] or std[0]
    
    def get_readers(self):
        """Returns (stdout, stderr) tuple.
        """
        return tuple(self.get_reader(std) for std in ('stdout', 'stderr'))
    
    def communicate(self, inp: AnyStr = None, timeout: int = None):
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
        self.close(timeout, True, True)
        return (self.stdout, self.stderr)
    
    def seekable(self) -> bool: # pylint: disable=no-self-use
        """Implementing file interface; returns False.
        """
        return False
    
    def flush(self) -> None:
        """Flushes stdin if there is one.
        """
        if self.writable():
            self.get_writer().flush()
    
    def __next__(self) -> AnyStr:
        """Returns the next line from the iterator.
        """
        return next(iter(self))
    
    def __iter__(self) -> Iterable[AnyStr]:
        """Returns the currently open iterator. If one isn't open,
        :method:`_reader(which=None)` is used to create one.
        """
        if not self._iterator:
            self._iterator = iter(ITERABLE_PROGRESS.wrap(
                self.get_reader(), desc=str(self)))
        return self._iterator
    
    def __exit__(self, exception_type, exception_value, traceback) -> None: # pylint: disable=arguments-differ
        """On exit from a context manager, calls
        :method:`close(raise_on_error=True, record_output=True)`.
        """
        if not self.closed:
            self.close(raise_on_error=True, record_output=True)
    
    def __del__(self) -> None:
        if not self.closed:
            try:
                self.close(1, False, False, True)
            except IOError: # pragma: no-cover
                pass
        super().__del__()
    
    @property
    def closed(self):
        """Whether the Process has been closed.
        """
        return self._std is None
    
    def close(
            self, timeout: int = None, raise_on_error: bool = False,
            record_output: bool = False, terminate: bool = False) -> int:
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
            :attribute:`stdout`\:attribute:`stderr`. Otherwise the data is lost.
        
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
        
        stdin = self._std['stdin']
        if stdin and stdin[0]:
            if stdin[1]:
                stdin[1].close()
            try:
                stdin[0].close()
            except IOError: # pragma: no-cover
                pass
            self._std['stdin'] = None
        
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
        
        self.stdout = _close_reader('stdout')
        self.stderr = _close_reader('stderr')
        self._iterator = None
        self._std = None
        
        if raise_on_error:
            self.check_valid_returncode()
        
        self._fire_listeners(EventType.CLOSE, returncode=self.returncode)
        
        return self.returncode
    
    def check_valid_returncode(self, valid : Container[int] = (
            0, None, signal.SIGPIPE, signal.SIGPIPE + 128)):
        """Check that the returncodes does not have a value associated with
        an error state.
        
        Raises:
            IOError if :attribute:`returncode` is associated with an error
            state.
        """
        if self.returncode not in valid:
            raise IOError("Process existed with return code {}".format(
                self.returncode))


# Methods

DEFAULTS = dict(
    xopen_context_wrapper = False
)

def configure(
        default_xopen_context_wrapper: bool = None,
        progress: bool = None,
        progress_wrapper: Callable[..., Iterable] = None,
        system_progress: bool = None,
        system_progress_wrapper: Union[str, Sequence[str]] = None,
        threads: Union[int, bool] = None,
        executable_path: Union[str, Sequence[str]] = None) -> None:
    """Conifgure xphyle.
    
    Args:
        default_xopen_context_wrapper: Whether to wrap files opened by
            :method:`xopen` in :class:`FileLikeWrapper`s by default (when
            `xopen`'s context_wrapper parameter is `None`.
        progress: Whether to wrap long-running operations with a progress bar
        progres_wrapper: Specify a non-default progress wrapper
        system_progress: Whether to use progress bars for system-level
        system_progress_wrapper: Specify a non-default system progress wrapper
        threads: The number of threads that can be used by compression formats
            that support parallel compression/decompression. Set to None or a
            number < 1 to automatically initalize to the number of cores on
            the local machine.
        executable_paths: List of paths where xphyle should look for system
            executables. These will be searched before the default system path.
    """
    if default_xopen_context_wrapper is not None:
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
# OpenArg = Union[PathOrFile, Type[Union[bytes, str]]] # pylint: disable=invalid-name

@contextmanager
def open_(
        path_or_file, mode: ModeArg = None, errors: bool = True,
        wrap_fileobj: bool = True, **kwargs) -> FileLike:
    """Context manager that frees you from checking if an argument is a path
    or a file object. Calls ``xopen`` to open files.
    
    Args:
        path_or_file: A relative or absolute path, a URL, a system command, a
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
    if path_or_file is None:
        if errors:
            raise ValueError("'path_or_file' cannot be None")
        else:
            yield None
    else:
        is_fileobj = not (
            isinstance(path_or_file, str) or
            path_or_file in (str, bytes))
        if not wrap_fileobj:
            if is_fileobj:
                yield path_or_file
            else:
                raise ValueError(
                    "'wrap_fileobj must be True if 'path' is not file-like")
        else:
            if not is_fileobj:
                kwargs['context_wrapper'] = True
            try:
                with xopen(path_or_file, mode, **kwargs) as fileobj:
                    yield fileobj
            except IOError:
                if errors:
                    raise
                else:
                    yield None


def xopen(
        path, mode: ModeArg = None,
        compression: CompressionArg = None, use_system: bool = True,
        context_wrapper: bool = None, file_type: FileType = None,
        validate: bool = True, **kwargs) -> FileLike:
    """
    Replacement for the builtin `open` function that can also open URLs and
    subprocessess, and automatically handles compressed files.
    
    Args:
        path: A relative or absolute path, a URL, a system command, a
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
        context_wrapper: If True, the file is wrapped in a `FileLikeWrapper`
            subclass before returning (`FileWrapper` for files/URLs,
            `StdWrapper` for STDIN/STDOUT/STDERR). If None, the default value
            (set using :method:`configure`) is used.
        file_type: a FileType; explicitly specify the file type. By default the
            file type is detected, but auto-detection might make mistakes, e.g.
            a local file contains a colon (':') in the name.
        validate: Ensure that the user-specified compression format matches the
            format guessed from the file extension or magic bytes.
        kwargs: Additional keyword arguments to pass to ``open``.
    
    `path` is interpreted as follows:
        * If starts with '|', it is assumed to be a system command
        * If a file-like object, it is used as-is
        * If one of STDIN, STDOUT, STDERR, the appropriate `sys` stream is used
        * If parseable by `xphyle.urls.parse_url()`, it is assumed to be a URL
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
    # Whether the file object is stdin/stdout/stderr
    is_std = path in (STDIN, STDOUT, STDERR)
    # Whether path is a string or fileobj
    is_str = isinstance(path, str)
    # Whether path is a class indicating a buffer type
    is_buffer = path in (str, bytes)
    
    if not file_type:
        if is_std:
            file_type = FileType.STDIO
        elif is_buffer:
            file_type = FileType.BUFFER
        elif not is_str:
            file_type = FileType.FILELIKE
        elif path.startswith('|'):
            file_type = FileType.PROCESS
    elif (is_str == (file_type is FileType.FILELIKE) or
          is_std != (file_type is FileType.STDIO) or
          is_buffer != (file_type is FileType.BUFFER)):
        raise ValueError("file_type = {} does not match path {}".format(
            file_type, path))
    
    if file_type in (FileType.URL, None):
        url_parts = parse_url(path)
        if not file_type:
            file_type = FileType.URL if url_parts else FileType.LOCAL
        elif not url_parts:
            raise ValueError("{} is not a valid URL".format(path))
    
    if not mode:
        # set to default
        if not is_buffer:
            mode = FileMode()
        elif path == str:
            mode = FileMode('wt')
        else:
            mode = FileMode('wb')
    elif isinstance(mode, str):
        # pylint: disable=redefined-variable-type
        if ('U' in mode
                and 'newline' in kwargs and
                kwargs['newline'] is not None):
            raise ValueError(
                "newline={} not compatible with universal newlines ('U') "
                "mode".format(kwargs['newline']))
        mode = FileMode(mode)
    
    if context_wrapper is None:
        context_wrapper = DEFAULTS['xopen_context_wrapper']
    
    # Return early if opening a process
    if file_type is FileType.PROCESS:
        if path.startswith('|'):
            path = path[1:]
        popen_args = dict(kwargs)
        for std in ('stdin', 'stdout', 'stderr'):
            popen_args[std] = PIPE
        if mode.writable:
            if compression is True:
                raise ValueError(
                    "Can determine compression automatically when writing to "
                    "process stdin")
            elif compression is None:
                compression = False
            target = 'stdin'
        else:
            target = 'stdout'
        popen_args[target] = dict(
            mode=mode, compression=compression, validate=validate,
            context_wrapper=context_wrapper)
        return popen(path, **popen_args)
    
    if file_type is FileType.BUFFER:
        if path == str:
            path = io.StringIO()
        elif path == bytes:
            path = io.BytesIO()
        if context_wrapper:
            buffer = path
        if not mode.readable:
            if compression is True:
                raise ValueError(
                    "Cannot guess compression for a write-only buffer")
            elif compression is None:
                compression = False
            validate = False
    
    # The file handle we will open
    fileobj = None
    # The name to use for the file
    name = None
    # Guessed compression type, if compression in (None, True)
    guess = None
    # Whether to try and guess file format
    guess_format = compression in (None, True)
    # Whether to validate that the actually compression format matches expected
    validate = validate and compression and not guess_format
    
    if file_type is FileType.STDIO:
        use_system = False
        if path == STDERR:
            if not mode.writable:
                raise ValueError("Mode must be writable for stderr")
            fileobj = sys.stderr
        else:
            fileobj = sys.stdin if mode.readable else sys.stdout
        # get the underlying binary stream
        fileobj = fileobj.buffer
        if mode.readable and (validate or guess_format):
            if not hasattr(fileobj, 'peek'):
                fileobj = io.BufferedReader(fileobj)
            guess = FORMATS.guess_format_from_buffer(fileobj)
        else:
            validate = False
    elif file_type in (FileType.FILELIKE, FileType.BUFFER):
        fileobj = path
        use_system = False
        
        # determine mode of fileobj
        if hasattr(fileobj, 'mode'):
            fileobj_mode = FileMode(path.mode)
        elif hasattr(fileobj, 'readable'):
            access = ModeAccess.READWRITE
            # if fileobj.readable and fileobj.writable:
            #     access = ModeAccess.READWRITE
            # elif fileobj.writable:
            #     access = ModeAccess.WRITE
            # else:
            #     access = ModeAccess.READ
            fileobj_mode = FileMode(
                access=access,
                coding='t' if hasattr(fileobj, 'encoding') else 'b')
        else:
            # TODO I don't think we can actually get here, but leaving for now.
            # pragma: no-cover
            raise ValueError("Cannot determine file mode")
        
        # make sure modes are compatible
        if not ((mode.readable and fileobj_mode.readable) or
                (mode.writable and fileobj_mode.writable)):
            raise ValueError(
                "mode {} and file mode {} are not compatible".format(
                    mode, fileobj_mode))
        
        # compression/decompression only possible for binary files
        if fileobj_mode.text:
            if compression:
                raise ValueError(
                    "Cannot compress to/decompress from a text-mode "
                    "file/buffer")
            else:
                guess_format = False
        elif validate or guess_format:
            if mode.readable:
                if not hasattr(fileobj, 'peek'):
                    fileobj = io.BufferedReader(fileobj)
                guess = FORMATS.guess_format_from_buffer(fileobj)
            elif hasattr(fileobj, 'name') and isinstance(fileobj.name, str):
                guess = FORMATS.guess_compression_format(fileobj.name)
            else:
                raise ValueError(
                    "Could not guess compression format from {}".format(path))
    elif file_type is FileType.URL:
        if not mode.readable:
            raise ValueError("URLs can only be opened in read mode")
        
        fileobj = open_url(path)
        if not fileobj:
            raise ValueError("Could not open URL {}".format(path))
        
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
        if mode.readable:
            path = check_readable_file(path)
            if validate or guess_format:
                guess = FORMATS.guess_format_from_file_header(path)
        else:
            path = check_writable_file(path)
            if validate or guess_format:
                guess = FORMATS.guess_compression_format(path)
    
    if validate and guess != compression:
        raise ValueError(
            "Acutal compression format {} does not match expected "
            "format {}".format(guess, compression))
    elif guess:
        compression = guess
    elif compression is True:
        raise ValueError(
            "Could not guess compression format from {}".format(path))
    
    if compression:
        fmt = FORMATS.get_compression_format(compression)
        compression = fmt.name
        fileobj = fmt.open_file(
            fileobj or path, mode, use_system=use_system, **kwargs)
        is_std = False
    elif not fileobj:
        fileobj = open(path, mode.value, **kwargs)
    elif mode.text and (is_std or (
            file_type is FileType.FILELIKE and not fileobj_mode.text)):
        fileobj = io.TextIOWrapper(fileobj)
        fileobj.mode = mode.value
    
    if context_wrapper:
        if is_std:
            fileobj = StdWrapper(fileobj, compression=compression)
        elif file_type == FileType.BUFFER:
            fileobj = BufferWrapper(fileobj, buffer, compression=compression)
        else:
            fileobj = FileWrapper(fileobj, name=name, compression=compression)
    
    return fileobj

def guess_file_format(path: str) -> str:
    """Try to guess the file format, first from the extension, and then
    from the header bytes.
    
    Args:
        path: The path to the file
    
    Returns:
        The v format, or None if one could not be determined
    """
    if path in (STDOUT, STDERR):
        raise ValueError("Cannot guess format from {}".format(path))
    fmt = FORMATS.guess_compression_format(path)
    if fmt is None and safe_check_readable_file(path):
        fmt = FORMATS.guess_format_from_file_header(path)
    return fmt

PopenStdParamsArg = Union[
    PopenStdArg, dict, Tuple[PopenStdArg, Union[ModeArg, dict]]] # pylint: disable=invalid-name

def popen(
        args: Union[str, Iterable], stdin: PopenStdParamsArg = None,
        stdout: PopenStdParamsArg = None, stderr: PopenStdParamsArg = None,
        shell: bool = False, **kwargs) -> Process:
    """Opens a subprocess, using xopen to open input/output streams.
    
    Args:
        args: argument string or tuple of arguments.
        stdin, stdout, stderr: file to use as stdin, PIPE to open a pipe, a
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
        args = ' '.join(args)
    elif not shell and is_str:
        args = shlex.split(args)
    std_args = {}
    
    # Open non-PIPE streams
    for name, arg, default_mode in zip(
            ('stdin', 'stdout', 'stderr'),
            (stdin, stdout, stderr),
            ('rb', 'wb', 'wb')):
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
            path_args['file_type'] = FileType.FILELIKE
            std_args[name] = path_args
        elif path not in (PIPE, None):
            path_args['use_system'] = False
            path_args['context_wrapper'] = True
            if 'mode' not in path_args:
                path_args['mode'] = default_mode
            path = xopen(path, **path_args)
        kwargs[name] = path
    
    # add defaults for some Popen args
    kwargs['shell'] = shell
    if 'executable' not in kwargs:
        kwargs['executable'] = os.environ.get('SHELL') if shell else None
    if 'preexec_fn' not in kwargs:
        kwargs['preexec_fn'] = _prefunc
    
    # create process
    process = Process(args, **kwargs)
    
    # Wrap PIPE streams
    if std_args:
        process.wrap_pipes(**std_args)
    
    return process

def _prefunc(): # pragma: no-cover
    """Handle a SIGPIPE error in Popen (happens when calling a command that has
    pipes).
    """
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
