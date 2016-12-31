# -*- coding: utf-8 -*-
"""The main xphyle methods -- open_ and xopen.
"""
from collections import defaultdict
from contextlib import contextmanager
import copy
import io
import os
import sys
from xphyle.types import (Callable, Iterable, Union, Sequence, FileLike,
                          PathOrFile, Any)

from xphyle.formats import FORMATS, THREADS
from xphyle.paths import (
    STDIN, STDOUT, STDERR, EXECUTABLE_CACHE,
    check_readable_file, check_writeable_file, safe_check_readable_file)
from xphyle.progress import ITERABLE_PROGRESS, PROCESS_PROGRESS
from xphyle.urls import parse_url, open_url, get_url_file_name

# pylint: disable=protected-access
from xphyle._version import get_versions
__version__ = get_versions()['version']
del get_versions

def configure(progress: bool = None,
              progress_wrapper: Callable[..., Iterable] = None,
              system_progress: bool = None,
              system_progress_wrapper: Union[str, Sequence[str]] = None,
              threads: Union[int, bool] = None,
              executable_path: Union[str, Sequence[str]] = None) -> None:
    """Conifgure xphyle.
    
    Args:
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
    if progress is not None:
        ITERABLE_PROGRESS.update(progress, progress_wrapper)
    if system_progress is not None:
        PROCESS_PROGRESS.update(system_progress, system_progress_wrapper)
    if threads is not None:
        THREADS.update(threads)
    if executable_path:
        EXECUTABLE_CACHE.add_search_path(executable_path)

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

@contextmanager
def open_(path_or_file, mode: str = 'r', errors: bool = True, **kwargs
         ) -> FileLike:
    """Context manager that frees you from checking if an argument is a path
    or a file object. Calls ``xopen`` to open files.
    
    Args:
        path_or_file: A path or file-like object
        mode: The file open mode
        errors: Whether to raise an error if there is a problem opening the
            file. If False, yields None when there is an error.
        kwargs: Additional args to pass through to xopen (if ``f`` is a path)
    
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
    if isinstance(path_or_file, str):
        kwargs['context_wrapper'] = True
        try:
            with xopen(path_or_file, mode, **kwargs) as fileobj:
                yield fileobj
        except IOError:
            if errors:
                raise
            else:
                yield None
    elif path_or_file is None and errors:
        raise ValueError("'path_or_file' cannot be None")
    else:
        yield path_or_file

def xopen(path: str, mode: str = 'r', compression: Union[bool, str] = None,
          use_system: bool = True, context_wrapper: bool = True,
          **kwargs) -> FileLike:
    """
    Replacement for the `open` function that automatically handles
    compressed files. If `use_system==True` and the file is compressed,
    the file is opened with a pipe to the system-level compression program
    (e.g. ``gzip`` for '.gz' files) if possible, otherwise the corresponding
    python library is used.
    
    Returns ``sys.stdout`` or ``sys.stdin`` if ``path`` is '-' (for
    modes 'w' and 'r' respectively), and ``sys.stderr`` if ``path``
    is '_'.
    
    Args:
        path: A relative or absolute path. Must be a string. If
            you have a situation you want to automatically handle either
            a path or a file object, use the ``open_`` wrapper instead.
        mode: Some combination of the open mode ('r', 'w', 'a', or 'x')
            and the format ('b' or 't'). If the later is not given, 't'
            is used by default.
        compression: If None or True, compression type (if any) will be
            determined automatically. If False, no attempt will be made to
            determine compression type. Otherwise this must specify the
            compression type (e.g. 'gz'). See `xphyle.compression` for
            details. Note that compression will *not* be guessed for
            '-' (stdin).
        use_system: Whether to attempt to use system-level compression
            programs.
        context_wrapper: If True and ``path`` == '-' or '_', returns
            a ContextManager (i.e. usable with ``with``) that wraps the
            system stream and is no-op on close.
        kwargs: Additional keyword arguments to pass to ``open``.
    
    Returns:
        An opened file-like object.
    
    Raises:
        ValueError if:
            * ``compression==True`` and compression format cannot be
            determined
            * the specified compression format is invalid
            * ``validate==True`` and the specified compression format is not the
                acutal format of the file
            * the path or mode are invalid
    """
    # pylint: disable=redefined-variable-type
    if not isinstance(path, str):
        raise ValueError("'path' must be a string")
    if not any(m in mode for m in ('r','w','a','x')):
        raise ValueError("'mode' must contain one of (r,w,a,x)")
    if 'U' in mode:
        if 'newline' in kwargs and kwargs['newline'] is not None:
            raise ValueError("newline={} not compatible with universal newlines "
                             "('U') mode".format(kwargs['newline']))
        mode = mode.replace('U','')
    if len(mode) == 1:
        mode += 't'
    elif not any(f in mode for f in ('b', 't')):
        raise ValueError("'mode' must contain one of (b,t)")
    
    # The file handle we will open
    fileobj = None
    # Whether to try and guess file format
    guess_format = compression in (None, True)
    # Whether to validate that the actually compression format matches expected
    validate = compression and not guess_format
    # Guessed compression type, if compression in (None, True)
    guess = None
    # Whether the file object is stdin/stdout/stderr
    is_std = path in (STDIN, STDOUT, STDERR)
    # The name to use for the file
    name = None
    
    # standard input and standard output handling
    if is_std:
        use_system = False
        if path == STDERR:
            assert 'r' not in mode
            fileobj = sys.stderr
        else:
            fileobj = sys.stdin if 'r' in mode else sys.stdout
        # get the underlying binary stream
        fileobj = fileobj.buffer
        if 'r' in mode and (validate or guess_format):
            if not hasattr(fileobj, 'peek'):
                fileobj = io.BufferedReader(fileobj)
            guess = FORMATS.guess_format_from_buffer(fileobj)
        else:
            validate = False
        
    else:
        # URL handling
        url_parts = parse_url(path)
        if url_parts:
            if 'r' not in mode:
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
                #         guess = get_format_for_mime_type(mime)
                #     # Try to guess from the file name
                #     if not guess and name:
                #         guess = guess_file_format(name)
        
        # Local file handling
        else:
            if 'r' in mode:
                path = check_readable_file(path)
                if validate or guess_format:
                    guess = FORMATS.guess_format_from_file_header(path)
            else:
                path = check_writeable_file(path)
                if validate or guess_format:
                    guess = FORMATS.guess_compression_format(path)
    
    if validate and guess != compression:
        raise ValueError("Acutal compression format {} does not match expected "
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
        fileobj = open(path, mode, **kwargs)
    elif is_std and 't' in mode:
        fileobj = io.TextIOWrapper(fileobj)
    
    if context_wrapper:
        wrapper_class = StdWrapper if is_std else FileWrapper
        fileobj = wrapper_class(fileobj, name=name, compression=compression)
    
    return fileobj

class FileEventListener(object):
    """Base class for listener events that can be registered on a FileWrapper.
    
    Args:
        args: positional arguments to pass through to ``execute``
        kwargs: keyword arguments to pass through to ``execute``
    """
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
    
    def __call__(self, file_wrapper):
        self.execute(file_wrapper._path, *self.args, **self.kwargs)
    
    def execute(self, path: str, *args, **kwargs) -> None:
        """Handle an event.
        
        Args:
            path: The path to the file that generated the event
        """
        raise NotImplementedError()

class Wrapper(object):
    """Base class for wrappers around file-like objects. Adds the following:
    
    1. A simple event system by which registered listeners can respond to
    file events. Currently, 'close' is the only supported event
    2. Wraps file iterators in a progress bar (if configured)
    
    Args:
        fileobj: The file-like object to wrap
    """
    def __init__(self, fileobj: FileLike, compression: bool = False):
        object.__setattr__(self, '_fileobj', fileobj)
        object.__setattr__(self, 'compression', compression)
        object.__setattr__(self, '_listeners', defaultdict(lambda: []))
    
    def __getattr__(self, name: str) -> Any:
        return getattr(self._fileobj, name)
    
    def __next__(self):
        return next(iter(self))
    
    def __iter__(self):
        if not hasattr(self, '_iterator'):
            itr = iter(ITERABLE_PROGRESS.wrap(self._fileobj, desc=self.name))
            setattr(self, '_iterator', itr)
        return self._iterator
    
    def __enter__(self):
        if self.closed:
            raise IOError("I/O operation on closed file.")
        return self
    
    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
    
    def close(self) -> None:
        """Close the file, close an open iterator, and fire 'close' events to
        any listeners.
        """
        self._close()
        if hasattr(self, '_iterator'):
            delattr(self, '_iterator')
        if 'close' in self._listeners:
            for listener in self._listeners['close']:
                listener(self)
    
    def _close(self):
        self._fileobj.close()

    def register_listener(self, event: str, listener: FileEventListener
                         ) -> None:
        """Register an event listener.
        
        Args:
            event: Event name (currently, only 'close' is recognized)
            listener: A listener object, which must be callable with a
                single argument -- this file wrapper.
        """
        self._listeners[event].append(listener)

class FileWrapper(Wrapper):
    """Wrapper around a file object.
    
    Args:
        source: Path or file object
        mode: File open mode
        compression: Compression type
        name: Use an alternative name for the file
        kwargs: Additional arguments to pass to xopen
    """
    def __init__(self, source: PathOrFile, mode: str = 'w',
                 compression: Union[bool, str] = False,
                 name: str = None, **kwargs):
        if isinstance(source, str):
            path = source
            source = xopen(source, mode=mode, compression=compression, **kwargs)
        elif name and not hasattr(source, 'name'):
            path = name
        else:
            path = source.name
        super(FileWrapper, self).__init__(source, compression=compression)
        object.__setattr__(self, '_path', path)
        if name:
            object.__setattr__(self, 'name', name)

class StdWrapper(Wrapper):
    """EventWrapper around stdin/stdout/stderr.
    
    Args:
        stream: The stream to wrap
        compression: Compression type
        name: An alternative name to use for the stream
    """
    def __init__(self, stream, compression: Union[bool, str] = False,
                 name: str = None):
        super(StdWrapper, self).__init__(stream, compression=compression)
        object.__setattr__(self, 'closed', False)
        if name:
            object.__setattr__(self, 'name', name)
    
    def _close(self):
        self._fileobj.flush()
        self.closed = True # pylint: disable=attribute-defined-outside-init
