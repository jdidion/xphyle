# -*- coding: utf-8 -*-
"""The main xphyle methods -- open_ and xopen.
"""
from collections import defaultdict
from contextlib import contextmanager
import copy
import os
import sys

from xphyle.formats import *
from xphyle.urls import *
from xphyle.paths import *
import xphyle.progress

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

def configure(progress=True, system_progress=True, threads=1):
    """Conifgure xphyle.
    
    Args:
        progress: Whether to wrap long-running operations with a progress bar.
            If this is callable, it will be called to obtain the wrapped
            iterable.
        system_progress: Whether to use progress bars for system-level
            operations. If this is a string, it will be used as the command
            for producing the progress bar; pv is used by default.
        threads: The number of threads that can be used by compression formats
            that support parallel compression/decompression. Set to None or a
            number < 1 to automatically initalize to the number of cores on
            the local machine.
    """
    xphyle.progress.wrapper = progress
    xphyle.progress.system_wrapper = system_progress
    xphyle.formats.threads = threads

# Guess file format

def guess_file_format(path : 'str') -> 'str':
    """Try to guess the file format, first from the extension, and then
    from the header bytes.
    
    Args:
        path: The path to the file
    
    Returns:
        The v format, or None if one could not be determined
    """
    if path in (STDOUT, STDERR):
        raise ValueError("Cannot guess format from {}".format(path))
    fmt = guess_compression_format(path)
    if fmt is None and safe_check_readable_file(path):
        fmt = guess_format_from_header(path)
    return fmt

# Opening files

@contextmanager
def open_(f, mode : 'str' = 'r', **kwargs):
    """Context manager that frees you from checking if an argument is a path
    or a file object. Calls ``xopen`` to open files.
    
    Args:
        f: A path or file-like object
        kwargs: Additional args to pass through to xopen (if ``f`` is a path)
    
    Yields:
        A file-like object
    
    Examples:
        with open_('myfile') as infile:
            print(next(infile))
      
        fh = open('myfile')
        with open_(fh) as infile:
            print(next(infile))
    """
    if isinstance(f, str):
        kwargs['context_wrapper'] = True
        with xopen(f, mode, **kwargs) as fp:
            yield fp
    else:
        yield f

def xopen(path : 'str', mode : 'str' = 'r', compression : 'bool|str' = None,
          use_system : 'bool' = True, context_wrapper : 'bool' = True,
          **kwargs) -> 'file':
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
            * the path or mode are invalid
    """
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
    fh = None
    # The wrapper to use if context_wrapper is True
    wrapper = FileWrapper
    
    # standard input and standard output handling
    if path in (STDOUT, STDERR):
        if path == STDERR:
            assert 'r' not in mode
            fh = sys.stderr
        else:
            fh = sys.stdin if 'r' in mode else sys.stdout
        if 'b' in mode:
            fh = fh.buffer
        if compression:
            use_system = False
            if 't' in mode:
                fh = fh.buffer
        elif context_wrapper:
            wrapper = StreamWrapper
    
    else:
        # URL handling
        url_parts = parse_url(path)
        if url_parts:
            if 'r' not in mode:
                raise ValueError("URLs can only be opened in read mode")
            
            fh = open_url(path)
            if not fh: # pragma: no cover
                raise ValueError("Could not open URL {}".format(path))
            
            wrapper = StreamWrapper
            use_system = False
            
            # Get compression format if not specified
            if compression in (None, True):
                guess = None
                # Check if the MIME type indicates that the file is compressed
                mime = get_url_mime_type(fh)
                if mime:
                    guess = get_format_for_mime_type(mime)
                # Try to guess from the file name
                if not guess:
                    name = get_url_file_name(fh, url_parts)
                    if name:
                        guess = guess_file_format(name)
                if guess:
                    compression = guess
    
        # Local file handling
        else:
            if 'r' in mode:
                path = check_readable_file(path)
            else:
                path = check_writeable_file(path)
            
            if compression in (None, True):
                guess = guess_file_format(path)
                if guess:
                    compression = guess
        
    if compression is True:
        raise ValueError(
            "Could not guess compression format from {}".format(path))

    if compression:
        fmt = get_compression_format(compression)
        fh = fmt.open_file(fh or path, mode, use_system=use_system, **kwargs)
    elif not fh:
        fh = open(path, mode, **kwargs)
        
    if context_wrapper:
        fh = wrapper(fh)
    
    return fh

# File wrapper

class FileWrapper(object):
    """Wrapper around a file object that adds two features:
    
    1. An event system by which registered listeners can respond to file events.
    Currently, 'close' is the only supported event.
    2. Wraps a file iterator in a progress bar (if configured)
    
    Args:
        source: Path or file object
        mode: File open mode
        kwargs: Additional arguments to pass to xopen
    """
    __slots__ = ['_file', '_path', '_listeners']
    
    def __init__(self, source, mode='w', **kwargs):
        if isinstance(source, str):
            path = source
            source = xopen(source, mode=mode, **kwargs)
        else:
            path = source.name
        object.__setattr__(self, '_file', source)
        object.__setattr__(self, '_path', path)
        object.__setattr__(self, '_listeners', defaultdict(lambda: []))
    
    def __getattr__(self, name):
        return getattr(self._file, name)
    
    def __iter__(self):
        return iter(xphyle.progress.wrap(self._file, desc=self._path))
    
    def __enter__(self):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self
    
    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
    
    def register_listener(self, event : 'str', listener):
        """Register an event listener.
        
        Args:
            event: Event name (currently, only 'close' is recognized)
            listener: A listener object, which must be callable with a
                single argument -- this file wrapper.
        """
        self._listeners[event].append(listener)
    
    def close(self):
        self._file.close()
        if 'close' in self._listeners:
            for listener in self._listeners['close']:
                listener(self)

class FileEventListener(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
    
    def __call__(self, file_wrapper):
        self.execute(file_wrapper._path, *self.args, **self.kwargs)

class StreamWrapper(object):
    """Wrapper around a stream (such as stdout) that implements the
    ContextManager operations and wraps iterators in a progress bar
    (if configured).
    
    Args:
        stream: The stream to wrap
    """
    __slots__ = ['_stream']
    
    def __init__(self, stream, name=None):
        object.__setattr__(self, '_stream', stream)
    
    def __getattr__(self, name):
        return getattr(self._stream, name)
    
    def __iter__(self):
        try:
            name = self._stream.name
        except:
            name = None
        return iter(xphyle.progress.wrap(self._stream, desc=name))
    
    def __enter__(self):
        return self
    
    def __exit__(self, exception_type, exception_value, traceback):
        self._stream.flush()
