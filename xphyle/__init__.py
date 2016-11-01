# -*- coding: utf-8 -*-
"""The main xphyle methods -- open_ and xopen.
"""
from contextlib import contextmanager
import os
import sys
from xphyle.formats import *
from xphyle.paths import *
import xphyle.progress

def configure(progress=True, system_progress=True):
    """Conifgure xphyle.
    
    Args:
        progress: Whether to wrap long-running operations with a progress bar.
            If this is callable, it will be called to obtain the wrapped
            iterable.
        system_progress: Whether to use progress bars for system-level
            operations. If this is a string, it will be used as the command
            for producing the progress bar; pv is used by default.
    """
    xphyle.progress.wrapper = progress
    xphyle.progress.system_wrapper = system_progress

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
          use_system : 'bool' = True, context_wrapper : 'bool' = False,
          **kwargs):
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
            if compression == True:
                raise ValueError("Compression can not be determined "
                                 "automatically from stdin")
            if 't' in mode:
                fh = fh.buffer
            fmt = get_compression_format(compression)
            fh = fmt.open_file_python(fh, mode, **kwargs)
        elif context_wrapper:
            class StdWrapper(object):
                def __init__(self, fh):
                    self.fh = fh
                def __enter__(self):
                    return self.fh
                def __exit__(self, exception_type, exception_value, traceback):
                    pass
            fh = StdWrapper(fh)
        return fh
    
    if 'r' in mode:
        path = check_readable_file(path)
    else:
        path = check_writeable_file(path)
    
    if compression in (None, True):
        guess = guess_file_format(path)
        if guess is not None:
            compression = guess
        elif compression == True:
            raise ValueError(
                "Could not guess compression format from {}".format(path))
        else:
            compression = False
    
    if compression:
        fmt = get_compression_format(compression)
        return fmt.open_file(path, mode, use_system=use_system, **kwargs)
    
    return open(path, mode, **kwargs)
