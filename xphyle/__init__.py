# -*- coding: utf-8 -*-
"""A collection of convenience methods for opening, reading, writing, and
otherwise managing files.
"""
from contextlib import contextmanager
import pickle
import csv
import importlib
import io
import os
import shutil
import sys
import tempfile

from xphyle.formats import *
from xphyle.paths import *

# Guess file format

def guess_file_format(path : 'str') -> 'str':
    """Try to guess the file format, first from the extension, and then
    from the header bytes.
    
    Args:
        path: The path to the file
    
    Returns:
        The compression format, or None if one could not be determined
    """
    if path in (STDOUT, STDERR):
        raise ValueError("Cannot guess format from {}".format(path))
    fmt = guess_compression_format(path)
    if fmt is None and safe_check_readable_file(path):
        fmt = guess_format_from_header(path)
    return fmt

# Opening files

@contextmanager
def open_(f, **kwargs):
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
        with xopen(f, **kwargs) as fp:
            yield fp
    else:
        yield f

def xopen(path : 'str', mode : 'str' ='r', compression : 'bool|str' = None,
          use_system : 'bool' = True, context_wrapper : 'bool' = False,
          resolve : 'int,>=0,<=2' = 2, **kwargs):
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
        resolve: By default, filename is fully resolved and checked for
          accessibility (2). Set to 1 to resolve but not check, and 0 to
          perform no resolution at all.
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
        mode = mode.replace('U','')
        kwargs['newline'] = None
    if len(mode) == 1:
        mode += 't'
    elif not any(f in mode for f in ('b', 't')):
        raise ValueError("'mode' must contain one of (b,t)")
    
    # standard input and standard output handling
    if path in (STDOUT, STDERR):
        if path == STDERR:
            assert 'r' not in mode
            fh = sys.stdout
        else:
            fh = sys.stdin if 'r' in mode else sys.stdout
        if 'b' in mode:
            fh = fh.buffer
        if compression:
            if compression == True:
                raise ValueError("Compression can not be determined "
                                 "automatically from stdin")
            fmt = get_compression_format(compression)
            fh = fmt.open_file_python(path, mode, **kwargs)
        if context_wrapper:
            class StdWrapper(object):
                def __init__(self, fh):
                    self.fh = fh
                def __enter__(self):
                    return self.fh
                def __exit__(exception_type, exception_value, traceback):
                    pass
            fh = StdWrapper(fh)
        return fh
    
    if resolve == 2:
        path = check_path(path, 'f', mode)
    elif resolve == 1:
        path = resolve_path(abspath(path))
    
    if compression is not False:
        if compression in (None, True):
            fmt = guess_file_format(path)
            if fmt:
                compression = fmt
            elif compression == True:
                raise ValueError(
                    "Could not guess compression format from {}".format(path))
        
        fmt = get_compression_format(compression)
        return fmt.open_file(path, mode, use_system=use_system, **kwargs)
    
    return open(path, mode, **kwargs)

# Reading data from/writing data to files

## Raw data

def safe_file_read(path : 'str', **kwargs) -> 'str':
    """Read the contents of a file if it exists.
    
    Args:
        path: Path to the file, or a file-like object
        kwargs: Additional arguments to pass to open_
    
    Returns:
        The contents of the file as a string, or empty string if the file does
        not exist.
    """
    try:
        path = check_readable_file(path)
        with open_(path, 'r', **kwargs) as f:
            return f.read()
    except:
        return ""
    
def safe_file_iter(path : 'str', convert : 'callable' = None,
                   strip_linesep : 'bool' = True, **kwargs) -> 'generator':
    """Iterate over a file if it exists.
    
    Args:
        path: Path to the file.
        convert: Function to call on each line in the file.
        kwargs: Additional arguments to pass to open_
    
    Returns:
        Iterator over the lines of a file, with line endings stripped, or
        None if the file doesn't exist or is not readable.
    """
    try:
        path = check_readable_file(path)
    except:
        return
    with open_(path, **kwargs) as f:
        itr = f
        if strip_linesep:
            itr = (line.rstrip() for line in itr)
        if convert:
            itr = (convert(line) for line in itr)
        for line in itr:
            yield line

def read_chunked(path : 'str', chunksize : 'int,>0' = 1024,
                 **kwargs) -> 'generator':
    """Iterate over a file in chunks.
    
    Args:
        path: Path to the file
        chunksize: Number of bytes to read at a time
        kwargs: Additional arguments to pass top ``open_``.
    
    Returns:
        Generator that reads a binary file in chunks of ``chunksize``.
    """
    kwargs['mode'] = 'rb'
    with open_(path, **kwargs) as infile:
        while True:
            data = infile.read(chunksize)
            if data:
                yield data
            else:
                break

def write_strings(path : 'str', strings, linesep : 'str' = '\n'):
    """Write delimiter-separated strings to a file.
    
    Args:
        path: Path to the file
        strings: Single string, or an iterable of strings
        linesep: The delimiter to use to separate the strings, or
            ``os.linesep`` if None (defaults to '\n')
    """
    with open_(path, 'w') as f:
        if isinstance(strings, str):
            f.write(strings)
        else:
            itr = iter(strings)
            f.write(next(itr))
            for s in itr:
                f.write(delim or os.linesep)
                f.write(s)

def write_dict(d : 'dict', path : 'str', sep : 'str' = '=',
               linesep : 'str' = '\n'):
    """Write a dict to a file as name=value lines.
    
    Args:
        d: The dict
        path: Path to the file
        sep: The delimiter between key and value (defaults to '=')
        linesep: The delimiter between values, or ``os.linesep`` if None
            (defaults to '\n')
    """
    write_file(
        path,
        ("{}{}{}".format(k, sep, v) for k, v in d.iteritems()),
        linesep=linesep)

## Compressed/encoded data

def read_pickled(binfile : 'str', compression : 'bool' = False):
    """Read objects from a pickled file, optionally decompressing
    before unpickling.
    
    Args:
        binfile: Path to the pickled file.
        compression: A valid argument to ``compression.get_decompressor``
    
    Returns:
        The pickled object, or None if the file does not exist or is empty.
    """
    if compression is False:
        try:
            with open_(binfile, 'rb') as pfile:
                return pickle.load(pfile)
        except:
            return None
    else:
        data = safe_file_read(binfile, mode='rb')
        if not data:
            return None
        decompressor = get_decompressor(binfile, compression) # TODO
        return pickle.loads(decompressor(data))

def compress_contents(path : 'str', compressed_path : 'str' = None,
                      compression=None):
    """Compress the contents of a file, either in-place or to a separate file.
    
    Args:
        path (str): The path of the file to copy, or a file-like object.
          This can itself be a compressed file (e.g. if you want to change
          the file from one compression format to another).
        compressed_path (str): The compressed file. If None, the file is
          compressed in place.
        compression: None or True, to guess compression format from the file
          name, or the name of any supported compression format.
    """
    inplace = compressed_path is None
    if inplace:
        if compression in (None, True):
            raise ValueException(
                "Either compressed_path must be specified or compression must "
                "be a valid compression type.")
        compressed_path = tempfile.mkstemp()[1]
    
    # Perform sequential compression as the source file might be quite large
    with open_(compressed_path, mode='wb', compression=compression) as cfile:
        for bytes in read_chunked(path):
            cfile.write(bytes)
    
    # Move temp file to original path
    if inplace:
        shutil.move(compressed_path, path)

# def write_archive(path : 'str', contents, **kwargs):
#     """Write entries to a compressed archive file.
#
#     Args:
#         path (str): Path of the archive file to write.
#         contents (iterable): A dict or an iterable of (name, content) tuples.
#           A content item can be a path to a readable file to be added
#           to the archive.
#         kwargs: Additional args to `open_archive_writer`.
#     """
#     if isinstance(contents, dict):
#         contents = dict.items()
#     with open_archive_writer(path, **kwargs) as c:
#         for name, content in contents:
#             if os.path.isfile(content):
#                 c.writefile(content, name)
#             else:
#                 c.writestr(content, name)

## Delimited files

def delimited_file_iter(path : 'str', delim : 'str' = '\t',
                        converters=None, **kwargs) -> 'generator':
    """Iterate over rows in a delimited file.
    
    Args:
        path: Path to the file, or a file-like object
        delim: field delimiter
        converters: function, or iterable of functions, to call on each field
        kwargs: additional arguments to pass to ``csv.reader``
    
    Yields:
        Rows of the delimited file
    """
    if not iterable(converters):
        if callable(converters):
            from itertools import cycle
            converters = cycle(converters)
        else:
            raise ValueError("'converters' must be iterable or callable")
    
    with open_(path, 'r') as f:
        g = (row for row in csv.reader(f, delimiter=delim, **kwargs))
        if converters:
            g = (fn(x) if fn else x
                for row in g
                for fn, x in zip(converters, row))
        for row in g:
            yield g

def delimited_file_to_dict(path : 'str', delim : 'str' = '\t',
                           key : 'int,>=0' = 0, converters=None,
                           skip_blank : 'bool' = True, **kwargs) -> 'dict':
    """Parse rows in a delimited file and add each row to a dict.
    
    Args:
        path: Path to the file, or a file-like object
        delim: Field delimiter
        key: The column to use as a dict key, or a function to extract the key
          from the row. All values must be unique, or an exception is raised.
        converters: function, or iterable of functions, to call on each field
        kwargs: Additional arguments to pass to ``csv.reader``
    
    Returns:
        A dict with as many element as rows in the file.
    """
    if isinstance(key, int):
        keyfn = lambda row: row[key]
    elif callable(key):
        keyfn = key
    else:
        raise ValueError("'key' must be an integer or callable")
    
    d = {}
    for row in delimited_file_iter(path, delim, converters):
        if len(row) == 0 and skip_blank:
            continue
        d[keyfn(v)] = v
    return d

def rows_to_delimited_file(rows : 'iterable', path : 'str',
                           delim : 'str' = '\t', **kwargs):
    """Write rows to delimited file.
    
    Args:
        rows: Iterable of rows
        path: Path to the file
        delim: Field delimiter
        kwargs: Additional args for ``csv.writer``
    """
    with xopen(txt_file, 'w') as o:
        w = csv.writer(o, delimiter=delim, **kwargs)
        w.writerows(rows)

## Property files

def parse_properties(source, delim : 'str' = '=', fn : 'callable' = None,
                     ordered : 'bool' = False) -> 'dict':
    """Read properties from a file (one per line) and/or a list.
    
    If ``fn`` is specified, apply the function to each value.
    
    Args:
        source: Property file, or a list of properties.
        delim: Key-value delimiter (defaults to '=')
        fn: Function to call on each value
        ordered: Whether to return an OrderedDict
    
    Returns:
        An OrderedDict, if 'ordered' is True, otherwise a PropDict, which allows
        property-style indexing.
    """
    if isinstance(source, str):
        source = safe_read_file_array(source)
    def parse_line(line):
        line = line.strip()
        if len(line) == 0 or line[0] == "#":
            return None
        return line.split(delim)
    g = filter(None, (parse_line(line) for line in source))
    if fn:
        g = ((k, fn(v)) for k,v in g)
    return OrderedDict(g) if ordered else PropDict(g)

## Code files

def load_module_from_file(path : 'str'):
    """Load a python module from a file.
    """
    loader = importlib.machinery.SourceFileLoader(filename(path), path)
    return loader.load_module()

# File wrappers that perform an action when the file is closed.

class FileWrapper(object):
    """Base class for file wrappers.
    """
    __slots__ = ['_file']
    
    def __init__(self, f, mode='w', **kwargs):
        object.__setattr__(self, '_file', open_(f, mode=mode, **kwargs))
    
    def __getattr__(self, name):
        return getattr(self._file, name)
    
    def __setattr__(self, name, value):
        setattr(self._file, name, value)

def compress_on_close(f, dest : 'str' = None, ctype=None):
    """Compress the file when it is closed.
    
    Args:
        f: Path or file object
        dest: compressed file, or None to compress in place
        ctype: compression type
    
    Returns:
        File-like object
    """
    class FileCompressor(FileWrapper):
        def close(self):
            self._file.close()
            compress_file(self._file, dest, ctype)
    return FileCompressor(f)

def move_on_close(f, dest : 'str'):
    """Move the file to a new location when it is closed.
    
    Args:
        f: Path or file object
        dest: Destination path
    
    Returns:
        File-like object
    """
    class FileMover(FileWrapper):
        def close(self):
            self._file.close()
            shutil.move(self._file.name, dest)
    return FileMover(f)

def del_on_close(f):
    """Delete the file when it is closed.
    
    Args:
        f: Path or file object
    
    Returns:
        File-like object
    """
    class FileDeleter(FileWrapper):
        def close(self):
            self._file.close()
            os.remove(self._file.name)
    return FileDeleter(f)

class FileCloser(object):
    """Dict-like container for files. Has a ``close`` method that closes
    all open files.
    """
    def __init__(self):
        self.files = {}
    
    def __getitem__(self, key : 'str'):
        return self.files[key]
    
    def __setitem__(self, key : 'str', f):
        """Add a file.
        
        Args:
            key: Dict key
            f: Path or file object. If this is a path, the file will be
                opened with mode 'r'.
        """
        self.add(f, key)
    
    def __contains__(self, key : 'str'):
        return key in self.files
        
    def add(self, f, key : 'str' = None, mode : 'str' = 'r'):
        """Add a file.
        
        Args:
            f: Path or file object. If this is a path, the file will be
                opened with the specified mode.
            key: Dict key. Defaults to the file name.
            mode: Open mode for file, if ``f`` is a path string.
        
        Returns:
            A file object
        """
        f = open_(f, mode)
        if key is None:
            key = f.name
        if key in self.files:
            raise ValueError("Already tracking file with key {}".format(key))
        self.files[key] = f
        return f

    def close(self):
        """Close all files being tracked.
        """
        for fh in self.files.values():
            fh.close()

# Misc

def linecount(f, delim : 'str' = None, bufsize : 'int' = 1024 * 1024) -> 'int':
    """Fastest pythonic way to count the lines in a file.
    
    Args:
        path: File object, or path to the file
        delim: Line delimiter, specified as a byte string (e.g. b'\n')
        bufsize: How many bytes to read at a time (1 Mb by default)
    
    Returns:
        The number of lines in the file
    """
    if delim is None:
        delim = os.linesep.encode()
    lines = 0
    with open_(f, 'rb') as fh:
        read_f = fh.read # loop optimization
        buf = read_f(buf_size)
        while buf:
            lines += buf.count(delim)
            buf = read_f(buf_size)
    return lines

@contextmanager
def buffer_string(value : 'str' = None):
    """Make creation and usage of a string buffer compatible with
    ``Popen.wait``.
    
    Args:
        value: String value to assign to the buffer
    
    Yields:
        io.StringIO wrapping ``value``
    """
    b = io.StringIO(value)
    try:
        yield b
    finally:
        b.close()
