# -*- coding: utf-8 -*-
"""A collection of convenience methods for reading, writing, and
otherwise managing files.
"""
from collections import OrderedDict, Iterable
import csv
import io
from itertools import cycle
import os
import shutil
import sys
import tempfile

from xphyle import open_
from xphyle.formats import *
from xphyle.paths import *

# Reading data from/writing data to files

## Raw data

def safe_read(path : 'str', **kwargs) -> 'str':
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
        with open_(path, mode='rt', **kwargs) as f:
            return f.read()
    except IOError:
        return ""
    
def safe_iter(path : 'str', convert : 'callable' = None,
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
    except IOError:
        return
    with open_(path, **kwargs) as f:
        itr = f
        if strip_linesep:
            itr = (line.rstrip() for line in itr)
        if convert:
            itr = (convert(line) for line in itr)
        for line in itr:
            yield line

def chunked_iter(path : 'str', chunksize : 'int,>0' = 1024,
                 **kwargs) -> 'generator':
    """Iterate over a file in chunks. The mode will always be overridden to 'rb'.
    
    Args:
        path: Path to the file
        chunksize: Number of bytes to read at a time
        kwargs: Additional arguments to pass top ``open_``
    
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

def write_iterable(iterable : 'iterable', path : 'str', linesep : 'str' = '\n',
                   convert : 'callable' = str, **kwargs):
    """Write delimiter-separated strings to a file.
    
    Args:
        iterable: An iterable
        path: Path to the file
        linesep: The delimiter to use to separate the strings, or
            ``os.linesep`` if None (defaults to '\n')
        convert: Function that converts a value to a string
        kwargs: Additional arguments to pass top ``open_``
    """
    if linesep is None:
        linesep = os.linesep
    with open_(path, mode='w', **kwargs) as f:
        f.write(linesep.join(convert(s) for s in iterable))

def read_dict(path, sep : 'str' = '=', convert : 'callable' = None,
              ordered : 'bool' = False, **kwargs) -> 'dict':
    """Read lines from simple property file (key=value). Comment lines (starting
    with '#') are ignored.
    
    Args:
        path: Property file, or a list of properties.
        sep: Key-value delimiter (defaults to '=')
        convert: Function to call on each value
        ordered: Whether to return an OrderedDict
        kwargs: Additional arguments to pass top ``open_``
    
    Returns:
        An OrderedDict, if 'ordered' is True, otherwise a dict.
    """
    def parse_line(line):
        line = line.strip()
        if len(line) == 0 or line[0] == "#":
            return None
        return line.split(sep)
    lines = filter(None, safe_iter(path, convert=parse_line, **kwargs))
    if convert:
        lines = ((k, convert(v)) for k, v in lines)
    return OrderedDict(lines) if ordered else dict(lines)

def write_dict(d : 'dict', path : 'str', sep : 'str' = '=',
               linesep : 'str' = '\n', convert : 'callable' = str):
    """Write a dict to a file as name=value lines.
    
    Args:
        d: The dict
        path: Path to the file
        sep: The delimiter between key and value (defaults to '=')
        linesep: The delimiter between values, or ``os.linesep`` if None
            (defaults to '\n')
        convert: Function that converts a value to a string
    """
    if linesep is None:
        linesep = os.linesep
    write_iterable(
        ("{}{}{}".format(k, sep, convert(v)) for k, v in d.items()),
        path, linesep=linesep)

## Delimited files

def delimited_file_iter(path : 'str', delim : 'str' = '\t',
                        header : 'bool' = False,
                        converters : 'callable|iterable' = None,
                        **kwargs) -> 'generator':
    """Iterate over rows in a delimited file.
    
    Args:
        path: Path to the file, or a file-like object
        delim: field delimiter
        converters: function, or iterable of functions, to call on each field
        kwargs: additional arguments to pass to ``csv.reader``
    
    Yields:
        Rows of the delimited file. If ``header==True``, the first row yielded
        is the header row. Converters are not applied to the header row.
    """
    with open_(path, 'r') as f:
        reader = csv.reader(f, delimiter=delim, **kwargs)
        if not converters:
            for row in reader:
                yield reader
        else:
            if not is_iterable(converters):
                if callable(converters):
                    converters = cycle([converters])
                else:
                    raise ValueError("'converters' must be iterable or callable")
                
            if header:
                yield next(reader)
            
            for row in reader:
                yield [fn(x) if fn else x for fn, x in zip(converters, row)]

def delimited_file_to_dict(path : 'str', delim : 'str' = '\t',
                           key : 'int,>=0|callable' = 0,
                           converters : 'callable|iterable' = None,
                           skip_blank : 'bool' = True, **kwargs) -> 'dict':
    """Parse rows in a delimited file and add rows to a dict based on a a
    specified key index or function.
    
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
    with open_(txt_file, 'w') as o:
        w = csv.writer(o, delimiter=delim, **kwargs)
        w.writerows(rows)

## Compressed files

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

def is_iterable(x):
    return isinstance(x, Iterable)
