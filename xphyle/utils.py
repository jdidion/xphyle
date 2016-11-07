# -*- coding: utf-8 -*-
"""A collection of convenience methods for reading, writing, and otherwise
managing files. All of these functions are 'safe', meaning that if you pass
``errors=False`` and there is a problem opening the file, the error will be
handled gracefully.
"""
from collections import OrderedDict, Iterable
import csv
import io
from itertools import cycle
import os
import shutil
import sys

from xphyle import *
from xphyle.formats import *
from xphyle.paths import *

# Reading data from/writing data to files

## Raw data

def iter_lines(path : 'str|file', convert : 'callable' = None,
               strip_linesep : 'bool' = True, **kwargs) -> 'generator':
    """Iterate over lines in a file.
    
    Args:
        path: Path to the file, or a file-like object
        convert: Function to call on each line in the file
        strip_linesep: Whether to strip off trailing line separators
        kwargs: Additional arguments to pass to open_
    
    Returns:
        Iterator over the lines of a file, with line endings stripped.
    """
    with open_(path, **kwargs) as f:
        if f is None:
            return ()
        itr = f
        if strip_linesep:
            itr = (line.rstrip() for line in itr)
        if convert:
            itr = (convert(line) for line in itr)
        for line in itr:
            yield line

def chunked_iter(path : 'str', chunksize : 'int,>0' = 1024,
                 **kwargs) -> 'generator':
    """Iterate over a file in chunks. The mode will always be overridden
    to 'rb'.
    
    Args:
        path: Path to the file, or a file-like object
        chunksize: Number of bytes to read at a time
        kwargs: Additional arguments to pass top ``open_``
    
    Returns:
        Generator that reads a binary file in chunks of ``chunksize``.
    """
    kwargs['mode'] = 'rb'
    with open_(path, **kwargs) as f:
        if f is None:
            return ()
        for chunk in iter_file_chunked(f, chunksize):
            yield chunk

def write_iterable(iterable : 'iterable', path : 'str|file',
                   linesep : 'str' = '\n', convert : 'callable' = str,
                   **kwargs):
    """Write delimiter-separated strings to a file.
    
    Args:
        iterable: An iterable
        path: Path to the file, or a file-like object
        linesep: The delimiter to use to separate the strings, or
            ``os.linesep`` if None (defaults to '\\n')
        convert: Function that converts a value to a string
        kwargs: Additional arguments to pass top ``open_``
    
    Returns:
        Total number of bytes written, or -1 if ``errors=False`` and there was
        a problem opening the file.
    """
    if linesep is None:
        linesep = os.linesep
    if 'mode' not in kwargs:
        kwargs['mode'] = 'w'
    written = 0
    with open_(path, **kwargs) as f:
        if f is None:
            return -1
        written += f.write(linesep.join(convert(s) for s in iterable))
    return written

# key=value files

def read_dict(path: 'str|file', sep : 'str' = '=', convert : 'callable' = None,
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
    lines = filter(None, iter_lines(path, convert=parse_line, **kwargs))
    if convert:
        lines = ((k, convert(v)) for k, v in lines)
    return OrderedDict(lines) if ordered else dict(lines)

def write_dict(d : 'dict', path : 'str', sep : 'str' = '=',
               linesep : 'str' = '\n', convert : 'callable' = str, **kwargs):
    """Write a dict to a file as name=value lines.
    
    Args:
        d: The dict
        path: Path to the file
        sep: The delimiter between key and value (defaults to '=')
        linesep: The delimiter between values, or ``os.linesep`` if None
            (defaults to '\\n')
        convert: Function that converts a value to a string
    """
    if linesep is None:
        linesep = os.linesep
    write_iterable(
        ("{}{}{}".format(k, sep, convert(v)) for k, v in d.items()),
        path, linesep=linesep, **kwargs)

## Other delimited files

def delimited_file_iter(path : 'str', sep : 'str' = '\t',
                        header : 'bool|iterable' = False,
                        converters : 'callable|iterable' = None,
                        yield_header : 'bool' = True,
                        row_type : 'str|callable' = 'list',
                        **kwargs) -> 'generator':
    """Iterate over rows in a delimited file.
    
    Args:
        path: Path to the file, or a file-like object
        sep: The field delimiter
        header: Either True or False to specifiy whether the file has a header,
            or an iterable of column names.
        converters: callable, or iterable of callables, to call on each value
        yield_header: If header == True, whether the first row yielded should be
            the header row
        row_type: The collection type to return for each row:
            tuple, list, or dict
        kwargs: additional arguments to pass to ``csv.reader``
    
    Yields:
        Rows of the delimited file. If ``header==True``, the first row yielded
        is the header row, and its type is always a list. Converters are not
        applied to the header row.
    """
    if row_type == 'dict' and not header:
        raise ValueError("Header must be specified for row_type=dict")
    
    with open_(path, **kwargs) as f:
        if f is None:
            return ()
        
        reader = csv.reader(f, delimiter=sep, **kwargs)
        
        if header is True:
            header_row = next(reader)
            if yield_header:
                yield header_row
        
        if converters:
            if not is_iterable(converters):
                if callable(converters):
                    converters = cycle([converters])
                else:
                    raise ValueError("'converters' must be iterable or callable")
            
            reader = (
                [fn(x) if fn else x for fn, x in zip(converters, row)]
                for row in reader)
        
        if row_type == 'tuple':
            reader = (tuple(row) for row in reader)
        elif row_type == 'dict':
            reader = (dict(zip(header_row, row)) for row in reader)
        elif callable(row_type):
            reader = (row_type(row) for row in reader)
        
        for row in reader:
            yield row

def delimited_file_to_dict(path : 'str', sep : 'str' = '\t',
                           header : 'bool|iterable' = False,
                           key : 'int,>=0|callable' = 0, **kwargs) -> 'dict':
    """Parse rows in a delimited file and add rows to a dict based on a a
    specified key index or function.
    
    Args:
        path: Path to the file, or a file-like object
        sep: Field delimiter
        key: The column to use as a dict key, or a function to extract the key
          from the row. If a string value, header must be specified. All values
          must be unique, or an exception is raised.
        kwargs: Additional arguments to pass to ``delimited_file_iter``
    
    Returns:
        A dict with as many element as rows in the file
    
    Raises:
        Exception if a duplicte key is generated
    """
    itr = None
    
    if isinstance(key, str):
        if not header:
            raise ValueError(
                "'header' must be specified if 'key' is a column name")
        if header is True:
            kwargs['yield_header'] = True
            itr = delimited_file_iter(path, sep, True, **kwargs)
            header = next(itr)
        key = header.index(key)
    
    if isinstance(key, int):
        keyfn = lambda row: row[key]
    elif callable(key):
        keyfn = key
    else:
        raise ValueError("'key' must be an column name, index, or callable")
    
    if itr is None:
        kwargs['yield_header'] = False
        itr = delimited_file_iter(path, sep, header, **kwargs)
    
    d = {}
    for row in itr:
        k = keyfn(row)
        if k in d:
            raise Exception("Duplicate key {}".format(k))
        d[k] = row
    return d

## Compressed files

def compress_file(source_file, compressed_file=None,
                  compression : 'bool|str' = None,
                  keep : 'bool' = True, compresslevel : 'int' = None,
                  use_system : 'bool' = True, **kwargs) -> 'str':
    """Compress an existing file, either in-place or to a separate file.
    
    Args:
        source_file: Path or file-like object to compress
        compressed_file: The compressed path or file-like object. If None,
            compression is performed in-place. If True, file name is determined
            from ``source_file`` and the uncompressed file is retained.
        compression: None or True, to guess compression format from the file
            name, or the name of any supported compression format.
        keep: Whether to keep the source file
        compresslevel: Compression level
        use_system: Whether to try to use system-level compression
        kwargs: Additional arguments to pass to the open method when
            opening the compressed file
    
    Returns:
        The path to the compressed file
    """
    if not isinstance(compression, str):
        if compressed_file:
            compression = guess_compression_format(compressed_file
                if isinstance(compressed_file, str) else compressed_file.name)
        else:
            raise ValueError(
                "'compressed_file' or 'compression' must be specified")
    
    fmt = get_compression_format(compression)
    return fmt.compress_file(
        source_file, compressed_file, keep, compresslevel, use_system, **kwargs)

def uncompress_file(compressed_file, dest_file=None,
                    compression : 'bool|str' = None,
                    keep : 'bool' = True, use_system : 'bool' = True,
                    **kwargs) -> 'str':
    """Uncompress an existing file, either in-place or to a separate file.
    
    Args:
        compressed_file: Path or file-like object to uncompress
        dest_file: Path or file-like object for the uncompressed file.
            If None, file will be uncompressed in-place. If True, file will be
            uncompressed to a new file (and the compressed file retained) whose
            name is determined automatically.
        compression: None or True, to guess compression format from the file
            name, or the name of any supported compression format.
        keep: Whether to keep the source file
        use_system: Whether to try to use system-level compression
        kwargs: Additional arguments to pass to the open method when
            opening the compressed file
    
    Returns:
        The path of the uncompressed file
    """
    if not isinstance(compression, str):
        source_path = compressed_file
        if not isinstance(compressed_file, str):
            source_path = compressed_file.name
        compression = guess_compression_format(source_path)
    fmt = get_compression_format(compression)
    return fmt.uncompress_file(
        compressed_file, dest_file, keep, use_system, **kwargs)

# FileEventListeners

class CompressOnClose(FileEventListener):
    """Compress a file after it is closed."""
    def execute(self, path, *args, **kwargs):
        self.compressed_path = compress_file(path, *args, **kwargs)

class MoveOnClose(FileEventListener):
    """Move a file after it is closed.."""
    def execute(self, path, dest):
        shutil.move(path, dest)

class RemoveOnClose(FileEventListener):
    """Remove a file after it is closed.."""
    def execute(self, path):
        os.remove(path)

# Misc

class FileManager(object):
    """Dict-like container for files. Has a ``close`` method that closes
    all open files.
    """
    def __init__(self):
        self.files = {}
    
    def __len__(self):
        return len(self.files)
    
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
        
    def add(self, f, key : 'str' = None, **kwargs):
        """Add a file.
        
        Args:
            f: Path or file object. If this is a path, the file will be
                opened with the specified mode.
            key: Dict key. Defaults to the file name.
            kwargs: Arguments to pass to xopen
        
        Returns:
            A file object
        """
        if isinstance(f, str):
            path = f
            f = xopen(f, **kwargs)
        else:
            path = f.name
        if key is None:
            key = path
        if key in self.files:
            raise ValueError("Already tracking file with key {}".format(key))
        self.files[key] = f
        return f
    
    def items(self):
        """Returns a list of all (key, file) pairs.
        """
        return self.files.items()
    
    def close(self):
        """Close all files being tracked.
        """
        for fh in self.files.values():
            if fh and not fh.closed:
                fh.close()

def linecount(f, linesep : 'str' = None, buffer_size : 'int' = 1024 * 1024,
              **kwargs) -> 'int':
    """Fastest pythonic way to count the lines in a file.
    
    Args:
        path: File object, or path to the file
        linesep: Line delimiter, specified as a byte string (e.g. b'\\n')
        bufsize: How many bytes to read at a time (1 Mb by default)
        kwargs: Additional arguments to pass to the file open method
    
    Returns:
        The number of lines in the file. Blank lines (including the last line
        in the file) are included.
    """
    if buffer_size < 1:
        raise ValueError("'buffer_size' must be >= ")
    if linesep is None:
        linesep = os.linesep.encode()
    if 'mode' not in kwargs:
        kwargs['mode'] = 'rb'
    elif kwargs['mode'] != 'rb':
        raise ValueError("File must be opened with mode 'rb'")
    with open_(f, **kwargs) as fh:
        if fh is None:
            return -1
        read_f = fh.read # loop optimization
        buf = read_f(buffer_size)
        if len(buf) == 0: # empty file case
            return 0
        lines = 1
        while buf:
            lines += buf.count(linesep)
            buf = read_f(buffer_size)
        return lines

def is_iterable(x):
    """Returns True if ``x`` is a non-string Iterable.
    
    Args:
        x: The object to test
    """
    return isinstance(x, Iterable) and not isinstance(x, str)
