# -*- coding: utf-8 -*-
"""Interfaces to compression file formats.
Magic numbers from: https://en.wikipedia.org/wiki/List_of_file_signatures
"""
from importlib import import_module
import io
import logging
import os
import re
import sys
from subprocess import Popen, PIPE
import tempfile

from xphyle.paths import *
from xphyle.progress import *

# Number of concurrent threads that can be used
# by formats that support parallelization
threads = 1

def get_threads():
    global threads
    if threads == False:
        threads = 1
    elif threads is True or not (isinstance(threads, int) and threads >= 1):
        import multiprocessing
        threads = multiprocessing.cpu_count()
    return threads

# File formats

class FileFormat(object):
    """Base class for classes that wrap built-in python file format libraries.
    The subclass must provide the ``name`` member.
    """
    _lib = None
    
    @property
    def lib(self):
        """Caches and returns the python module assocated with this file format.
        
        Returns:
            The module
        
        Raises:
            CompressionError if the module cannot be imported.
        """
        if not self._lib:
            self._lib = import_module(self.name)
        return self._lib

# Interfaces to file compression formats. Most importantly, these attempt
# to use system-level processes, which is faster than using the python
# implementations.

# Wrappers around system-level compression executables

class SystemReader:
    """Read from a compressed file using a system-level compression program.
    
    Args:
        executable_path: The fully resolved path the the executable
        filename: The compressed file to read
        ext: The file extension; if ``filename`` already has an extension, this
            must match (including the leading '.')
        command: Format string with two variables -- ``exe`` (the path to the
          system executable), and ``filename``
        executable_name: The display name of the executable, or ``None`` to use
          the basename of ``executable_path``
    """
    def __init__(self, executable_path : 'str', filename : 'str',
                 command : 'str', executable_name : 'str' = None):
        self.name = filename
        self.command = command
        self.executable_name = (
            executable_name or os.path.basename(executable_path))
        self.process = Popen(self.command, stdout=PIPE)
        self.closed = False
    
    def readable(self): return True
    def writable(self): return False
    def seekable(self): return False
    def flush(self): pass
    
    def close(self):
        self.close = True
        retcode = self.process.poll()
        if retcode is None:
            # still running
            self.process.terminate() # pragma: no-cover
        self._raise_if_error()

    def __iter__(self):
        for line in self.process.stdout:
            yield line
        self.process.wait()
        self._raise_if_error()

    def _raise_if_error(self):
        """Raise EOFError if process is not running anymore and the
        exit code is nonzero.
        """
        retcode = self.process.poll()
        if retcode is not None and retcode != 0: # pragma: no-cover
            raise EOFError(
                "{} process returned non-zero exit code {}. "
                "Is the input file truncated or corrupt?".format(
                self.executable_name, retcode))

    def read(self, *args):
        data = self.process.stdout.read(*args)
        if len(args) == 0 or args[0] <= 0:
            # wait for process to terminate until we check the exit code
            self.process.wait()
        self._raise_if_error()
        return data

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

class SystemWriter:
    """Write to a compressed file using a system-level compression program.
    
    Args:
        executable_path: The fully resolved path the the executable
        filename: The compressed file to read
        ext: The file extension; if ``filename`` already has an extension, this
            must match (including the leading '.')
        mode: The write mode (w/a/x)
        command: Format string with two variables -- ``exe`` (the path to the
          system executable), and ``filename``
        executable_name: The display name of the executable, or ``None`` to use
          the basename of ``executable_path``
    """
    def __init__(self, executable_path : 'str', filename : 'str',
                 mode : 'str' = 'w', command : 'str' = "{exe}",
                 executable_name : 'str' = None):
        self.name = filename
        self.command = command
        self.executable_name = (
            executable_name or os.path.basename(executable_path))
        self.outfile = open(filename, mode)
        self.devnull = open(os.devnull, 'w')
        self.closed = False
        try:
            # Setting close_fds to True is necessary due to
            # http://bugs.python.org/issue12786
            self.process = Popen(
                self.command, stdin=PIPE, stdout=self.outfile,
                stderr=self.devnull, close_fds=True)
        except IOError as e: # pragma: no-cover
            self.outfile.close()
            self.devnull.close()
            raise
    
    def readable(self): return False
    def writable(self): return True
    def seekable(self): return False
    
    def write(self, arg):
        self.process.stdin.write(arg)
    
    def flush(self):
        self.process.stdin.flush()
    
    def close(self):
        self.closed = True
        self.process.stdin.close()
        retcode = self.process.wait()
        self.outfile.close()
        self.devnull.close()
        if retcode != 0: # pragma: no-cover
            raise IOError("Output {} process terminated with "
                          "exit code {}".format(self.executable_name, retcode))

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

# Supported compression formats

compression_formats = {}
"""Dict of registered compression formats"""

magic_bytes = {}
"""Dict mapping the first byte in a 'magic' sequence to a tuple of
(format, rest_of_sequence)
"""
max_magic_bytes = 0
"""Maximum number of bytes in a registered magic byte sequence"""

mime_types = {}
"""Dict mapping MIME types to file formats"""

def register_compression_format(format_class : 'class'):
    """Register a new compression format.
    
    Args:
        ``format_class`` -- a subclass of CompressionFormat
    """
    fmt = format_class()
    aliases = set(fmt.exts)
    #if isinstance(fmt.system_commands, dict):
    #    aliases = aliases | set(fmt.system_commands.values())
    #else:
    aliases.update(fmt.system_commands)
    aliases.add(fmt.name)
    for alias in aliases:
        # TODO: warn about overriding existing format?
        compression_formats[alias] = fmt
    for magic in fmt.magic_bytes:
        global max_magic_bytes
        max_magic_bytes = max(max_magic_bytes, len(magic))
        magic_bytes[magic[0]] = (fmt.name, magic[1:])
    for mime in fmt.mime_types:
        mime_types[mime] = fmt.name

def get_compression_format(name : 'str') -> 'CompressionFormat':
    """Returns the CompressionFormat associated with the given name, or raises
    ValueError if that format is not supported.
    """
    if name in compression_formats:
        return compression_formats[name]
    raise ValueError("Unsupported compression format: {}".format(name))

def guess_compression_format(name : 'str') -> 'str':
    """Guess the compression format by name or file extension.
    """
    if name in compression_formats:
        return compression_formats[name].name
    i = name.rfind(os.extsep)
    if i >= 0:
        ext = name[(i+1):]
        if ext in compression_formats:
            return compression_formats[ext].name
    return None

def guess_format_from_file_header(path : 'str') -> 'str':
    """Guess file format from 'magic bytes' at the beginning of the file.
    
    Note that ``path`` must be openable and readable. If it is a named pipe or
    other pseudo-file type, the magic bytes will be destructively consumed and
    thus will open correctly.
    
    Args:
        path: Path to the file
    
    Returns:
        The name of the format, or ``None`` if it could not be guessed.
    """
    with open(path, 'rb') as fh:
        return guess_format_from_header_bytes(fh.read(max_magic_bytes))

def guess_format_from_buffer(buffer : 'str') -> 'str':
    """Guess file format from a byte buffer that provides a ``peek`` method.
    
    Args:
        buffer: The buffer object
    
    Returns:
        The name of the format, or ``None`` if it could not be guessed.
    """
    return guess_format_from_header_bytes(buffer.peek(max_magic_bytes))

def guess_format_from_header_bytes(b : 'bytes') -> 'str':
    """Guess file format from a sequence of bytes from a file header.
    
    Args:
        bytes: The bytes
    
    Returns:
        The name of the format, or ``None`` if it could not be guessed.
    """
    num_bytes = len(b)
    if num_bytes > 0:
        if b[0] in magic_bytes:
            fmt, tail = magic_bytes[b[0]]
            if (num_bytes > len(tail) and
                    tuple(b[i] for i in range(1, len(tail)+1)) == tail):
                return fmt
    return None

def get_format_for_mime_type(mime_type : 'str') -> 'str':
    pass

class CompressionFormat(FileFormat):
    """Base class for classes that provide access to system-level and
    python-level implementations of compression formats.
    """
    @property
    def default_ext(self) -> 'str':
        return self.exts[0]
    
    def _get_compresslevel(self, level=None):
        if level is None:
            level = self.default_compresslevel
        elif level < self.compresslevel_range[0]:
            level = self.compresslevel_range[0]
        elif level > self.compresslevel_range[1]:
            level = self.compresslevel_range[1]
        return level
    
    @property
    def can_use_system_compression(self) -> 'bool':
        """Returns True if at least one command in ``self.system_commands``
        resolves to an existing, executable file.
        """
        return self.compress_path is not None
    
    @property
    def can_use_system_uncompression(self) -> 'bool':
        """Returns True if at least one command in ``self.system_commands``
        resolves to an existing, executable file.
        """
        return self.uncompress_path is not None
    
    def compress(self, bytes : 'bytes', **kwargs) -> 'bytes':
        """Compress bytes.
        
        Args:
            bytes: The bytes to compress
            kwargs: Additional arguments to compression function.
        
        Returns:
            The compressed bytes
        """
        kwargs['compresslevel'] = self._get_compresslevel(
            kwargs.get('compresslevel', None))
        return self.lib.compress(bytes, **kwargs)
    
    def compress_string(self, text : 'str', encoding : 'str' = 'utf-8',
                        **kwargs) -> 'bytes':
        """Compress a string.
        
        Args:
            text: The text to compress
            encoding: The byte encoding (utf-8)
            kwargs: Additional arguments to compression function
        
        Returns:
            The compressed text, as bytes
        """
        return self.compress(text.encode(encoding))
    
    def compress_iterable(self, strings : 'list', delimiter : 'bytes' = b'',
                          encoding : 'str' = 'utf-8', **kwargs) -> 'bytes':
        """Compress an iterable of strings using the python-level interface.
        
        Args:
            strings: An iterable of strings
            delimiter: The delimiter (byte string) to use to separate strings
            encoding: The byte encoding (utf-8)
            kwargs: Additional arguments to compression function
        
        Returns:
            The compressed text, as bytes
        """
        return self.compress(
            delimiter.join(s.encode(encoding) for s in strings),
            **kwargs)
    
    def decompress(self, bytes, **kwargs) -> 'bytes':
        """Decompress bytes.
        
        Args:
            bytes: The compressed data
            kwargs: Additional arguments to the decompression function
        
        Returns:
            The decompressed bytes
        """
        return self.lib.decompress(bytes, **kwargs)
    
    def decompress_string(self, bytes : 'bytes', encoding : 'str' = 'utf-8',
                          **kwargs) -> 'str':
        """Decompress bytes and return as a string.
        
        Args:
            bytes: The compressed data
            encoding: The byte encoding to use
            kwargs: Additional arguments to the decompression function
        
        Returns:
            The decompressed data as a string
        """
        return self.decompress(bytes, **kwargs).decode(encoding)
    
    def open_file(self, filename : 'str', mode : 'str',
                  ext : 'str' = None, use_system : 'bool' = True, **kwargs):
        """Opens a compressed file for reading or writing.
        
        If ``use_system`` is True and the system provides an accessible
        executable, then system-level compression is used. Otherwise defaults
        to using the python implementation.
        
        Args:
            filename: The file to open
            mode: The file open mode
            ext: The file extension (including leading '.'); if None, the
                extension is determined automatically, and if there is no
                extension, the format's default extension is used instead.
            use_system: Whether to attempt to use system-level compression
            kwargs: Additional arguments to pass to the python-level open
                method, if system-level compression isn't used.
        
        Returns:
            A file-like object
        """
        if use_system:
            z = None
            if 'r' in mode and self.can_use_system_compression:
                z = SystemReader(
                    self.compress_path,
                    filename,
                    self.get_command('d', src=filename),
                    self.compress_name)
            elif 'r' not in mode and self.can_use_system_uncompression:
                for c in mode:
                    if c in ('w', 'a', 'x'):
                        bin_mode = c + 'b'
                        break
                else:
                    raise ValueError("Invalid mode: {}".format(mode))
                z = SystemWriter(
                    self.uncompress_path,
                    filename,
                    bin_mode,
                    self.get_command('c'),
                    self.uncompress_name)
            if z:
                if 't' in mode:
                    z = io.TextIOWrapper(z)
                return z
        
        return self.open_file_python(filename, mode, **kwargs)
    
    def open_file_python(self, f, mode : 'str', **kwargs):
        """Open a file using the python library.
        
        Args:
            f: The file to open -- a path or open file object
            mode: The file open mode
            kwargs: Additional arguments to pass to the open method
        
        Returns:
            A file-like object
        """
        return self.lib.open(f, mode, **kwargs)
    
    def compress_file(self, source, dest=None, keep : 'bool' = True,
                      compresslevel : 'int' = None,
                      use_system : 'bool' = True, **kwargs) -> 'str':
        """Compress data from one file and write to another.
        
        Args:
            source: Source file, either a path or an open file-like object.
            dest: Destination file, either a path or an open file-like object.
                If None, the file name is determined from ``source``.
            keep: Whether to keep the source file
            compresslevel: Compression level
            use_system: Whether to try to use system-level compression
            kwargs: Additional arguments to pass to the open method when opening
                the destination file
        
        Returns:
            Path to the destination file
        """
        source_is_path = isinstance(source, str)
        if source_is_path:
            check_readable_file(source)
            source_path = source
        else:
            source_path = source.name
        
        in_place = False
        if dest is None:
            in_place = True
            dest = "{}.{}".format(source_path, self.default_ext)
        dest_is_path = isinstance(dest, str)
        if dest_is_path:
            check_writeable_file(dest)
        
        try:
            if use_system and self.can_use_system_compression:
                if source_is_path:
                    cmd_src = source
                    prc_src = None
                else:
                    cmd_src = STDIN
                    prc_src = source
                dest_file = open(dest, 'wb') if dest_is_path else dest
                cmd = self.get_command(
                    'c', src=cmd_src, compresslevel=compresslevel)
                p = wrap_subprocess(cmd, stdin=prc_src, stdout=dest_file)
                p.communicate()
            else:
                source_file = open(source, 'rb') if source_is_path else source
                dest_file = self.open_file_python(dest, 'wb', **kwargs)
                try:
                    # Perform sequential compression as the source
                    # file might be quite large
                    for chunk in iter_file_chunked(source_file):
                        dest_file.write(chunk)
                finally:
                    if source_is_path:
                        source_file.close()

            if not keep:
                if not source_is_path:
                    source.close()
                os.remove(source_path)
        finally:
            if dest_is_path:
                dest_file.close()
        
        return dest
    
    def uncompress_file(self, source, dest=None, keep : 'bool' = True,
                        use_system : 'bool' = True, **kwargs) -> 'str':
        """Uncompress data from one file and write to another.
        
        Args:
            source: Source file, either a path or an open file-like object.
            dest: Destination file, either a path or an open file-like object.
                If None, the file name is determined from ``source``.
            keep: Whether to keep the source file
            use_system: Whether to try to use system-level compression
            kwargs: Additional arguments to passs to the open method when
                opening the compressed file
        
        Returns:
            Path to the destination file
        """
        source_is_path = isinstance(source, str)
        if source_is_path:
            check_readable_file(source)
            source_path = source
        else:
            source_path = source.name
        source_parts = split_path(source_path)
        
        in_place = False
        if dest is None:
            in_place = True
            if len(source_parts) > 2:
                dest = (
                    os.path.join(*source_parts[0:2]) +
                    ''.join(source_parts[2:-1]))
            else:
                raise Exception("Cannot determine path for uncompressed file")
        dest_is_path = isinstance(dest, str)
        if dest_is_path:
            check_writeable_file(dest)
        dest_file = open(dest, 'wb') if dest_is_path else dest
        
        try:
            if use_system and self.can_use_system_uncompression:
                src = source if source_is_path else STDIN
                cmd = self.get_command('d', src=src)
                psrc = None if source_is_path else source
                p = wrap_subprocess(cmd, stdin=psrc, stdout=dest_file)
                p.communicate()
            else:
                source_file = self.open_file_python(source, 'rb', **kwargs)
                try:
                    # Perform sequential decompression as the source
                    # file might be quite large
                    for chunk in iter_file_chunked(source_file):
                        dest_file.write(chunk)
                finally:
                    if source_is_path:
                        source_file.close()

            if not keep:
                if not source_is_path:
                    source.close()
                os.remove(source_path)
        finally:
            if dest_is_path:
                dest_file.close()

        return dest

class SingleExeCompressionFormat(CompressionFormat):
    """Base class form ``CompressionFormat``s that use the same executable for
    compressing and uncompressing.
    """
    @property
    def executable_path(self) -> 'str':
        self._resolve_executable()
        return self._executable_path
    compress_path = executable_path
    uncompress_path = executable_path
    
    @property
    def executable_name(self) -> 'str':
        self._resolve_executable()
        return self._executable_name
    compress_name = executable_name
    uncompress_name = executable_name
    
    def _resolve_executable(self):
        if not hasattr(self, '_executable_path'):
            self._executable_path, self._executable_name = _resolve_exe(
                self.system_commands)

class Gzip(SingleExeCompressionFormat):
    """Implementation of CompressionFormat for gzip files.
    """
    name = 'gzip'
    exts = ('gz',)
    system_commands = ('pigz','gzip')
    default_compresslevel = 6
    magic_bytes = [(0x1f, 0x8b)]
    mime_types = (
        'application/gz',
        'application/gzip',
        'application/x-gz',
        'application/x-gzip'
    )
    
    @property
    def compresslevel_range(self):
        if self.executable_name == 'pigz':
            return (0, 11)
        else:
            return (1, 9)
    
    def get_command(self, op, src=STDIN, stdout=True, compresslevel=None):
        cmd = [self.executable_path]
        if op == 'c':
            compresslevel = self._get_compresslevel(compresslevel)
            cmd.append('-{}'.format(compresslevel))
        elif op == 'd':
            cmd.append('-d')
        if stdout:
            cmd.append('-c')
        if self.executable_name == 'pigz' and get_threads() > 1:
            cmd.extend(('-p', str(threads)))
        if src != STDIN:
            cmd.append(src)
        return cmd
    
    def open_file_python(self, filename, mode, **kwargs):
        z = self.lib.open(filename, mode, **kwargs)
        if 'b' in mode:
            if 'r' in mode:
                z = io.BufferedReader(z)
            else:
                z = io.BufferedWriter(z)
        return z

register_compression_format(Gzip)

class BZip2(SingleExeCompressionFormat):
    """Implementation of CompressionFormat for bzip2 files.
    """
    name = 'bz2'
    exts = ('bz2','bzip','bzip2')
    system_commands = ('bzip2',)
    compresslevel_range = (1, 9)
    default_compresslevel = 6
    magic_bytes = ((0x42, 0x5A, 0x68),)
    mime_types = (
        'application/bz2',
        'application/bzip2',
        'application/x-bz2',
        'application/x-bzip2'
    )
    
    def get_command(self, op, src=STDIN, stdout=True, compresslevel=6):
        cmd = [self.executable_path]
        if op == 'c':
            compresslevel = self._get_compresslevel(compresslevel)
            cmd.append('-{}'.format(compresslevel))
            cmd.append('-z')
        elif op == 'd':
            cmd.append('-d')
        if stdout:
            cmd.append('-c')
        if src != STDIN:
            cmd.append(src)
        return cmd
    
    def open_file_python(self, filename, mode, **kwargs):
        if 't' in mode:
            mode = mode.replace('t','')
            return io.TextIOWrapper(
                self.lib.BZ2File(filename, mode, **kwargs))
        else:
            return self.lib.BZ2File(filename, mode, **kwargs)

register_compression_format(BZip2)

class Lzma(SingleExeCompressionFormat):
    """Implementation of CompressionFormat for lzma (.xz) files.
    """
    name = 'lzma'
    exts = ('xz', 'lzma', '7z', '7zip')
    system_commands = ('xz', 'lzma')
    compresslevel_range = (0, 9)
    default_compresslevel = 6
    magic_bytes = (
        (0x4C, 0x5A, 0x49, 0x50), # lz
        (0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00), # xz
        (0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C) # 7z
    )
    mime_types = (
        'application/lzma',
        'application/x-lzma',
        'application/xz',
        'application/x-xz',
        'application/7z-compressed'
        'application/x-7z-compressed'
    )
    
    def get_command(self, op, src=STDIN, stdout=True, compresslevel=6):
        cmd = [self.executable_path]
        if op == 'c':
            compresslevel = self._get_compresslevel(compresslevel)
            cmd.append('-{}'.format(compresslevel))
            cmd.append('-z')
        elif op == 'd':
            cmd.append('-d')
        if stdout:
            cmd.append('-c')
        if get_threads() > 1:
            cmd.extend(('-T', str(threads)))
        if src != STDIN:
            cmd.append(src)
        return cmd
    
    def compress(self, bytes, **kwargs) -> 'bytes':
        kwargs = dict((k, kwargs[k])
            for k, v in kwargs.items()
            if k in ('format','check','preset','filter'))
        return self.lib.compress(bytes, **kwargs)

register_compression_format(Lzma)

# class DualExeCompressionFormat(CompressionFormat):
#     """CompressionFormat that uses the same executable for compressing and
#     uncompressing.
#     """
#     @property
#     def compress_path(self) -> 'str':
#         self._resolve_compress()
#         return self._compress_path
#
#     @property
#     def uncompress_path(self) -> 'str':
#         self._resolve_uncompress()
#         return self._uncompress_path
#
#     @property
#     def compress_name(self) -> 'str':
#         self._resolve_compress()
#         return self._compress_name
#
#     @property
#     def uncompress_name(self) -> 'str':
#         self._resolve_uncompress()
#         return self._uncompress_name
#
#     def _resolve_compress(self):
#         if not hasattr(self, '_compress_path'):
#             self._compress_path, self._compress_name = _resolve_exe(
#                 self.system_commands['compress'])
#
#     def _resolve_uncompress(self):
#         if not hasattr(self, '_uncompress_path'):
#             self._uncompress_path, self._uncompress_name = _resolve_exe(
#                 self.system_commands['uncompress'])
#
# class Lzw(DualExeCompressionFormat):
#     exts = ('Z', 'lzw')
#     name = 'lzw'
#     system_commands = dict(compress='compress', uncompress='uncompress')
#     compresslevel_range = (0, 7)
#     default_compresslevel = 7
#
#     def get_command(self, op, src=STDIN, stdout=True, compresslevel=7):
#         compresslevel += 9
#         if op == 'c':
#             cmd = [self.compress_path]
#             cmd.extend(('-b', compresslevel))
#         else:
#             cmd = [self.uncompress_path]
#         if stdout:
#             cmd.append('-c')
#         if src != STDIN:
#             cmd.append(src)
#         return cmd
#
# register_compression_format(Lzw)

def _resolve_exe(names):
    path = None
    name = None
    for cmd in names:
        exe = get_executable_path(cmd)
        if exe:
            path = exe
            name = cmd
            break
    return (path, name)

# Misc functions

def iter_file_chunked(fh, chunksize : 'int,>0' = 1024):
    """Returns a progress bar-wrapped iterator over a file that reads
    fixed-size chunks.
    """
    def _itr():
        while True:
            data = fh.read(chunksize)
            if data:
                yield data
            else:
                break
    try:
        name = fh.name
    except:
        name = None
    return wrap(_itr(), desc=name)
