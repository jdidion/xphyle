# -*- coding: utf-8 -*-
"""Interfaces to file compression formats. Most importantly, these attempt
to use system-level processes, which is faster than using the python
implementations.
"""

from contextlib import contextmanager
from importlib import import_module
import io
import os
import sys
from subprocess import Popen, PIPE
from xphyle.paths import get_executable_path, splitext

class CompressionError(Exception):
    pass

class FileFormat(object):
    """Base class for classes that wrap built-in python file format libraries.
    The subclass must provide the ``lib_name`` member.
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
            try:
                self._lib = import_module(self.lib_name)
            except Exception as e:
                raise CompressionError(
                    "Library does not exist: {}".format(self.lib_name)) from e
        return self._lib

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
    def __init__(self, executable_path : 'str', filename : 'str', ext : 'str',
                 command : 'str', executable_name : 'str' = None):
        self.name = filename
        self.command = command.format(
            exe=executable_path,
            filename=filename,
            ext=ext).split(' ')
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
            self.process.terminate()
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
        if retcode is not None and retcode != 0:
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
    def __init__(self, executable_path : 'str', filename : 'str', ext : 'str',
                 mode : 'str' = 'w', command : 'str' = "{exe}",
                 executable_name : 'str' = None):
        self.name = filename
        self.command = command.format(
            exe=executable_path,
            ext=ext).split(' ')
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
        except IOError as e:
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
        if retcode != 0:
            raise IOError("Output {} process terminated with "
                          "exit code {}".format(self.executable_name, retcode))

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

# Supported compression formats

compression_formats = {}
"""Dict of registered compression formats"""

def register_compression_format(format_class : 'class'):
    """Register a new compression format.
    
    Args:
        ``format_class`` -- a subclass of CompressionFormat
    """
    fmt = format_class()
    aliases = set(fmt.exts) | set(fmt.system_commands)
    aliases.add(fmt.lib_name)
    for alias in aliases:
        # TODO: warn about overriding existing format?
        compression_formats[alias] = fmt

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
        return name
    i = name.rfind(os.extsep)
    if i >= 0:
        ext = name[(i+1):]
        if ext in compression_formats:
            return ext
    return None

class CompressionFormat(FileFormat):
    """Base class for classes that provide access to system-level and
    python-level implementations of compression formats.
    """
    @property
    def executable_path(self) -> 'str':
        self._resolve_executable()
        return self._executable_path
    
    @property
    def executable_name(self) -> 'str':
        self._resolve_executable()
        return self._executable_name
    
    def _resolve_executable(self):
        if not hasattr(self, '_executable_path'):
            self._executable_path = None
            self._executable_name = None
            for cmd in self.system_commands:
                exe = get_executable_path(cmd)
                if exe:
                    self._executable_name = cmd
                    self._executable_path = exe
                    break
    
    def can_use_system_compression(self) -> 'bool':
        """Returns True if at least one command in ``self.system_commands``
        resolves to an existing, executable file.
        """
        return self.executable_path is not None
    
    def compress(self, bytes : 'bytes') -> 'bytes':
        """Compress bytes.
        
        Args:
            bytes: The bytes to compress
        
        Returns:
            The compressed bytes
        """
        return self.lib.compress(bytes)
    
    def compress_string(self, text : 'str',
                        encoding : 'str' = 'utf-8') -> 'bytes':
        """Compress a string.
        
        Args:
            text: The text to compress
            encoding: The byte encoding (utf-8)
        
        Returns:
            The compressed text, as bytes
        """
        return self.compress(text.encode(encoding))
    
    def compress_iterable(self, strings : 'list', delimiter : 'bytes' = b'',
                          encoding : 'str' = 'utf-8') -> 'bytes':
        """Compress an iterable of strings using the python-level interface.
        
        Args:
            strings: An iterable of strings
            delimiter: The delimiter (byte string) to use to separate strings
            encoding: The byte encoding (utf-8)
        
        Returns:
            The compressed text, as bytes
        """
        return self.lib.compress(delimiter.join(
            s.encode(encoding) for s in strings))
    
    def decompress(self, bytes) -> 'bytes':
        """Decompress bytes.
        
        Args:
            bytes: The compressed data
        
        Returns:
            The decompressed bytes
        """
        return self.lib.decompress(bytes)
    
    def decompress_string(self, bytes : 'bytes',
                          encoding : 'str' = 'utf-8') -> 'str':
        """Decompress bytes and return as a string.
        
        Args:
            bytes: The compressed data
            encoding: The byte encoding to use
        
        Returns:
            The decompressed data as a string
        """
        return self.decompress(bytes).decode(encoding)
    
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
        if use_system and self.can_use_system_compression():
            try:
                parts = splitext(filename)
                ext = parts[-1] if len(parts) > 1 else self.exts[0]
                if 'r' in mode:
                    z = SystemReader(
                        self.executable_path,
                        filename,
                        ext,
                        self.system_reader_command,
                        self.executable_name)
                else:
                    for c in mode:
                        if c in ('w', 'a', 'x'):
                            bin_mode = c + 'b'
                            break
                    else:
                        raise ValueError("Invalid mode: {}".format(mode))
                    z = SystemWriter(
                        self.executable_path,
                        filename,
                        ext,
                        bin_mode,
                        self.system_writer_command,
                        self.executable_name)
                if 't' in mode:
                    z = io.TextIOWrapper(z)
                return z
            except IOError:
                raise Exception('could not open system reader/writer')
                # TODO: log
                pass
        
        return self.open_file_python(filename, mode, **kwargs)
    
    def open_file_python(self, filename : 'str', mode : 'str', **kwargs):
        """Open a file using the python library.
        
        Args:
            filename: The file to open
            mode: The file open mode
            kwargs: Additional arguments to pass to the open method
        
        Returns:
            A file-like object
        """
        return self.lib.open(filename, mode, **kwargs)

class Gzip(CompressionFormat):
    """Implementation of CompressionFormat for gzip files.
    """
    exts = ('gz',)
    lib_name = 'gzip'
    system_commands = ('gzip',)
    system_reader_command = "{exe} -cd {filename}"
    system_writer_command = "{exe}"
    
    def open_file_python(self, filename, mode, **kwargs):
        z = self.lib.open(filename, mode, **kwargs)
        if 'b' in mode:
            if 'r' in mode:
                z = io.BufferedReader(z)
            else:
                z = io.BufferedWriter(z)
        return z
register_compression_format(Gzip)

class BZip2(CompressionFormat):
    """Implementation of CompressionFormat for bzip2 files.
    """
    exts = ('bz2','bzip','bzip2')
    lib_name = 'bz2'
    system_commands = ('bzip2',)
    system_reader_command = "{exe} -cd {filename}"
    system_writer_command = "{exe} -z"
    
    def open_file_python(self, filename, mode, **kwargs):
        if 't' in mode:
            mode = mode.replace('t','')
            return io.TextIOWrapper(
                self.lib.BZ2File(filename, mode, **kwargs))
        else:
            return self.lib.BZ2File(filename, mode, **kwargs)
register_compression_format(BZip2)

class Lzma(CompressionFormat):
    """Implementation of CompressionFormat for lzma (.xz) files.
    """
    exts = ('xz', 'lzma', '7z', '7zip')
    lib_name = 'lzma'
    system_commands = ('xz', 'lzma')
    system_reader_command = "{exe} -cd -S {ext} {filename}"
    system_writer_command = "{exe} -z -S {ext}"
register_compression_format(Lzma)

# Archive formats

archive_formats = {}
"""Dict of registered archive formats"""

def register_archive_format(format_class : 'class'):
    """Register a new compression format.
    
    Args:
        ``format_class`` -- a subclass of ArchiveFormat
    """
    aliases = set(format_class.exts) & set((format_class.lib_name,))
    for alias in aliases:
        # TODO: warn about overriding existing format?
        archive_formats[alias] = format_class

def get_archive_format(name : 'str') -> 'ArchiveWriter':
    """Get the ArchiveWriter class for the given name.
    """
    if name in archive_formats:
        return archive_formats[name]
    raise ValueError("Unsupported archive format: {}".format(name))

def guess_archive_format(name : 'str') -> '(str,str)':
    """Guess the archive format from the file extension.
    
    Returns:
        (archive_format, compression_format), or ``None`` if the format
        can't be determined.
    """
    file_parts = splitext(filename, False)
    if len(file_parts) < 2:
        return None
    if len(file_parts) == 2 and file_parts[2] in archive_formats:
        return (file_parts[2], None)
    # it might be a compressed archive
    if guess_compression_format(file_parts[-1]) and file_parts[-2] in archive_formats:
        return (file_parts[-2], file_parts[-1])
    return None

class ArchiveWriter(FileFormat):
    """Base class for writers that store arbitrary data and files within
    archives (e.g. tar, zip).
    
    Args:
        path: Path to the archive file
        compression: If None or False, do not compress the archive. If True,
            compress the archive using the default format. Otherwise specifies
            the name of compression format to use.
        kwargs: Additional arguments to pass to the open method
    """
    def __init__(self, path : 'str', compression : 'bool' = False, **kwargs):
        self.path = path
        self.compression = compression
        self.open_args = kwargs
        self.archive = None
    
    def open(self):
        if self.archive is not None:
            raise Exception("Archive is already open")
        self.archive = self._open()
    
    def write_file(self, path : 'str', archive_name : 'str' = None):
        """Copy a file into the archive.
        
        Args:
            path: Path to the file
            archive_name: Name to give the file within the archive. Defaults
                to ``path`` if None.
        """
        if archive_name is None:
            archive_name = path
        path = check_readable_file(path)
        self._write_file(path, archive_name)
    
    def write(self, string : 'str', archive_name : 'str'):
        """Write the contents of a string into the archive.
        
        Args:
            string: The string to write
            archive_name: The name to assign the string in the archive
        """
        raise NotImplemented()
    
    def close(self):
        if self.archive is None:
            raise Exception("Archive is not open")
        self.archive.close()
    
    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

class CompressableArchiveWriter(object):
    """Base class for an ArchiveWriter that is compressable by an external
    executable (e.g. tar.gz) rather than having its own internal compression
    scheme (e.g. zip).
    """
    def close(self):
        super(CompressedArchiveWriter, self).close()
        if self.compression:
            ctype = self.compression
            if ctype is True:
                ctype = self.default_compression
            compress_file(self.archive.name, ctype=ctype)

class ZipWriter(ArchiveWriter):
    """Simple, write-only interface to a zip file.
    """
    exts = ('zip',)
    lib_name = 'zipfile'
    
    def _open(self):
        return self.lib.Zipfile(
            self.path, 'w',
            self.lib.ZIP_DEFLATED if self.compression else self.lib.ZIP_STORED,
            **self.open_args)
    
    def _write_file(self, path, archive_name):
        self.archive.write(path, archive_name)
    
    def write(self, string, archive_name):
        self.archive.writestr(archive_name, string)
register_archive_format(ZipWriter)

class TarWriter(CompressableArchiveWriter):
    """Simple, write-only interface to a tar file.
    """
    exts = ('tar',)
    lib_name = 'tarfile'
    default_compression = 'gzip'
    
    def _open(self):
        return self.lib.TarFile(self.path, 'w', **self.open_args)
    
    def _write_file(self, path, archive_name):
        self.archive.add(path, archive_name)
    
    def write(self, string, archive_name):
        ti = tarfile.TarInfo(arcname)
        ti.frombuf(string)
        self.archive.addfile(ti)
register_archive_format(TarWriter)
