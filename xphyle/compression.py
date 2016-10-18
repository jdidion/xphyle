# -*- coding: utf-8 -*-
"""Interfaces to file compression formats. Most importantly, these attempt
to use system-level processes, which is faster than using the python
implementations.
"""

from importlib import import_module
import io
import os
import sys
from subprocess import Popen, PIPE
from xphyle.paths import get_executable_path

class FileFormat(object):
    """Base class for classes that wrap built-in python file format libraries.
    """
    _lib = None
    
    @property
    def lib(self):
        """Caches and returns the python module assocated with this file format.
        """
        if not self._lib:
            self._lib = import_module(self.lib_name)
        return self._lib

# Wrappers around system-level compression executables

class SystemReader:
    """Read from a compressed file using a system-level compression program.
    
    Args:
        filename: The compressed file to read
        command: Format string with two variables -- ``exe`` (the path to the
          system executable), and ``filename``
        executable_path: The fully resolved path the the executable
        executable_name: The display name of the executable, or ``None`` to use
          the basename of ``executable_path``
    """
    def __init__(self, filename : 'str', command : 'str',
                 executable_path : 'str', executable_name=None : 'str'):
        self.name = filename
        self.command = command.format(
            exe=executable_path, filename=filename).split(' ')
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
        filename: The compressed file to read
        mode: The write mode (w/a/x)
        command: Format string with two variables -- ``exe`` (the path to the
          system executable), and ``filename``
        executable_path: The fully resolved path the the executable
        executable_name: The display name of the executable, or ``None`` to use
          the basename of ``executable_path``
    """
    def __init__(self, filename : 'str', mode='w' : 'str', command : 'str',
                 executable_path : 'str', executable_name=None : 'str'):
        self.name = filename
        self.command = command.format(
            exe=executable_path, filename=filename).split(' ')
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
    aliases = set(fmt.exts) + set((fmt.lib_name, fmt.system_command))
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
    ext = '{}{}'.format(os.extsep, name)
    if ext in compression_formats:
        return ext
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
        if not hasattr(self, '_executable_path'):
            self._executable_path = get_executable_path(self.system_command)
        return self._executable_path
    
    def can_use_system_compression(self) -> 'bool':
        """Returns True if ``self.system_command`` resolves to an existing,
        executable file.
        """
        return self.executable_path is not None
    
    def compress_iterable(self, strings) -> 'bytes':
        """Compress an iterable of strings using the python-level interface.
        """
        self.lib.compress(b''.join(s.encode() for s in strings)))
    
    def open_file(self, filename : 'str', mode : 'str',
                  use_system=True : 'bool', **kwargs):
        """Opens a compressed file for reading or writing.
        
        If ``use_system`` is True and the system provides an accessible
        executable, then system-level compression is used. Otherwise defaults
        to using the python implementation.
        
        Args:
            filename: The file to open
            mode: The file open mode
            use_system: Whether to attempt to use system-level compression
            kwargs: Additional arguments to pass to the python-level open
                method, if system-level compression isn't used.
        
        Returns:
            A file-like object
        """
        if use_system and self.can_use_system_compression():
            try:
                if 'r' in mode:
                    z = SystemReader(
                        filename,
                        self.system_reader_command,
                        self.executable_path,
                        self.system_command)
                else:
                    z = SystemWriter(
                        filename,
                        mode,
                        self.system_writer_command,
                        self.executable_path,
                        self.system_command)
                if 't' in mode:
                    z = io.TextIOWrapper(z)
                return z
            except:
                pass
        
        return open_file_python(filename, mode, **kwargs)
    
    def open_file_python(filename : 'str', mode : 'str', **kwargs):
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
    system_command = 'gzip'
    system_reader_command = "{exe} -cd {filename}"
    system_writer_command = "{exe} {filename}"
    
    def open_file_python(filename, mode, **kwargs):
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
    system_command = 'bzip2'
    system_reader_command = "{exe} {filename}" # TODO
    system_writer_command = "{exe} {filename}"
    
    def open_file(filename, mode, use_system=False, **kwargs):
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
    exts = ('xz', 'lzma')
    lib_name = 'lzma'
    system_command = 'lzma'
    system_reader_command = "{exe} {filename}" # TODO
    system_writer_command = "{exe} {filename}"
register_compression_format(Lzma)

# Archive formats

archive_formats = {}
"""Dict of registered archive formats"""

def register_archive_format(format_class : 'class'):
    """Register a new compression format.
    
    Args:
        ``format_class`` -- a subclass of ArchiveFormat
    """
    aliases = set(fmt.exts) + set((fmt.lib_name,))
    for alias in aliases:
        # TODO: warn about overriding existing format?
        archive_formats[alias] = fmt

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
    def __init__(self, path : 'str', compression=False : 'bool', **kwargs):
        self.path = path
        self.compression = compression
        self.open_args = kwargs
        self.archive = None
    
    def open(self):
        if self.archive is not None:
            raise Exception("Archive is already open")
        self.archive = self._open()
    
    def write_file(self, path : 'str', archive_name=None : 'str'):
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
