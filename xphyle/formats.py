# -*- coding: utf-8 -*-
"""Interfaces to compression file formats.
Magic numbers from: https://en.wikipedia.org/wiki/List_of_file_signatures
"""
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from importlib import import_module
import io
import os
from subprocess import Popen, PIPE

from xphyle.paths import (
    STDIN, EXECUTABLE_CACHE, check_readable_file, check_writable_file,
    split_path)
from xphyle.progress import PROCESS_PROGRESS, iter_file_chunked
from xphyle.types import (
    FileMode, ModeCoding, ModeArg, PathOrFile, FileLike, Union, Callable,
    Iterable, Iterator, List, Tuple, ModuleType, PathLike, FileLikeInterface,
    FileLikeBase, AnyStr, AnyChar, IO, cast)

class ThreadsVar(object):
    """Maintain ``threads`` variable.
    """
    def __init__(self, default_value: int = 1) -> None:
        self.threads = default_value
        self.default_value = default_value
    
    def update(self, threads: Union[bool, int] = True) -> None:
        """Update the number of threads to use.
        
        Args:
            threads: True = use all available cores; False or an int <= 1 means
                single-threaded; None means reset to the default value;
                otherwise an integer number of threads.
        """
        if threads is None:
            self.threads = self.default_value
        elif threads is False:
            self.threads = 1
        elif threads is True:
            import multiprocessing
            self.threads = multiprocessing.cpu_count()
        elif threads < 1:
            self.threads = 1
        else:
            self.threads = threads # pylint: disable=redefined-variable-type

THREADS = ThreadsVar()
"""Number of concurrent threads that can be used by formats that support 
parallelization.
"""

# File formats
# pylint: disable=no-member

class FileFormat(object):
    """Base class for classes that wrap built-in python file format libraries.
    The subclass must provide the ``name`` member.
    """
    # ISSUE: types is in mypy
    _lib = None # type: ModuleType
    
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

# Wrappers around system-level compression executables

class SystemIO(FileLikeBase):
    """Base class for SystemReader and SystemWriter.
    
    Args:
        name: The file name.
    """
    def __init__(self, path: PathLike) -> None:
        self._name = str(path)
        self._closed = False
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def closed(self) -> bool:
        return self._closed


class SystemReader(SystemIO):
    """Read from a compressed file using a system-level compression program.
    
    Args:
        executable_path: The fully resolved path the the system executable
        path: The compressed file to read
        command: List of command arguments.
        executable_name: The display name of the executable, or ``None`` to use
          the basename of ``executable_path``
    """
    # pylint: disable=no-self-use
    def __init__(
            self, executable_path: PathLike, path: PathLike, 
            command: List[str], executable_name: str = None) -> None:
        super().__init__(path)
        self.command = command
        self.executable_name = (
            executable_name or os.path.basename(str(executable_path)))
        self.process = Popen(self.command, stdout=PIPE)
    
    @property
    def mode(self): # pragma: no-cover
        return 'rb'
    
    def readable(self) -> bool:
        """Implementing file interface; returns True.
        """
        return True
    
    def flush(self) -> None:
        """Implementing file interface; no-op.
        """
        pass
    
    def close(self) -> None:
        """Close the reader; terminates the underlying process.
        """
        self._closed = True
        retcode = self.process.poll()
        if retcode is None:
            # still running
            self.process.terminate() # pragma: no-cover
        self._raise_if_error()

    def __iter__(self) -> Iterator:
        yield from self.process.stdout
        self.process.wait()
        self._raise_if_error()

    def _raise_if_error(self) -> None:
        """Raise EOFError if process is not running anymore and the
        exit code is nonzero.
        """
        retcode = self.process.poll()
        if retcode is not None and retcode != 0: # pragma: no-cover
            raise EOFError(
                "{} process returned non-zero exit code {}. "
                "Is the input file truncated or corrupt?".format(
                    self.executable_name, retcode))
    
    def read(self, *args) -> bytes:
        """Read bytes from the stream. Arguments are passed through to the
        subprocess ``read`` method.
        """
        data = self.process.stdout.read(*args)
        if len(args) == 0 or args[0] <= 0:
            # wait for process to terminate until we check the exit code
            self.process.wait()
        self._raise_if_error()
        return data


class SystemWriter(SystemIO):
    """Write to a compressed file using a system-level compression program.
    
    Args:
        executable_path: The fully resolved path the the system executable.
        path: The compressed file to read.
        mode: The write mode (w/a/x).
        command: Format string with two variables -- ``exe`` (the path to the
          system executable), and ``path``.
        executable_name: The display name of the executable, or ``None`` to use
          the basename of ``executable_path``.
    """
    def __init__(
            self, executable_path: PathLike, path: PathLike, 
            mode: ModeArg = 'w', command: List[str] = None, 
            executable_name: str = None) -> None:
        super().__init__(path)
        self.executable_name = (
            executable_name or os.path.basename(str(executable_path)))
        self.command = command or [self.executable_name]
        if isinstance(mode, str):
            mode = FileMode(mode)
        self.outfile = open(str(path), mode.value)
        self.devnull = open(os.devnull, 'w')
        try:
            self.process = Popen(
                self.command, stdin=PIPE, stdout=self.outfile,
                stderr=self.devnull)
        except IOError: # pragma: no-cover
            self.outfile.close()
            self.devnull.close()
            raise
    
    @property
    def mode(self): # pragma: no-cover
        return 'wb'
    
    def writable(self) -> bool:
        """Implementing file interface; returns True.
        """
        return True
    
    def write(self, arg) -> int:
        """Write to stdin of the underlying process.
        """
        return self.process.stdin.write(arg)
    
    def flush(self) -> None:
        """Flush stdin of the underlying process.
        """
        self.process.stdin.flush()
    
    def close(self) -> None:
        """Close the writer; terminates the underlying process.
        """
        self._closed = True
        self.process.stdin.close()
        retcode = self.process.wait()
        self.outfile.close()
        self.devnull.close()
        if retcode != 0: # pragma: no-cover
            raise IOError(
                "Output {} process terminated with exit code {}".format(
                    self.executable_name, retcode))


class CompressionFormat(FileFormat, metaclass=ABCMeta):
    """Base class for classes that provide access to system-level and
    python-level implementations of compression formats.
    """
    @property
    @abstractmethod
    def name(self) -> str:
        """The canonical format name.
        """
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def exts(self) -> Tuple[str, ...]:
        """The valid file extensions.
        """
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def system_commands(self) -> Tuple[str, ...]:
        """The names of the system-level commands, in order of preference.
        """
        raise NotImplementedError()
    
    @property
    def default_compresslevel(self) -> int: # pragma: no-cover
        """The default compression level, if compression is supported and is
        user-configurable, otherwise None.
        """
        return None
    
    @property
    def compresslevel_range(self) -> Tuple[int, int]: # pragma: no-cover
        """The range of valid compression levels: (lowest, highest).
        """
        return None
    
    @property
    @abstractmethod
    def compress_path(self) -> PathLike:
        """The path of the compression program.
        """
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def decompress_path(self) -> PathLike:
        """The path of the decompression program.
        """
        raise NotImplementedError()
    
    @property
    def compress_name(self) -> str:
        """The name of the compression program.
        """
        return os.path.basename(str(self.compress_path))
    
    @property
    def decompress_name(self) -> str:
        """The name of the decompression program.
        """
        return os.path.basename(str(self.decompress_path))
    
    @property
    @abstractmethod
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        """The initial bytes that indicate the file type.
        """
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def mime_types(self) -> Tuple[str, ...]:
        """The MIME types.
        """
        raise NotImplementedError()
    
    @property
    def aliases(self) -> Tuple:
        """All of the aliases by which this format is known.
        """
        aliases = set(self.exts)
        #if isinstance(fmt.system_commands, dict):
        #    aliases = aliases | set(fmt.system_commands.values())
        #else:
        aliases.update(self.system_commands)
        aliases.add(self.name)
        return tuple(aliases)
    
    @property
    def default_ext(self) -> str:
        """The default file extension for this format.
        """
        return self.exts[0]
    
    def _get_compresslevel(self, level: int = None) -> int:
        if level is None:
            level = self.default_compresslevel
        elif level < self.compresslevel_range[0]:
            level = self.compresslevel_range[0]
        elif level > self.compresslevel_range[1]:
            level = self.compresslevel_range[1]
        return level
    
    @property
    def can_use_system_compression(self) -> bool:
        """Whether at least one command in ``self.system_commands``
        resolves to an existing, executable file.
        """
        return self.compress_path is not None
    
    @property
    def can_use_system_decompression(self) -> bool:
        """Whether at least one command in ``self.system_commands``
        resolves to an existing, executable file.
        """
        return self.decompress_path is not None
    
    def compress(self, raw_bytes: bytes, **kwargs) -> bytes:
        """Compress bytes.
        
        Args:
            raw_bytes: The bytes to compress
            kwargs: Additional arguments to compression function.
        
        Returns:
            The compressed bytes
        """
        kwargs['compresslevel'] = self._get_compresslevel(
            kwargs.get('compresslevel', None))
        return self.lib.compress(raw_bytes, **kwargs)
    
    def compress_string(
            self, text: str, encoding: str = 'utf-8', **kwargs) -> bytes:
        """Compress a string.
        
        Args:
            text: The text to compress
            encoding: The byte encoding (utf-8)
            kwargs: Additional arguments to compression function
        
        Returns:
            The compressed text, as bytes
        """
        return self.compress(text.encode(encoding), **kwargs)
    
    def compress_iterable(
            self, strings: Iterable[str], delimiter: bytes = b'',
            encoding: str = 'utf-8', **kwargs) -> bytes:
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
    
    def decompress(self, compressed_bytes, **kwargs) -> bytes:
        """Decompress bytes.
        
        Args:
            compressed_bytes: The compressed data
            kwargs: Additional arguments to the decompression function
        
        Returns:
            The decompressed bytes
        """
        return self.lib.decompress(compressed_bytes, **kwargs)
    
    def decompress_string(
            self, compressed_bytes: bytes, encoding: str = 'utf-8',
            **kwargs) -> str:
        """Decompress bytes and return as a string.
        
        Args:
            compressed_bytes: The compressed data
            encoding: The byte encoding to use
            kwargs: Additional arguments to the decompression function
        
        Returns:
            The decompressed data as a string
        """
        return self.decompress(compressed_bytes, **kwargs).decode(encoding)
    
    @abstractmethod
    def get_command(
            self, operation: str, src: str = STDIN, stdout: bool = True,
            compresslevel: int = None) -> List[str]:
        """Build the command for the system executable.
        
        Args:
            operation: 'c' = compress, 'd' = decompress
            src: The source file path, or STDIN if input should be read from
                stdin
            stdout: Whether output should go to stdout
            compresslevel: Integer compression level; typically 1-9
        
        Returns:
            List of command arguments
        """
        raise NotImplementedError()
    
    def open_file(
            self, path: str, mode: ModeArg, use_system: bool = True,
            **kwargs) -> FileLike:
        """Opens a compressed file for reading or writing.
        
        If ``use_system`` is True and the system provides an accessible
        executable, then system-level compression is used. Otherwise defaults
        to using the python implementation.
        
        Args:
            path: The path of the file to open.
            mode: The file open mode.
            use_system: Whether to attempt to use system-level compression.
            kwargs: Additional arguments to pass to the python-level open
                method, if system-level compression isn't used.
        
        Returns:
            A file-like object.
        """
        if isinstance(mode, str):
            mode = FileMode(mode)
        
        if use_system:
            # pylint: disable=redefined-variable-type
            gzfile = None # type: FileLikeInterface
            if mode.readable and self.can_use_system_compression:
                gzfile = SystemReader(
                    self.compress_path,
                    path,
                    self.get_command('d', src=path),
                    self.compress_name)
            elif not mode.readable and self.can_use_system_decompression:
                bin_mode = FileMode(
                    access=mode.access, coding=ModeCoding.BINARY)
                gzfile = SystemWriter(
                    self.decompress_path,
                    path,
                    bin_mode,
                    self.get_command('c'),
                    self.decompress_name)
            if gzfile:
                if mode.text:
                    return io.TextIOWrapper(gzfile)
                else:
                    return gzfile
        
        return self.open_file_python(path, mode, **kwargs)
    
    def open_file_python(
            self, path_or_file: PathOrFile, mode: ModeArg,
            **kwargs) -> FileLike:
        """Open a file using the python library.
        
        Args:
            f: The file to open -- a path or open file object.
            mode: The file open mode.
            kwargs: Additional arguments to pass to the open method.
        
        Returns:
            A file-like object.
        """
        if isinstance(mode, str):
            mode = FileMode(mode)
        return self.lib.open(path_or_file, mode.value, **kwargs)
    
    def compress_file(
            self, source: PathOrFile, dest: PathOrFile = None,
            keep: bool = True, compresslevel: int = None,
            use_system: bool = True, **kwargs) -> str:
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
            Path to the destination file.
        """
        source_is_path = isinstance(source, str)
        if source_is_path:
            source_path = str(source)
            check_readable_file(source_path)
        else:
            source_io = cast(IO, source)
            source_path = source_io.name
            # pragma: no-cover
            try:
                source_io.fileno()
            except OSError:
                use_system = False
        
        if dest is None:
            dest = "{}.{}".format(source_path, self.default_ext)
            dest_is_path = True
        else:
            dest_is_path = isinstance(dest, str)
        if dest_is_path:
            check_writable_file(str(dest))
        
        try:
            if use_system and self.can_use_system_compression:
                if source_is_path:
                    cmd_src = str(source)
                    prc_src = None
                else:
                    cmd_src = STDIN
                    prc_src = cast(FileLike, source)
                if dest_is_path:
                    dest_name = str(dest)
                    dest_file = open(dest_name, 'wb')
                else:
                    dest_file = cast(FileLike, dest)
                    dest_name = dest_file.name
                cmd = self.get_command(
                    'c', src=cmd_src, compresslevel=compresslevel)
                proc = PROCESS_PROGRESS.wrap(
                    cmd, stdin=prc_src, stdout=dest_file)
                proc.communicate()
            else:
                if source_is_path:
                    source_file = open(str(source), 'rb')
                else:
                    source_file = cast(FileLike, source)
                dest_name = str(dest)
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
                    cast(FileLike, source).close()
                os.remove(source_path)
        finally:
            if dest_is_path:
                dest_file.close()
        
        return dest_name
    
    def decompress_file(
            self, source: PathOrFile, dest: PathOrFile = None,
            keep: bool = True, use_system: bool = True, **kwargs) -> str:
        """Decompress data from one file and write to another.
        
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
            source_path = str(source)
            check_readable_file(source_path)
        else:
            source_path = cast(FileLike, source).name
        source_parts = split_path(source_path)
        
        if dest is None:
            if len(source_parts) > 2:
                dest = (
                    os.path.join(*source_parts[0:2]) +
                    ''.join(source_parts[2:-1]))
                dest_is_path = True
            else:
                raise Exception("Cannot determine path for decompressed file")
        else:
            dest_is_path = isinstance(dest, str)
        if dest_is_path:
            dest_name = str(check_writable_file(str(dest)))
            dest_file = open(dest_name, 'wb')
        else:
            dest_file = cast(FileLike, dest)
            dest_name = dest_file.name
            # pragma: no-cover
            try:
                dest_file.fileno()
            except OSError:
                use_system = False
        
        try:
            if use_system and self.can_use_system_decompression:
                src = str(source) if source_is_path else STDIN
                cmd = self.get_command('d', src=src)
                psrc = None if source_is_path else cast(FileLike, source)
                proc = PROCESS_PROGRESS.wrap(cmd, stdin=psrc, stdout=dest_file)
                proc.communicate()
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
                    cast(FileLike, source).close()
                os.remove(source_path)
        finally:
            if dest_is_path:
                dest_file.close()
        
        return dest_name

class SingleExeCompressionFormat(CompressionFormat): # pylint: disable=abstract-method
    """Base class form ``CompressionFormat``s that use the same executable for
    compressing and decompressing.
    """
    def __init__(self):
        self._executable_path = None
        self._executable_name = None
        self._resolved = False
    
    @property
    def executable_path(self) -> PathLike:
        """The path of the system executable.
        """
        self._resolve_executable()
        return self._executable_path
    
    @property
    def executable_name(self) -> str:
        """The name of the system executable.
        """
        self._resolve_executable()
        return self._executable_name
    
    @property
    def compress_path(self) -> PathLike:
        return self.executable_path
    
    @property
    def decompress_path(self) -> PathLike:
        return self.executable_path
    
    @property
    def compress_name(self) -> str:
        return self.executable_name
    
    @property
    def decompress_name(self) -> str:
        return self.executable_name
    
    def _resolve_executable(self) -> None:
        if not self._resolved:
            exe = EXECUTABLE_CACHE.resolve_exe(self.system_commands)
            if exe:
                self._executable_path, self._executable_name = exe
            self._resolved = True

class GzipBase(SingleExeCompressionFormat):
    """Base class for gzip and bgzip files.
    """
    def open_file_python(
            self, path_or_file: PathOrFile, mode: ModeArg,
            **kwargs) -> FileLike:
        # pylint: disable=redefined-variable-type
        if isinstance(mode, str):
            mode = FileMode(mode)
        compressed_file = self.lib.open(path_or_file, mode.value, **kwargs)
        if mode.binary:
            if mode.readable:
                compressed_file = io.BufferedReader(compressed_file)
            else:
                compressed_file = io.BufferedWriter(compressed_file)
        return compressed_file


class Gzip(GzipBase):
    """Implementation of CompressionFormat for gzip files.
    """
    @property
    def name(self) -> str:
        return 'gzip'
    
    @property
    def exts(self) -> Tuple[str, ...]:
        return ('gz',)
    
    @property
    def system_commands(self) -> Tuple[str, ...]:
        return ('pigz','gzip')
    
    @property
    def default_compresslevel(self) -> int:
        return 6
    
    @property
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        return ((0x1f, 0x8b),)
    
    @property
    def mime_types(self) -> Tuple[str, ...]:
        return (
            'application/gz',
            'application/gzip',
            'application/x-gz',
            'application/x-gzip')
    
    @property
    def compresslevel_range(self) -> Tuple[int, int]:
        """The compression level; pigz allows 0-11 (har har) while
        gzip allows 0-9.
        """
        if self.executable_name == 'pigz':
            return (0, 11)
        else:
            return (1, 9)
    
    def get_command(
            self, operation, src=STDIN, stdout=True, compresslevel=None
            ) -> List[str]:
        cmd = [str(self.executable_path)]
        if operation == 'c':
            compresslevel = self._get_compresslevel(compresslevel)
            cmd.append('-{}'.format(compresslevel))
        elif operation == 'd':
            cmd.append('-d')
        if stdout:
            cmd.append('-c')
        threads = THREADS.threads
        if self.executable_name == 'pigz' and threads > 1:
            cmd.extend(('-p', str(threads)))
        if src != STDIN:
            cmd.append(src)
        return cmd


class BGzip(GzipBase):
    """bgzip is block gzip. bgzip files are compatible with gzip. Typically,
    this format is only used when specifically requested, or when a bgzip
    file specifically has a .bgz (rather than .gz) extension.
    """
    @property
    def name(self) -> str:
        return 'bgzip'
    
    # TODO: This is a bug: bgzip doesn't actually allow bgz extension, but the
    # way Formats currently works does not support on extension being used with
    # multiple formats.
    
    @property
    def exts(self) -> Tuple[str, ...]:
        return ('bgz',)
    
    @property
    def system_commands(self) -> Tuple[str, ...]:
        return ('bgzip',)
    
    @property
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        return ((0x1f, 0x8b, 0x08, 0x04),)
    
    @property
    def mime_types(self) -> Tuple[str, ...]:
        return (
            'application/bgz',
            'application/bgzip',
            'application/x-bgz',
            'application/x-bgzip')
    
    def get_command(
            self, operation, src=STDIN, stdout=True, compresslevel=None
            ) -> List[str]:
        cmd = [str(self.executable_path)]
        if operation == 'd':
            cmd.append('-d')
        if stdout:
            cmd.append('-c')
        threads = THREADS.threads
        if threads > 1:
            cmd.extend(('-@', str(threads)))
        if src != STDIN:
            cmd.append(src)
        return cmd


class BZip2(SingleExeCompressionFormat):
    """Implementation of CompressionFormat for bzip2 files.
    """
    @property
    def name(self) -> str:
        return 'bz2'
    
    @property
    def exts(self) -> Tuple[str, ...]:
        return ('bz2', 'bzip', 'bzip2')
    
    @property
    def system_commands(self) -> Tuple[str, ...]:
        return ('pbzip2', 'bzip2')
    
    @property
    def compresslevel_range(self) -> Tuple[int, int]:
        return (1, 9)
    
    @property
    def default_compresslevel(self) -> int:
        return 6
    
    @property
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        return ((0x42, 0x5A, 0x68),)
    
    @property
    def mime_types(self) -> Tuple[str, ...]:
        return (
            'application/bz2',
            'application/bzip2',
            'application/x-bz2',
            'application/x-bzip2')
    
    def get_command(
            self, operation, src=STDIN, stdout=True, compresslevel=6
            ) -> List[str]:
        cmd = [str(self.executable_path)]
        if operation == 'c':
            compresslevel = self._get_compresslevel(compresslevel)
            cmd.append('-{}'.format(compresslevel))
            cmd.append('-z')
        elif operation == 'd':
            cmd.append('-d')
        if stdout:
            cmd.append('-c')
        threads = THREADS.threads
        if self.executable_name == 'pbzip2' and threads > 1:
            cmd.append('-p{}'.format(threads))
        if src != STDIN:
            cmd.append(src)
        return cmd
    
    def open_file_python(
            self, path_or_file: PathOrFile, mode: ModeArg,
            **kwargs) -> FileLike:
        if isinstance(mode, str):
            mode = FileMode(mode)
        if mode.text:
            return io.TextIOWrapper(
                self.lib.BZ2File(path_or_file, mode.access.value, **kwargs))
        else:
            return self.lib.BZ2File(path_or_file, mode.value, **kwargs)


class Lzma(SingleExeCompressionFormat):
    """Implementation of CompressionFormat for lzma (.xz) files.
    """
    @property
    def name(self) -> str:
        return 'lzma'
    
    @property
    def exts(self) -> Tuple[str, ...]:
        return ('xz', 'lzma', '7z', '7zip')
    
    @property
    def system_commands(self) -> Tuple[str, ...]:
        return ('xz', 'lzma')
    
    @property
    def compresslevel_range(self) -> Tuple[int, int]:
        return (0, 9)
    
    @property
    def default_compresslevel(self) -> int:
        return 6
    
    @property
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        return (
            (0x4C, 0x5A, 0x49, 0x50), # lz
            (0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00), # xz
            (0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C) # 7z
        )
    
    @property
    def mime_types(self) -> Tuple[str, ...]:
        return (
            'application/lzma',
            'application/x-lzma',
            'application/xz',
            'application/x-xz',
            'application/7z-compressed'
            'application/x-7z-compressed')
    
    def get_command(
            self, operation, src=STDIN, stdout=True, compresslevel=6
            ) -> List[str]:
        cmd = [str(self.executable_path)]
        if operation == 'c':
            compresslevel = self._get_compresslevel(compresslevel)
            cmd.append('-{}'.format(compresslevel))
            cmd.append('-z')
        elif operation == 'd':
            cmd.append('-d')
        if stdout:
            cmd.append('-c')
        threads = THREADS.threads
        if threads > 1:
            cmd.extend(('-T', str(threads)))
        if src != STDIN:
            cmd.append(src)
        return cmd
    
    def compress(self, raw_bytes, **kwargs) -> bytes:
        kwargs = dict(
            (k, kwargs[k])
            for k, v in kwargs.items() if k in (
                'format','check','preset','filter'))
        return self.lib.compress(raw_bytes, **kwargs)

# class DualExeCompressionFormat(CompressionFormat):
#     """CompressionFormat that uses the same executable for compressing and
#     decompressing.
#     """
#     @property
#     def compress_path(self) -> str:
#         self._resolve_compress()
#         return self._compress_path
#
#     @property
#     def decompress_path(self) -> str:
#         self._resolve_decompress()
#         return self._decompress_path
#
#     @property
#     def compress_name(self) -> str:
#         self._resolve_compress()
#         return self._compress_name
#
#     @property
#     def decompress_name(self) -> str:
#         self._resolve_decompress()
#         return self._decompress_name
#
#     def _resolve_compress(self):
#         if not hasattr(self, '_compress_path'):
#             self._compress_path, self._compress_name = _resolve_exe(
#                 self.system_commands['compress'])
#
#     def _resolve_decompress(self):
#         if not hasattr(self, '_decompress_path'):
#             self._decompress_path, self._decompress_name = _resolve_exe(
#                 self.system_commands['decompress'])
#
# class Lzw(DualExeCompressionFormat):
#     exts = ('Z', 'lzw')
#     name = 'lzw'
#     system_commands = dict(compress='compress', decompress='decompress')
#     compresslevel_range = (0, 7)
#     default_compresslevel = 7
#
#     def get_command(self, operation, src=STDIN, stdout=True, compresslevel=7):
#         compresslevel += 9
#         if operation == 'c':
#             cmd = [self.compress_path]
#             cmd.extend(('-b', compresslevel))
#         else:
#             cmd = [self.decompress_path]
#         if stdout:
#             cmd.append('-c')
#         if src != STDIN:
#             cmd.append(src)
#         return cmd
#
# register_compression_format(Lzw)

# Supported compression formats
class Formats(object):
    """Manages a set of compression formats.
    """
    def __init__(self):
        self.compression_formats = {}
        """Dict of registered compression formats"""
        self.compression_format_aliases = {}
        """Dict mapping aliases to compression format names."""
        self.magic_bytes = defaultdict(lambda: [])
        """Dict mapping the first byte in a 'magic' sequence to a tuple of
        (format, rest_of_sequence)
        """
        self.max_magic_bytes = 0
        """Maximum number of bytes in a registered magic byte sequence"""
        self.mime_types = {}
        """Dict mapping MIME types to file formats"""

    def register_compression_format(
            self, format_class: Callable[[], CompressionFormat]) -> None:
        """Register a new compression format.
        
        Args:
            format_class: a subclass of CompressionFormat
        """
        fmt = format_class()
        self.compression_formats[fmt.name] = fmt
        for alias in fmt.aliases:
            # TODO: warn about overriding existing format?
            self.compression_format_aliases[alias] = fmt.name
        for magic in fmt.magic_bytes:
            self.max_magic_bytes = max(self.max_magic_bytes, len(magic))
            self.magic_bytes[magic[0]].append((fmt.name, magic[1:]))
        for mime in fmt.mime_types:
            self.mime_types[mime] = fmt.name
    
    def list_compression_formats(self) -> Tuple:
        """Returns a list of all registered compression formats.
        """
        return tuple(self.compression_formats.keys())
    
    def get_compression_format(self, name: str) -> CompressionFormat:
        """Returns the CompressionFormat associated with the given name.
        
        Raises:
            ValueError if that format is not supported.
        """
        if name in self.compression_format_aliases:
            name = self.compression_format_aliases[name]
            return self.compression_formats[name]
        raise ValueError("Unsupported compression format: {}".format(name))
    
    def get_compression_format_name(self, alias: str):
        """Returns the cannonical name for the given alias.
        """
        if alias in self.compression_formats:
            return alias
        return self.compression_format_aliases.get(alias, None)
    
    def guess_compression_format(self, name: str) -> str:
        """Guess the compression format by name or file extension.
        
        Returns:
            The format name, or ``None`` if it could not be guessed.
        """
        if name in self.compression_format_aliases:
            return self.compression_format_aliases[name]
        i = name.rfind(os.extsep)
        if i >= 0:
            ext = name[(i+1):]
            if ext in self.compression_format_aliases:
                return self.compression_format_aliases[ext]
        return None
    
    def guess_format_from_file_header(self, path: str) -> str:
        """Guess file format from 'magic bytes' at the beginning of the file.
        
        Note that ``path`` must be openable and readable. If it is a named pipe
        or other pseudo-file type, the magic bytes will be destructively
        consumed and thus will open correctly.
        
        Args:
            path: Path to the file
        
        Returns:
            The format name, or ``None`` if it could not be guessed.
        """
        with open(path, 'rb') as infile:
            magic = infile.read(self.max_magic_bytes)
        return self.guess_format_from_header_bytes(magic)
    
    def guess_format_from_buffer(self, buffer: io.BufferedReader) -> str:
        """Guess file format from a byte buffer that provides a ``peek`` 
        method.
        
        Args:
            buffer: The buffer object
        
        Returns:
            The format name, or ``None`` if it could not be guessed.
        """
        magic = buffer.peek(self.max_magic_bytes)
        return self.guess_format_from_header_bytes(magic)
    
    def guess_format_from_header_bytes(self, header_bytes: bytes) -> str:
        """Guess file format from a sequence of bytes from a file header.
        
        Args:
            header_bytes: The bytes
        
        Returns:
            The format name, or ``None`` if it could not be guessed.
        """
        num_bytes = len(header_bytes)
        if num_bytes > 0:
            if header_bytes[0] in self.magic_bytes.keys():
                # check candidates by decreasing header length
                candidates = sorted(
                    self.magic_bytes[header_bytes[0]],
                    key=lambda x: len(x[1]),
                    reverse=True)
                for fmt, tail in candidates:
                    if (num_bytes > len(tail) and tuple(
                            header_bytes[i]
                            for i in range(1, len(tail)+1)) == tail):
                        return fmt
        return None
    
    def get_format_for_mime_type(self, mime_type: str) -> str:
        """Returns the file format associated with a MIME type, or None if no
        format is associated with the mime type.
        """
        return self.mime_types.get(mime_type, None)

FORMATS = Formats()
FORMATS.register_compression_format(Gzip)
FORMATS.register_compression_format(BGzip)
FORMATS.register_compression_format(BZip2)
FORMATS.register_compression_format(Lzma)
