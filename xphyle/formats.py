# -*- coding: utf-8 -*-
"""Interfaces to compression file formats.
Magic numbers from: https://en.wikipedia.org/wiki/List_of_file_signatures
"""
from abc import ABC, ABCMeta, abstractmethod
from collections import defaultdict
from importlib import import_module
import io
from io import UnsupportedOperation
import os
from pathlib import Path, PurePath
import re
from subprocess import Popen, PIPE, CalledProcessError, check_output
from typing import (
    Callable,
    Iterable,
    Iterator,
    List,
    Tuple,
    Set,
    IO,
    Optional,
    Union,
    cast,
)

# noinspection PyUnresolvedReferences
from types import ModuleType
from xphyle.paths import (
    STDIN,
    EXECUTABLE_CACHE,
    check_readable_file,
    check_writable_file,
    check_std,
    split_path,
    deprecated_str_to_path,
)
from xphyle.progress import PROCESS_PROGRESS, iter_file_chunked
from xphyle.types import (
    FileMode,
    FileLike,
    FileLikeInterface,
    FileLikeBase,
    ModeCoding,
    ModeArg,
    PathOrFile,
)


class ThreadsVar:
    """Maintain ``threads`` variable.
    """

    def __init__(self, default_value: int = 1) -> None:
        self.threads = default_value
        self.default_value = default_value

    def update(self, threads: Optional[Union[bool, int]] = True) -> None:
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
            self.threads = threads  # pylint: disable=redefined-variable-type


THREADS = ThreadsVar()
"""Number of concurrent threads that can be used by formats that support 
parallelization.
"""


# File formats
# pylint: disable=no-member


class FileFormat(ABC):
    """Base class for classes that wrap built-in python file format libraries.
    The subclass must provide the ``name`` member.
    """

    _lib: ModuleType = None

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    def module_name(self):
        return self.name

    @property
    def lib(self):
        """Caches and returns the python module assocated with this file format.

        Returns:
            The module

        Raises:
            ImportError if the module cannot be imported.
        """
        if not self._lib:
            self._lib = import_module(self.module_name)
        return self._lib


# Wrappers around system-level compression executables


class SystemIO(FileLikeBase, metaclass=ABCMeta):
    """Base class for SystemReader and SystemWriter.

    Args:
        path: The file path.
    """

    @deprecated_str_to_path(1, "path")
    def __init__(self, path: PurePath) -> None:
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
    @deprecated_str_to_path(1, "executable_path", 2, "path")
    def __init__(
        self,
        executable_path: PurePath,
        path: PurePath,
        command: List[str],
        executable_name: str = None,
    ) -> None:
        super().__init__(path)
        self.command = command
        self.executable_name = executable_name or executable_path.name
        self.process = Popen(self.command, stdout=PIPE)

    @property
    def mode(self):  # pragma: no-cover
        return "rb"

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
            self.process.terminate()  # pragma: no-cover
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
        if retcode is not None and retcode != 0:  # pragma: no-cover
            raise EOFError(
                f"{self.executable_name} process returned non-zero exit code "
                f"{retcode}. Is the input file truncated or corrupt?"
            )

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

    @deprecated_str_to_path(1, "executable_path", 2, "path")
    def __init__(
        self,
        executable_path: PurePath,
        path: PurePath,
        mode: ModeArg = "w",
        command: List[str] = None,
        executable_name: str = None,
    ) -> None:
        super().__init__(path)
        self.executable_name = executable_name or executable_path.name
        self.command = command or [self.executable_name]
        if isinstance(mode, str):
            mode = FileMode(mode)
        self.outfile = open(path, mode.value)
        self.devnull = open(os.devnull, "w")
        try:
            self.process = Popen(
                self.command, stdin=PIPE, stdout=self.outfile, stderr=self.devnull
            )
        except IOError:  # pragma: no-cover
            self.outfile.close()
            self.devnull.close()
            raise

    @property
    def mode(self):  # pragma: no-cover
        return "wb"

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
        if retcode != 0:  # pragma: no-cover
            raise IOError(
                f"Output {self.executable_name} process terminated with exit "
                f"code {retcode}"
            )


class CompressionFormat(FileFormat):
    """Base class for classes that provide access to system-level and
    python-level implementations of compression formats.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """The canonical format name.
        """
        pass

    @property
    @abstractmethod
    def exts(self) -> Tuple[str, ...]:
        """The commonly used file extensions.
        """
        pass

    @property
    def allowed_exts(self) -> Tuple[str, ...]:
        """Extensions that are allowed to be used. Defaults to `self.exts`.
        """
        return self.exts

    @property
    def system_commands(self) -> Tuple[str, ...]:
        """The names of the system-level commands, in order of preference.
        """
        return self.name,

    @property
    def default_compresslevel(self) -> Optional[int]:  # pragma: no-cover
        """The default compression level, if compression is supported and is
        user-configurable, otherwise None.
        """
        return None

    @property
    def compresslevel_range(self) -> Optional[Tuple[int, int]]:  # pragma: no-cover
        """The range of valid compression levels: (lowest, highest).
        """
        return None

    @property
    @abstractmethod
    def compress_path(self) -> PurePath:
        """The path of the compression program.
        """
        pass

    @property
    @abstractmethod
    def decompress_path(self) -> PurePath:
        """The path of the decompression program.
        """
        pass

    @property
    def compress_name(self) -> str:
        """The name of the compression program.
        """
        return self.compress_path.name

    @property
    def decompress_name(self) -> str:
        """The name of the decompression program.
        """
        return self.decompress_path.name

    @property
    @abstractmethod
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        """The initial bytes that indicate the file type.
        """
        pass

    @property
    @abstractmethod
    def mime_types(self) -> Tuple[str, ...]:
        """The MIME types.
        """
        pass

    @property
    def aliases(self) -> Tuple:
        """All of the aliases by which this format is known.
        """
        aliases = set(self.exts)
        # if isinstance(fmt.system_commands, dict):
        #     aliases = aliases | set(fmt.system_commands.values())
        # else:
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
        kwargs["compresslevel"] = self._get_compresslevel(
            kwargs.get("compresslevel", None)
        )
        return self.lib.compress(raw_bytes, **kwargs)

    def compress_string(self, text: str, encoding: str = "utf-8", **kwargs) -> bytes:
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
        self,
        strings: Iterable[str],
        delimiter: bytes = b"",
        encoding: str = "utf-8",
        **kwargs,
    ) -> bytes:
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
            delimiter.join(s.encode(encoding) for s in strings), **kwargs
        )

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
        self, compressed_bytes: bytes, encoding: str = "utf-8", **kwargs
    ) -> str:
        """Decompress bytes and return as a string.

        Args:
            compressed_bytes: The compressed data
            encoding: The byte encoding to use
            kwargs: Additional arguments to the decompression function

        Returns:
            The decompressed data as a string
        """
        return self.decompress(compressed_bytes, **kwargs).decode(encoding)

    # TODO: create an Enum for operation

    @abstractmethod
    def get_command(
        self,
        operation: str,
        src: PurePath = STDIN,
        stdout: bool = True,
        compresslevel: int = None,
    ) -> List[str]:
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
        pass

    def handle_command_return(
        self, returncode: int, cmd: List[str], stderr: bytes = None
    ) -> None:
        """Handle the returned values from executing a system-level command.

        Args:
            returncode: The returncode from the command (typically, anything
                other than 0 is an error).
            cmd: The command that generated the return value.
            stderr: The standard error from the command.

        Raises:
            IOError if the command output represents an error.
        """
        if returncode != 0:
            # Wrap the CalledProcessError in an IOError, to be
            # compatible with errors that arise from using the
            # python library equivalents
            cpe = CalledProcessError(returncode, " ".join(cmd))
            cpe.stderr = stderr
            raise IOError from cpe

    @deprecated_str_to_path(1, "path")
    def open_file(
        self, path: PurePath, mode: ModeArg, use_system: bool = True, **kwargs
    ) -> FileLike:
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
            compressed_file: Optional[FileLikeInterface] = None
            if mode.readable and self.can_use_system_compression:
                compressed_file = SystemReader(
                    self.compress_path,
                    path,
                    self.get_command("d", src=path),
                    self.compress_name,
                )
            elif not mode.readable and self.can_use_system_decompression:
                bin_mode = FileMode(access=mode.access, coding=ModeCoding.BINARY)
                compressed_file = SystemWriter(
                    self.decompress_path,
                    path,
                    bin_mode,
                    self.get_command("c"),
                    self.decompress_name,
                )
            if compressed_file:
                if mode.text:
                    # noinspection PyTypeChecker
                    return io.TextIOWrapper(compressed_file)
                else:
                    return compressed_file

        return self.open_file_python(path, mode, **kwargs)

    def open_file_python(
        self, path_or_file: PathOrFile, mode: ModeArg, **kwargs
    ) -> FileLike:
        """Open a file using the python library.

        Args:
            path_or_file: The file to open -- a path or open file object.
            mode: The file open mode.
            kwargs: Additional arguments to pass to the open method.

        Returns:
            A file-like object.
        """
        if isinstance(mode, str):
            mode = FileMode(mode)
        return self.lib.open(path_or_file, mode.value, **kwargs)

    @deprecated_str_to_path(1, "source", 2, "dest")
    def compress_file(
        self,
        source: PathOrFile,
        dest: PathOrFile = None,
        keep: bool = True,
        compresslevel: int = None,
        use_system: bool = True,
        **kwargs,
    ) -> PurePath:
        """Compress data from one file and write to another.

        Args:
            source: Source file, either a path or an open file-like object.
            dest: Destination file, either a path or an open file-like object.
                If None, the file name is determined from ``source``.
            keep: Whether to keep the source file.
            compresslevel: Compression level.
            use_system: Whether to try to use system-level compression.
            kwargs: Additional arguments to pass to the open method when opening
                the destination file.

        Returns:
            Path to the destination file.

        Raises:
            IOError if there is an error compressing the file.
        """
        source_is_path = isinstance(source, PurePath)
        if source_is_path:
            source_path = source
            check_readable_file(cast(PurePath, source))
        else:
            source_io = cast(IO, source)
            source_path = Path(source_io.name)
            # pragma: no-cover
            try:
                source_io.fileno()
            except OSError:
                use_system = False

        if dest is None:
            dest = Path(f"{source_path}.{self.default_ext}")
            dest_is_path = True
        else:
            dest_is_path = isinstance(dest, PurePath)
        if dest_is_path:
            check_writable_file(cast(PurePath, dest))

        dest_file = None

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
                    dest_file = open(dest_name, "wb")
                else:
                    dest_file = cast(FileLike, dest)
                    dest_name = dest_file.name
                cmd = self.get_command("c", src=cmd_src, compresslevel=compresslevel)
                proc = PROCESS_PROGRESS.wrap(
                    cmd, stdin=prc_src, stdout=dest_file, stderr=PIPE
                )
                _, stderr = proc.communicate()
                self.handle_command_return(proc.returncode, cmd, stderr)
            else:
                if source_is_path:
                    source_file = open(source, "rb")
                else:
                    source_file = cast(FileLike, source)
                dest_name = str(dest)
                dest_file = self.open_file_python(dest, "wb", **kwargs)
                try:
                    # Perform sequential compression as the source
                    # file might be quite large
                    for chunk in iter_file_chunked(source_file):
                        dest_file.write(chunk)
                except EOFError as err:
                    raise IOError from err
                finally:
                    if source_is_path:
                        source_file.close()

            if not keep:
                if not source_is_path:
                    cast(FileLike, source).close()
                source_path.unlink()
        finally:
            if dest_is_path and dest_file is not None:
                dest_file.close()

        return Path(dest_name)

    @deprecated_str_to_path(1, "source", 2, "dest")
    def decompress_file(
        self,
        source: PathOrFile,
        dest: Optional[PathOrFile] = None,
        keep: bool = True,
        use_system: bool = True,
        **kwargs,
    ) -> PurePath:
        """Decompress data from one file and write to another.

        Args:
            source: Source file, either a path or an open file-like object.
            dest: Destination file, either a path or an open file-like object.
                If None, the file name is determined from ``source``.
            keep: Whether to keep the source file.
            use_system: Whether to try to use system-level compression.
            kwargs: Additional arguments to passs to the open method when
                opening the compressed file.

        Returns:
            Path to the destination file.

        Raises:
            IOError if there is an error decompressing the file.
        """
        source_is_path = isinstance(source, PurePath)
        if source_is_path:
            source_path = source
            check_readable_file(cast(PurePath, source))
        else:
            source_path = Path(cast(FileLike, source).name)
        source_parts = split_path(source_path)

        if dest is None:
            if len(source_parts) > 2:
                dest = (
                    Path(source_parts[0])
                    / source_parts[1]
                    / "".join(source_parts[2:-1])
                )
                dest_is_path = True
            else:
                raise IOError("Cannot determine path for decompressed file")
        else:
            dest_is_path = isinstance(dest, PurePath)
        if dest_is_path:
            dest_name = str(check_writable_file(cast(PurePath, dest)))
            dest_file = open(dest_name, "wb")
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
                cmd = self.get_command("d", src=src)
                psrc = None if source_is_path else cast(FileLike, source)
                proc = PROCESS_PROGRESS.wrap(
                    cmd, stdin=psrc, stdout=dest_file, stderr=PIPE
                )
                _, stderr = proc.communicate()
                self.handle_command_return(proc.returncode, cmd, stderr)
            else:
                source_file = self.open_file_python(source, "rb", **kwargs)
                try:
                    # Perform sequential decompression as the source
                    # file might be quite large
                    for chunk in iter_file_chunked(source_file):
                        dest_file.write(chunk)
                except EOFError as err:
                    raise IOError from err
                finally:
                    if source_is_path:
                        source_file.close()

            if not keep:
                if not source_is_path:
                    cast(FileLike, source).close()
                source_path.unlink()
        finally:
            if dest_is_path:
                dest_file.close()

        return Path(dest_name)

    def get_list_command(self, path: PurePath) -> Optional[List[str]]:
        """Get the command to list contents of a compressed file.

        Args:
            path: Path to the compressed file.

        Returns:
            List of command arguments, or None if the uncompressed size
            cannot be determined (without actually decompressing the file).
        """
        return None

    def parse_file_listing(self, listing: str) -> Tuple[int, int, float]:
        """Parse the result of the list command.

        Args:
            listing: The output of executing the list command.

        Returns:
            A tuple (<compressed size in bytes>, <uncompressed size in bytes>,
            <compression ratio>).
        """
        raise UnsupportedOperation()

    def uncompressed_size(self, path: PurePath) -> Optional[int]:
        """Get the uncompressed size of a compressed file.

        Args:
            path: Path to the compressed file.

        Returns:
            The uncompressed size of the file in bytes, or None if the
            uncompressed size cannot be determined (without actually
            decompressing the file).
        """
        list_command = self.get_list_command(path)
        if list_command is None:
            return None
        listing: str = check_output(list_command, universal_newlines=True)
        _, uncompressed, _ = self.parse_file_listing(listing)
        return uncompressed


class Formats:
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
        self, format_class: Callable[[], CompressionFormat]
    ) -> None:
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

    def list_extensions(self, with_sep: bool = False) -> Iterable[str]:
        """Returns an iterable with all valid extensions.

        Args:
            with_sep: Add separator prefix to each extension.
        """
        exts: Set[str] = set()
        for fmt in self.compression_formats.values():
            exts.update(fmt.exts)
        if with_sep:
            exts = set("{}{}".format(os.extsep, ext) for ext in exts)
        return exts

    def has_compatible_extension(self, dest_fmt, ext_fmt) -> bool:
        """Checks that `dest_fmt` is allowed to use a file extension supported by
        `ext_fmt`. This is mostly to handle the special case where `dest_fmt` and
        `ext_fmt` allow the same extension and the actual format cannot be detected
        from the file header.

        Returns:
            True if an allowed extension of `dest_fmt` is supported by `ext_fmt`
            else False.
        """
        allowed = set(self.get_compression_format(dest_fmt).allowed_exts)
        supported = set(self.get_compression_format(ext_fmt).exts)
        return bool(allowed & supported)

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

    def guess_compression_format(self, name: Union[str, PurePath]) -> Optional[str]:
        """Guess the compression format by name or file extension.

        Returns:
            The format name, or ``None`` if it could not be guessed.
        """
        if isinstance(name, PurePath):
            check_std(name, error=True)
            name = str(name)
        if name in self.compression_format_aliases:
            return self.compression_format_aliases[name]
        i = name.rfind(os.extsep)
        if i >= 0:
            ext = name[(i + 1):]
            if ext in self.compression_format_aliases:
                return self.compression_format_aliases[ext]
        return None

    @deprecated_str_to_path(1, "path")
    def guess_format_from_file_header(self, path: PurePath) -> Optional[str]:
        """Guess file format from 'magic bytes' at the beginning of the file.

        Note that ``path`` must be openable and readable. If it is a named pipe
        or other pseudo-file type, the magic bytes will be destructively
        consumed and thus will open correctly.

        Args:
            path: Path to the file

        Returns:
            The format name, or ``None`` if it could not be guessed.
        """
        check_std(path, error=True)
        with open(path, "rb") as infile:
            magic = infile.read(self.max_magic_bytes)
        return self.guess_format_from_header_bytes(magic)

    def guess_format_from_buffer(self, buffer: io.BufferedReader) -> Optional[str]:
        """Guess file format from a byte buffer that provides a ``peek``
        method.

        Args:
            buffer: The buffer object

        Returns:
            The format name, or ``None`` if it could not be guessed.
        """
        magic = buffer.peek(self.max_magic_bytes)
        return self.guess_format_from_header_bytes(magic)

    def guess_format_from_header_bytes(self, header_bytes: bytes) -> Optional[str]:
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
                    reverse=True,
                )
                for fmt, tail in candidates:
                    if (
                        num_bytes > len(tail)
                        and tuple(header_bytes[i] for i in range(1, len(tail) + 1))
                        == tail
                    ):
                        return fmt
        return None

    def get_format_for_mime_type(self, mime_type: str) -> str:
        """Returns the file format associated with a MIME type, or None if no
        format is associated with the mime type.
        """
        return self.mime_types.get(mime_type, None)


FORMATS = Formats()


def compression_format(cls):
    """Required decorator on *concrete* CompressionFormat subclasses. Registers
    the CompressionFormat in FORMATS.
    """
    if not issubclass(cls, CompressionFormat):
        raise ValueError(
            "compression_format decorator may only be applied to "
            "CompressionFormat subclasses"
        )
    FORMATS.register_compression_format(cls)
    return cls


class SingleExeCompressionFormat(
    CompressionFormat, metaclass=ABCMeta
):  # pylint: disable=abstract-method
    """Base class form ``CompressionFormat``s that use the same executable for
    compressing and decompressing.
    """

    def __init__(self):
        self._executable_path = None
        self._executable_name = None
        self._resolved = False

    @property
    def executable_path(self) -> PurePath:
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
    def compress_path(self) -> PurePath:
        return self.executable_path

    @property
    def decompress_path(self) -> PurePath:
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


class DualExeCompressionFormat(
    CompressionFormat, metaclass=ABCMeta
):  # pylint: disable=abstract-method
    """CompressionFormat that uses different executables for compressing and
    decompressing.
    """

    def __init__(self):
        self._compress_path = None
        self._compress_name = None
        self._compress_resolved = False
        self._compress_lib = None
        self._decompress_path = None
        self._decompress_name = None
        self._decompress_lib = None
        self._decompress_resolved = False

    @property
    def compress_path(self) -> str:
        self._resolve_compress()
        return self._compress_path

    @property
    def decompress_path(self) -> str:
        self._resolve_decompress()
        return self._decompress_path

    @property
    @abstractmethod
    def compress_commands(self) -> Tuple[str, ...]:
        pass

    @property
    @abstractmethod
    def decompress_commands(self) -> Tuple[str, ...]:
        pass

    @property
    def system_commands(self) -> Tuple[str, ...]:
        return self.compress_commands + self.decompress_commands

    @property
    def compress_name(self) -> str:
        self._resolve_compress()
        return self._compress_name

    @property
    def compress_lib(self):
        """Caches and returns the python module for compressing this file format.

        Returns:
            The module

        Raises:
            ImportError if the module cannot be imported.
        """
        if not self._compress_lib:
            self._compress_lib = import_module(self.compress_name)
        return self._compress_lib

    @property
    def decompress_name(self) -> str:
        self._resolve_decompress()
        return self._decompress_name

    @property
    def decompress_lib(self):
        """Caches and returns the python module for decompressing this file format.

        Returns:
            The module

        Raises:
            ImportError if the module cannot be imported.
        """
        if not self._decompress_lib:
            self._decompress_lib = import_module(self.decompress_name)
        return self._decompress_lib

    def get_command(
        self,
        operation: str,
        src: PurePath = STDIN,
        stdout: bool = True,
        compresslevel: Optional[int] = None,
    ) -> List[str]:
        if operation == "c":
            return self.get_compress_command(src, stdout, compresslevel)
        else:
            return self.get_decompress_command(src, stdout)

    @abstractmethod
    def get_compress_command(
        self, src: PurePath = STDIN, stdout: bool = True, compresslevel: int = None
    ) -> List[str]:
        """Build the compress command for the system executable.

        Args:
            src: The source file path, or STDIN if input should be read from
                stdin
            stdout: Whether output should go to stdout
            compresslevel: Integer compression level; typically 1-9

        Returns:
            List of command arguments
        """
        pass

    @abstractmethod
    def get_decompress_command(
        self, src: PurePath = STDIN, stdout: bool = True
    ) -> List[str]:
        """Build the decompress command for the system executable.

        Args:
            src: The source file path, or STDIN if input should be read from
                stdin
            stdout: Whether output should go to stdout

        Returns:
            List of command arguments
        """
        pass

    def _resolve_compress(self):
        if not self._compress_resolved:
            exe = EXECUTABLE_CACHE.resolve_exe(self.compress_commands)
            if exe:
                self._compress_path, self._compress_name = exe
            self._compress_resolved = True

    def _resolve_decompress(self):
        if not self._decompress_resolved:
            exe = EXECUTABLE_CACHE.resolve_exe(self.decompress_commands)
            if exe:
                self._decompress_path, self._decompress_name = exe
            self._decompress_resolved = True


# TODO: make a shared superclass for formats that clone the gzip CLI


@compression_format
class Gzip(SingleExeCompressionFormat):
    """Implementation of CompressionFormat for gzip files.
    """

    @property
    def name(self) -> str:
        return "gzip"

    @property
    def exts(self) -> Tuple[str, ...]:
        return "gz",

    @property
    def system_commands(self) -> Tuple[str, ...]:
        return "pigz", "gzip"

    @property
    def default_compresslevel(self) -> int:
        return 4

    @property
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        return (0x1F, 0x8B),

    @property
    def mime_types(self) -> Tuple[str, ...]:
        return (
            "application/gz",
            "application/gzip",
            "application/x-gz",
            "application/x-gzip",
        )

    @property
    def compresslevel_range(self) -> Tuple[int, int]:
        """The compression level; pigz allows 0-11 (har har) while
        gzip allows 0-9.
        """
        if self.executable_name == "pigz":
            return 0, 11
        else:
            return 1, 9

    def get_command(
        self,
        operation: str,
        src: PurePath = STDIN,
        stdout: bool = True,
        compresslevel: int = None,
    ) -> List[str]:
        cmd = [str(self.executable_path)]
        if operation == "c":
            compresslevel = self._get_compresslevel(compresslevel)
            cmd.append("-{}".format(compresslevel))
        elif operation == "d":
            cmd.append("-d")
        if stdout:
            cmd.append("-c")
        threads = THREADS.threads
        if self.executable_name == "pigz" and threads > 1:
            cmd.extend(("-p", str(threads)))
        if src != STDIN:
            cmd.append(str(src))
        return cmd

    def handle_command_return(
        self, returncode: int, cmd: List[str], stderr: bytes = None
    ) -> None:
        # pigz fails silently when the file is corrupt
        if returncode == 0 and "pigz" in cmd[0] and stderr and b"skipping" in stderr:
            returncode = 1
        super().handle_command_return(returncode, cmd, stderr)

    def get_list_command(self, path: PurePath) -> List[str]:
        return [str(self.executable_path), "-l", str(path)]

    def parse_file_listing(self, listing: str) -> Tuple[int, int, float]:
        parsed = re.split(" +", listing.splitlines(keepends=False)[1].strip())
        ratio = float(parsed[2][:-1]) / 100
        return int(parsed[0]), int(parsed[1]), ratio

    def open_file_python(
        self, path_or_file: PathOrFile, mode: ModeArg, **kwargs
    ) -> FileLike:
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


@compression_format
class BGzip(DualExeCompressionFormat):
    """bgzip is block gzip. bgzip files are compatible with gzip. Typically,
    this format is only used when specifically requested, or when a bgzip
    file specifically has a .bgz (rather than .gz) extension.

    The bgzip program is only used for compression; gzip is used for decompression
    because bgzip does not support decompressing a file with a non-.gz extension.
    """

    @property
    def name(self) -> str:
        return "bgzip"

    @property
    def module_name(self):
        return "gzip"

    @property
    def exts(self) -> Tuple[str, ...]:
        return "bgz",

    @property
    def allowed_exts(self) -> Tuple[str, ...]:
        return "bgz", "gz"

    @property
    def aliases(self) -> Tuple:
        return "bgzip", "bgz"

    @property
    def default_compresslevel(self) -> Optional[int]:
        return 4

    @property
    def compress_commands(self) -> Tuple[str, ...]:
        return "bgzip",

    @property
    def decompress_commands(self) -> Tuple[str, ...]:
        return "pigz", "gzip"

    @property
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        return (0x1F, 0x8B, 0x08, 0x04),

    @property
    def mime_types(self) -> Tuple[str, ...]:
        return (
            "application/bgz",
            "application/bgzip",
            "application/x-bgz",
            "application/x-bgzip",
        )

    @property
    def compresslevel_range(self) -> Tuple[int, int]:
        return 1, 9

    def get_compress_command(
        self, src: PurePath = STDIN, stdout: bool = True, compresslevel: int = None
    ) -> List[str]:
        cmd = [str(self.compress_path)]
        compress_level = self._get_compresslevel(compresslevel)
        if compress_level:
            cmd.extend(("-l", str(compress_level)))
        if stdout:
            cmd.append("-c")
        threads = THREADS.threads
        if threads > 1:
            cmd.extend(("-@", str(threads)))
        if src != STDIN:
            cmd.append(str(src))
        return cmd

    def get_decompress_command(
        self, src: PurePath = STDIN, stdout: bool = True
    ) -> List[str]:
        cmd = [str(self.decompress_path), "-d"]
        if stdout:
            cmd.append("-c")
        threads = THREADS.threads
        if self.decompress_name == "pigz" and threads > 1:
            cmd.extend(("-p", str(threads)))
        if src != STDIN:
            if src.suffix != ".gz":
                # bgzip does not support decompression of files without a .gz extension
                cmd.extend(("-S", src.suffix))
            cmd.append(str(src))
        return cmd

    def open_file_python(
        self, path_or_file: PathOrFile, mode: ModeArg, **kwargs
    ) -> FileLike:
        # pylint: disable=redefined-variable-type
        if isinstance(mode, str):
            mode = FileMode(mode)
        if mode.writable:
            raise NotImplementedError(
                "Writing to a bgzip file using a python library is not yet supported"
            )
        compressed_file = self.lib.open(path_or_file, mode.value, **kwargs)
        if mode.binary:
            compressed_file = io.BufferedWriter(compressed_file)
        return compressed_file


@compression_format
class Zstd(SingleExeCompressionFormat):
    """Implementation of CompressionFormat for zstd (.zst) files.

    Todo:
     * zstd can compress/decompress in other formats. Benchmark to
       see if it is faster than those specialized tools and, if so,
       default to using zstd for every format for which it is faster.
     * Investigate whether there is a difference between pzstd -p and
       zstd -T.
    """

    @property
    def name(self) -> str:
        return "zstd"

    @property
    def module_name(self):
        return "zstandard"

    @property
    def exts(self) -> Tuple[str, ...]:
        return "zst",

    @property
    def compresslevel_range(self) -> Tuple[int, int]:
        return 1, 22

    @property
    def default_compresslevel(self) -> int:
        return 3

    @property
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        return (
            (0xFD, 0x2F, 0xB5, 0x1E),  # v0.1
            (0xFD, 0x2F, 0xB5, 0x22),  # v0.2
            (0xFD, 0x2F, 0xB5, 0x23),  # v0.3
            (0xFD, 0x2F, 0xB5, 0x24),  # v0.4
        )

    @property
    def mime_types(self) -> Tuple[str, ...]:
        return (
            "application/zstd",
            "application/x-zstd"
        )

    def get_command(
        self,
        operation: str,
        src: PurePath = STDIN,
        stdout: bool = True,
        compresslevel: int = None,
    ) -> List[str]:
        cmd = [str(self.executable_path)]
        if operation == "c":
            compresslevel = self._get_compresslevel(compresslevel)
            if compresslevel > 19:
                cmd.append("--ultra")
            cmd.append("-{}".format(compresslevel))
        elif operation == "d":
            cmd.append("-d")
        if stdout:
            cmd.append("-c")
        threads = THREADS.threads
        if threads == 1:
            cmd.append(f"--single-thread")
        elif threads > 1:
            # zstd spawns an additional thread for I/O
            cmd.append(f"-T{threads-1}")
        if src != STDIN:
            cmd.append(str(src))
        return cmd

    def get_list_command(self, path: PurePath) -> List[str]:
        return [str(self.executable_path), "-l", str(path)]

    def parse_file_listing(self, listing: str) -> Tuple[int, int, float]:
        parsed = re.split(" +", listing.splitlines(keepends=False)[1].strip())
        ratio = float(parsed[2][:-1]) / 100
        return int(parsed[0]), int(parsed[1]), ratio

    def open_file_python(
        self, path_or_file: PathOrFile, mode: ModeArg, **kwargs
    ) -> FileLike:
        # pylint: disable=redefined-variable-type
        if isinstance(mode, str):
            mode = FileMode(mode)
        if mode.binary:
            raw_mode = mode
        else:
            raw_mode = FileMode(access=mode.access, coding=ModeCoding.BINARY)
        raw_file = open(path_or_file, raw_mode.value, **kwargs)
        if mode.readable:
            compressed_file = self.lib.ZstdDecompressor().stream_reader(raw_file)
        else:
            compressed_file = self.lib.ZstdCompressor().stream_writer(raw_file)
        if not mode.binary:
            compressed_file = io.TextIOWrapper(compressed_file)
        elif mode.readable:
            compressed_file = io.BufferedReader(compressed_file)
        else:
            compressed_file = io.BufferedWriter(compressed_file)
        return compressed_file


@compression_format
class BZip2(SingleExeCompressionFormat):
    """Implementation of CompressionFormat for bzip2 files.
    """

    @property
    def name(self) -> str:
        return "bz2"

    @property
    def exts(self) -> Tuple[str, ...]:
        return "bz2", "bzip", "bzip2"

    @property
    def system_commands(self) -> Tuple[str, ...]:
        return "pbzip2", "bzip2"

    @property
    def compresslevel_range(self) -> Tuple[int, int]:
        return 1, 9

    @property
    def default_compresslevel(self) -> int:
        return 9

    @property
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        return (0x42, 0x5A, 0x68),

    @property
    def mime_types(self) -> Tuple[str, ...]:
        return (
            "application/bz2",
            "application/bzip2",
            "application/x-bz2",
            "application/x-bzip2",
        )

    def get_command(
        self,
        operation: str,
        src: PurePath = STDIN,
        stdout: bool = True,
        compresslevel: Optional[int] = 6,
    ) -> List[str]:
        cmd = [str(self.executable_path)]
        if operation == "c":
            compresslevel = self._get_compresslevel(compresslevel)
            cmd.append("-{}".format(compresslevel))
            cmd.append("-z")
        elif operation == "d":
            cmd.append("-d")
        if stdout:
            cmd.append("-c")
        threads = THREADS.threads
        if self.executable_name == "pbzip2" and threads > 1:
            cmd.append("-p{}".format(threads))
        if src != STDIN:
            cmd.append(str(src))
        return cmd

    # noinspection PyTypeChecker
    def open_file_python(
        self, path_or_file: PathOrFile, mode: ModeArg, **kwargs
    ) -> FileLike:
        if isinstance(mode, str):
            mode = FileMode(mode)
        if mode.text:
            return io.TextIOWrapper(
                self.lib.BZ2File(path_or_file, mode.access.value, **kwargs)
            )
        else:
            return self.lib.BZ2File(path_or_file, mode.value, **kwargs)


@compression_format
class Lzma(SingleExeCompressionFormat):
    """Implementation of CompressionFormat for lzma (.xz) files.
    """

    @property
    def name(self) -> str:
        return "lzma"

    @property
    def exts(self) -> Tuple[str, ...]:
        return "xz", "lzma", "7z", "7zip"

    @property
    def system_commands(self) -> Tuple[str, ...]:
        return "xz", "lzma"

    @property
    def compresslevel_range(self) -> Tuple[int, int]:
        return 0, 9

    @property
    def default_compresslevel(self) -> int:
        return 2

    @property
    def magic_bytes(self) -> Tuple[Tuple[int, ...], ...]:
        return (
            (0x4C, 0x5A, 0x49, 0x50),  # lz
            (0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00),  # xz
            (0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C),  # 7z
        )

    @property
    def mime_types(self) -> Tuple[str, ...]:
        return (
            "application/lzma",
            "application/x-lzma",
            "application/xz",
            "application/x-xz",
            "application/7z-compressed" "application/x-7z-compressed",
        )

    def get_command(
        self,
        operation: str,
        src: PurePath = STDIN,
        stdout: bool = True,
        compresslevel: Optional[int] = 6,
    ) -> List[str]:
        cmd = [str(self.executable_path)]
        if operation == "c":
            compresslevel = self._get_compresslevel(compresslevel)
            cmd.append("-{}".format(compresslevel))
            cmd.append("-z")
        elif operation == "d":
            cmd.append("-d")
        if stdout:
            cmd.append("-c")
        threads = THREADS.threads
        if threads > 1:
            cmd.extend(("-T", str(threads)))
        if src != STDIN:
            cmd.append(str(src))
        return cmd

    def get_list_command(self, path: PurePath) -> List[str]:
        return [str(self.executable_path), "-lv", str(path)]

    def parse_file_listing(self, listing: str) -> Tuple[int, int, float]:
        parsed = listing.splitlines(keepends=False)
        compressed, uncompressed = (
            int(re.sub(r"[.,]", "", re.match(r".+?([\d.,]+) B\)?", size).group(1)))
            for size in parsed[3:5]
        )
        ratio = float(parsed[5][22:])
        return compressed, uncompressed, ratio

    def compress(self, raw_bytes: bytes, **kwargs) -> bytes:
        kwargs = dict(
            (k, kwargs[k])
            for k, v in kwargs.items()
            if k in {"format", "check", "preset", "filter"}
        )
        return self.lib.compress(raw_bytes, **kwargs)


# @compression_format
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
