# -*- coding: utf-8 -*-
"""Common interface to enable operations to be wrapped in a progress bar.
By default, tqdm is used for python-level operations and pv for system-level
operations.
"""
import shlex
from subprocess import Popen, PIPE
from xphyle.paths import EXECUTABLE_CACHE, check_path
from xphyle.types import (
    Iterable, Union, Callable, Tuple, Sequence, FileLike, PathLike, PathType,
    Permission)

# Python-level progress wrapper

class Tqdm(object):
    """Default python progress bar wrapper.
    """
    def __init__(self):
        import tqdm
        self.wrapper_fn = tqdm.tqdm
    
    def __call__(self, itr: Iterable, desc: str, size: int) -> Iterable:
        return self.wrapper_fn(itr, desc=desc, total=size)

class IterableProgress(object):
    """Manages the python-level wrapper.
    
    Args:
        default_wrapper: Callable (typically a class) that returns a Callable
            with the signature of ``wrap``.
    """
    def __init__(self, default_wrapper: Callable = Tqdm) -> None:
        self.enabled = False
        self.wrapper = None # type: Callable[..., Iterable]
        self.default_wrapper = default_wrapper
    
    def update(
            self, enable: bool = None,
            wrapper: Callable[..., Iterable] = None) -> None:
        """Enable the python progress bar and/or set a new wrapper.
        
        Args:
            enable: Whether to enable use of a progress wrapper.
            wrapper: A callable that takes three arguments, itr, desc, size,
                and returns an iterable.
        """
        if enable is not None:
            self.enabled = enable
        
        if wrapper:
            self.wrapper = wrapper
        elif self.enabled and not self.wrapper:
            try:
                self.wrapper = self.default_wrapper()
            except ImportError as err:
                raise ValueError(
                    "Could not create default python wrapper; valid wrapper "
                    "must be specified") from err
    
    def wrap(
            self, itr: Iterable, desc: str = None,
            size: int = None) -> Iterable:
        """Wrap an iterable in a progress bar.
        
        Args:
            itr: The Iterable to wrap.
            desc: Optional description.
            size: Optional max value of the progress bar.
        
        Returns:
            The wrapped Iterable.
        """
        if self.enabled:
            return self.wrapper(itr, desc=desc, size=size)
        else:
            return itr

ITERABLE_PROGRESS = IterableProgress()

# System-level progress wrapper

def system_progress_command(
        exe: PathLike, *args, require: bool = False) -> Tuple: # pragma: no-cover
    """Resolve a system-level progress bar command.
    
    Args:
        exe: The executable name or absolute path.
        args: A list of additional command line arguments.
        require: Whether to raise an exception if the command does not exist.
    
    Returns:
        A tuple of (executable_path, *args).
    """
    executable_path = EXECUTABLE_CACHE.get_path(str(exe))
    if executable_path is not None:
        check_path(executable_path, PathType.FILE, Permission.EXECUTE)
    elif require:
        raise IOError("pv is not available on the path")
    return (executable_path,) + tuple(args)

def pv_command(require: bool = False) -> Tuple: # pragma: no-cover
    """Default system wrapper command.
    """
    return system_progress_command('pv', '-pre', require=require)

class ProcessProgress(object):
    """Manage the system-level progress wrapper.
    
    Args:
        default_wrapper: Callable that returns the argument list for the
            default wrapper command.
    """
    def __init__(self, default_wrapper: Callable = pv_command) -> None:
        self.enabled = False
        self.wrapper = None # type: Sequence[str]
        self.default_wrapper = default_wrapper
    
    def update(
            self, enable: bool = None,
            wrapper: Union[str, Sequence[str]] = None) -> None:
        """Enable the python system progress bar and/or set the wrapper
        command.
        
        Args:
            enable: Whether to enable use of a progress wrapper.
            wrapper: A command string or sequence of command arguments.
        """
        if enable is not None:
            self.enabled = enable
        
        if wrapper:
            if isinstance(wrapper, str):
                self.wrapper = tuple(shlex.split(wrapper))
            else:
                self.wrapper = wrapper
        elif self.enabled and not self.wrapper:
            try:
                self.wrapper = self.default_wrapper()
            except IOError as err:
                raise ValueError(
                    "Could not create default system wrapper; valid wrapper "
                    "must be specified") from err
    
    def wrap(
            self, cmd: Sequence[str], stdin: FileLike, stdout: FileLike,
            **kwargs) -> Popen: # pragma: no-cover
        """Pipe a system command through a progress bar program.
        
        For the process to be wrapped, one of ``stdin``, ``stdout`` must not be
        None.
        
        Args:
            cmd: Command arguments.
            stdin: File-like object to read into the process stdin, or None to
                use `PIPE`.
            stdout: File-like object to write from the process stdout, or None
                to use `PIPE`.
            kwargs: Additional arguments to pass to Popen.
        
        Returns:
            Open process.
        """
        if not self.enabled or (stdin is None and stdout is None):
            return Popen(cmd, stdin=stdin, stdout=stdout, **kwargs)
        
        if stdin is not None:
            proc1 = Popen(self.wrapper, stdin=stdin, stdout=PIPE)
            proc2 = Popen(cmd, stdin=proc1.stdout, stdout=stdout)
        else:
            proc1 = Popen(cmd, stdout=PIPE)
            proc2 = Popen(self.wrapper, stdin=proc1.stdout, stdout=stdout)
        proc1.stdout.close()
        return proc2

PROCESS_PROGRESS = ProcessProgress()

# Misc functions

def iter_file_chunked(fileobj: FileLike, chunksize: int = 1024) -> Iterable:
    """Returns a progress bar-wrapped iterator over a file that reads
    fixed-size chunks.
    
    Args:
        fileobj: A file-like object.
        chunksize: The maximum size in bytes of each chunk.
    
    Returns:
        An iterable over the chunks of the file.
    """
    def _itr():
        while True:
            data = fileobj.read(chunksize)
            if data:
                yield data
            else:
                break
    
    name = None
    if hasattr(fileobj, 'name'):
        name = getattr(fileobj, 'name')
        
    return ITERABLE_PROGRESS.wrap(_itr(), desc=name)
