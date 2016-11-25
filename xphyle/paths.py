# -*- coding: utf-8 -*-
"""Convenience functions for working with file paths.
"""
import errno
import os
import re
import shutil
import stat
import tempfile
from xphyle.types import Sequence, Tuple, Union, Pattern, Iterable

ACCESS = dict(
    r=(os.R_OK, stat.S_IREAD),
    w=(os.W_OK, stat.S_IWRITE),
    a=(os.W_OK, stat.S_IWRITE),
    x=(os.X_OK, stat.S_IEXEC))
"""Dictionary mapping mode characters to access method constants"""
STDIN = STDOUT = '-'
"""Placeholder for ``sys.stdin`` or ``sys.stdout`` (depending on access mode)"""
STDERR = '_'
"""Placeholder for ``sys.stderr``"""

def get_access(mode: str) -> int:
    """Returns the access mode constant associated with given mode string.
    
    Args:
        mode: A mode string
    
    Returns:
        The access mode constant
    
    Examples:
        a = get_access('rb') # -> os.R_OK
    """
    for access, ints in ACCESS.items():
        if access in mode:
            return ints[0]
    raise ValueError("{} does not contain a valid access mode".format(mode))

def set_access(path: str, mode: str) -> int:
    """Sets file access from a mode string.
    
    Args:
        path: The file to chmod
        mode: Mode string consisting of one or more of 'r', 'w', 'x'
    
    Returns:
        The integer equivalent of the specified mode string
    """
    mode_flag = 0
    for char in mode:
        if char not in ACCESS:
            raise ValueError("Invalid mode character {}".format(char))
        mode_flag |= ACCESS[char][1]
    os.chmod(path, mode_flag)
    return mode_flag

def check_access(path: str, access: Union[int, str]) -> None:
    """Check that ``path`` is accessible.
    
    Args:
        path: The path to check
        access: String or int access specifier
    
    Raises:
        IOError if the path cannot be accessed according to ``access``
    """
    if isinstance(access, str):
        access = get_access(access)
    if path in (STDOUT, STDERR):
        if path == STDOUT and access not in (os.R_OK, os.W_OK):
            raise IOError(errno.EACCES, "STDOUT access must be r or w", path)
        elif path == STDERR and access != os.W_OK:
            raise IOError(errno.EACCES, "STDERR access must be w", path)
    elif not os.access(path, access):
        raise IOError(errno.EACCES, "{} not accessable".format(path), path)

def abspath(path: str) -> str:
    """Returns the fully resolved path associated with ``path``.
    
    Args:
        path: Relative or absolute path
    
    Returns:
        Fully resolved path
    
    Examples:
        abspath('foo') # -> /path/to/curdir/foo
        abspath('~/foo') # -> /home/curuser/foo
    """
    if path in (STDOUT, STDERR):
        return path
    return os.path.abspath(os.path.expanduser(path))

def split_path(path: str, keep_seps: bool = True, resolve: bool = True
              ) -> Tuple:
    """Splits a path into a (parent_dir, name, *ext) tuple.
    
    Args:
        path: The path
        keep_seps: Whether the extension separators should be kept as part
            of the file extensions
        resolve: Whether to resolve the path before splitting
    
    Returns:
        A tuple of length >= 2, in which the first element is the parent
        directory, the second element is the file name, and the remaining
        elements are file extensions.
    
    Examples:
        split_path('myfile.foo.txt', False)
        # -> ('/current/dir', 'myfile', 'foo', 'txt')
        split_path('/usr/local/foobar.gz', True)
        # -> ('/usr/local', 'foobar', '.gz')
    """
    if resolve:
        path = abspath(path)
    parent = os.path.dirname(path)
    file_parts = tuple(os.path.basename(path).split(os.extsep))
    if len(file_parts) == 1:
        seps = ()
    else:
        seps = file_parts[1:]
        if keep_seps:
            seps = tuple(
                '{}{}'.format(os.extsep, ext)
                for ext in file_parts[1:])
    return (parent, file_parts[0]) + seps

def filename(path: str) -> str:
    """Equivalent to ``split_path(path)[1]``.
    
    Args:
        The path
    
    Returns:
        The filename part of ``path``
    """
    return split_path(path)[1]

def resolve_path(path: str, parent: str = None) -> str:
    """Resolves the absolute path of the specified file and ensures that the
    file/directory exists.
    
    Args:
        path: Path to resolve
        parent: The directory containing ``path`` if ``path`` is relative
    
    Returns:
        The absolute path
    
    Raises:
        IOError: if the path does not exist or is invalid
    """
    if path in (STDOUT, STDERR):
        return path
    if parent:
        path = os.path.join(abspath(parent), path)
    else:
        path = abspath(path)
    if not os.path.exists(path):
        raise IOError(errno.ENOENT, "{} does not exist".format(path), path)
    return path

def check_path(path: str, ptype: str = None, access: Union[int, str] = None
              ) -> str:
    """Resolves the path (using ``resolve_path``) and checks that the path is
    of the specified type and allows the specified access.
    
    Args:
        path: The path to check
        ptype: 'f' for file or 'd' for directory.
        access: One of the access values from :module:`os`
    
    Returns:
        The fully resolved path
    
    Raises:
        IOError if the path does not exist, is not of the specified type,
        or doesn't allow the specified access.
    """
    path = resolve_path(path)
    if ptype is not None:
        if ptype == 'f' and not (
                path in (STDOUT, STDERR) or os.path.isfile(path)):
            raise IOError(errno.EISDIR, "{} not a file".format(path), path)
        elif ptype == 'd' and not os.path.isdir(path):
            raise IOError(errno.ENOTDIR, "{} not a directory".format(path),
                          path)
    if access is not None:
        check_access(path, access)
    return path

def check_readable_file(path: str) -> str:
    """Check that ``path`` exists and is readable.
    
    Args:
        path: The path to check
    
    Returns:
        The fully resolved path of ``path``
    """
    return check_path(path, 'f', 'r')

def check_writeable_file(path: str, mkdirs: bool = True) -> str:
    """If ``path`` exists, check that it is writeable, otherwise check that
    its parent directory exists and is writeable.
    
    Args:
        path: The path to check
        mkdirs: Whether to create any missing directories (True)
    
    Returns:
        The fully resolved path
    """
    if os.path.exists(path):
        return check_path(path, 'f', 'w')
    else:
        path = abspath(path)
        dirpath = os.path.dirname(path)
        if os.path.exists(dirpath):
            check_path(dirpath, 'd', 'w')
        elif mkdirs:
            os.makedirs(dirpath)
        return path

# "Safe" versions of the check methods, meaning they return None
# instead of throwing exceptions

def safe_check_path(path: str, *args, **kwargs) -> str:
    """Safe vesion of ``check_path``. Returns None rather than throw an
    exception.
    """
    try:
        return check_path(path, *args, **kwargs)
    except IOError:
        return None

def safe_check_readable_file(path: str) -> str:
    """Safe vesion of ``check_readable_file``. Returns None rather than throw an
    exception.
    """
    try:
        return check_readable_file(path)
    except IOError:
        return None

def safe_check_writeable_file(path: str) -> str:
    """Safe vesion of ``check_writeable_file``. Returns None rather than throw
    an exception.
    """
    try:
        return check_writeable_file(path)
    except IOError:
        return None

def find(root: str, pattern: Union[str, Pattern], types: str = 'f',
         recursive: bool = True) -> Sequence[str]:
    """Find all paths under ``root`` that match ``pattern``.
    
    Args:
        root: Directory at which to start search
        pattern: File name pattern to match (string or re object)
        types: Types to return -- files ("f"), directories ("d") or both ("fd")
        recursive: Whether to search directories recursively
    
    Returns:
        List of matching paths
    """
    if isinstance(pattern, str):
        pattern = re.compile(pattern)
    found = []
    for parent, dirs, files in os.walk(root):
        if types != "f":
            found.extend(
                os.path.join(parent, d)
                for d in dirs
                if pattern.match(d))
        if types != "d":
            found.extend(
                os.path.join(parent, f)
                for f in files
                if pattern.match(f))
        if not recursive:
            break
    
    return found

class ExecutableCache(object):
    """Lookup and cache executable paths.
    
    Args:
        default_path: The default executable path
    """
    def __init__(self, default_path: Sequence[str] = os.get_exec_path()):
        self.cache = {}
        self.search_path = None
        self.reset_search_path(default_path)

    def add_search_path(self, paths: Union[str, Sequence[str]]) -> None:
        """Add directories to the beginning of the executable search path.
        
        Args:
            paths: List of paths, or a string with directories separated by
                ``os.path.sep``
        """
        # pylint: disable=global-statement,invalid-name
        if isinstance(paths, str):
            paths = paths.split(os.path.sep)
        self.search_path = list(paths) + self.search_path
    
    def reset_search_path(self,
                          default_path: Sequence[str] = os.get_exec_path()
                         ) -> None:
        """Reset the search path to ``default_path``.
        
        Args:
            default_path: The default executable path
        """
        self.search_path = []
        if default_path:
            self.add_search_path(default_path)
    
    def get_path(self, executable: str) -> str:
        """Get the full path of ``executable``.
        
        Args:
            executable: A executable name
        
        Returns:
            The full path of ``executable``, or None if the path cannot be
            found.
        """
        exe_name = os.path.basename(executable)
        if exe_name in self.cache:
            return self.cache[exe_name]
        
        exe_file = safe_check_path(executable, 'f', 'x')
        if not exe_file:
            for path in self.search_path:
                exe_file = safe_check_path(
                    os.path.join(path.strip('"'), executable),
                    'f', 'x')
                if exe_file:
                    break
        
        self.cache[exe_name] = exe_file
        return exe_file
    
    def resolve_exe(self, names: Iterable[str]) -> Tuple:
        """Given an iterable of command names, find the first that resolves to
        an executable.
        
        Args:
            names: An iterable of command names
        
        Returns:
            A tuple (path, name) of the first command to resolve, or None if
            none of the commands resolve
        """
        for cmd in names:
            exe = self.get_path(cmd)
            if exe:
                return (exe, cmd)
        return None

EXECUTABLE_CACHE = ExecutableCache()
"""Singleton instance of ExecutableCache."""

# Temporary files and directories

class TempPath(object):
    """Base class for temporary files/directories.
    
    Args:
        parent: The parent directory
        mode: The access mode
        path_type: 'f' = file, 'd' = directory
    """
    def __init__(self, parent: str = None, mode: str = 'rwx',
                 path_type: str = 'd'):
        self.parent = parent
        self.path_type = path_type
        self._mode = mode
    
    @property
    def exists(self) -> bool:
        """Whether the directory exists.
        """
        # pylint: disable=no-member
        return os.path.exists(self.absolute_path)
    
    @property
    def mode(self) -> str:
        """The access mode of the path. Defaults to the parent's mode.
        """
        if not self._mode:
            if self.parent:
                self._mode = self.parent.mode
            else:
                raise Exception("Cannot determine mode without 'parent'")
        return self._mode
    
    def set_access(self, mode: str = None, set_parent: bool = False,
                   additive: bool = False) -> str:
        """Set the access mode for the path.
        
        Args:
            mode: The new mode to set. If node, the existing mode is used
            set_parent: Whether to recursively set the mode of all parents.
                This is done additively.
            additive: Whether permissions should be additive (e.g.
                if ``mode == 'w'`` and ``self.mode == 'r'``, the new mode
                is 'rw')
        
        Returns:
            The mode that was set
        """
        # pylint: disable=no-member
        if not self.exists:
            return None
        if mode:
            if additive:
                self._mode = ''.join(set(self.mode) | set(mode))
            else:
                self._mode = mode
        else:
            mode = self.mode
        if set_parent and self.parent:
            self.parent.set_access(mode, True, True)
        # Always set 'x' on directories, otherwise they are not listable
        if self.path_type in ('d', 'dir') and 'x' not in mode:
            mode += 'x'
        set_access(self.absolute_path, mode)
        return mode

class TempPathDescriptor(TempPath):
    """Describes a temporary file or directory within a TempDir.
    
    Args:
        name: The file/direcotry name
        parent: The parent directory, a TempPathDescriptor
        mode: The access mode
        suffix, prefix: The suffix and prefix to use when calling
            ``mkstemp`` or ``mkdtemp``
        path_type: 'f' or 'file' (for file), 'd' or 'dir' (for directory),
            or 'fifo' (for FIFO)
    """
    def __init__(self, name: str = None, parent: TempPath = None,
                 mode: str = None, suffix: str = '', prefix: str = '',
                 contents: str = '', path_type: str = 'f'):
        if contents and path_type != 'f':
            raise ValueError("'contents' only valid for files")
        super(TempPathDescriptor, self).__init__(parent, mode, path_type)
        self.name = name
        self.prefix = prefix
        self.suffix = suffix
        self.contents = contents
        self._mode = mode
        self._abspath = None
        self._relpath = None
    
    @property
    def absolute_path(self) -> str:
        """The absolute path.
        """
        if self._abspath is None:
            self._init_path()
        return self._abspath
    
    @property
    def relative_path(self) -> str:
        """The relative path.
        """
        if self._relpath is None:
            self._init_path()
        return self._relpath
    
    def _init_path(self) -> None:
        if self.parent is None:
            raise Exception("Cannot determine absolute path without 'root'")
        self._relpath = os.path.join(self.parent.relative_path, self.name)
        self._abspath = os.path.join(self.parent.absolute_path, self.name)
    
    def create(self, apply_permissions: bool = True) -> None:
        """Create the file/directory.
        
        Args:
            apply_permissions: Whether to set access permissions according to
                ``self.mode
        """
        if self.path_type not in ('d', 'dir'):
            if self.path_type == 'fifo':
                if os.path.exists(self.absolute_path):
                    os.remove(self.absolute_path)
                os.mkfifo(self.absolute_path)
            # TODO: Opening a FIFO for write blocks. It's possible to get around
            # this using a subprocess to pipe through a buffering program (such
            # as pv) to the FIFO instead
            if self.path_type != 'fifo':
                with open(self.absolute_path, 'wt') as outfile:
                    outfile.write(self.contents or '')
            elif self.contents:
                raise Exception("Currently, contents cannot be written to a FIFO")
        elif not os.path.exists(self.absolute_path):
            os.mkdir(self.absolute_path)
        if apply_permissions:
            self.set_access()

class TempDir(TempPath):
    """Context manager that creates a temporary directory and cleans it up
    upon exit.
    
    Args:
        mode: Access mode to set on temp directory. All subdirectories and
            files will inherit this mode unless explicity set to be different.
        path_descriptors: Iterable of TempPathDescriptors.
        kwargs: Additional arguments passed to tempfile.mkdtemp
    
    By default all subdirectories and files inherit the mode of the temporary
    directory. If TempPathDescriptors are specified, the paths are created
    before permissions are set, enabling creation of a read-only temporary file
    system.
    """
    def __init__(self, mode: str = 'rwx',
                 path_descriptors: Iterable[TempPathDescriptor] = None,
                 **kwargs):
        super(TempDir, self).__init__(mode=mode)
        self.absolute_path = abspath(tempfile.mkdtemp(**kwargs))
        self.relative_path = ''
        self.paths = {}
        if path_descriptors:
            self.make_paths(*path_descriptors)
        self.set_access()
        
    def __enter__(self):
        return self
    
    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
    
    def __getitem__(self, path: str) -> TempPathDescriptor:
        return self.paths[path]
    
    def __contains__(self, path: str) -> bool:
        return path in self.paths
    
    def close(self) -> None:
        """Delete the temporary directory and all files/subdirectories within.
        """
        # First need to make all paths removable
        if not self.exists:
            return
        for path in self.paths.values():
            path.set_access('rwx', True)
        shutil.rmtree(self.absolute_path)
    
    def make_path(self, desc: str = None, apply_permissions: bool = True,
                  **kwargs) -> str:
        """Create a file or directory within the TempDir.
        
        Args:
            desc: A TempPathDescriptor
            apply_permissions: Whether access permissions should be applied to
                the new file/directory
            kwargs: Arguments to TempPathDescriptor. Ignored unless ``desc``
                is None
        
        Returns:
            The absolute path to the new file/directory
        """
        if not desc:
            desc = TempPathDescriptor(**kwargs)
        
        # If the subdirectory is given as a path, resolve it
        if not desc.parent:
            desc.parent = self
        elif isinstance(desc.parent, str):
            desc.parent = self[desc.parent]
        
        # Determine the name of the new file/directory
        if not desc.name:
            parent = desc.parent.absolute_path
            if desc.path_type in ('d', 'dir'):
                path = tempfile.mkdtemp(
                    prefix=desc.prefix, suffix=desc.suffix, dir=parent)
                desc.name = os.path.basename(path)
            else:
                path = tempfile.mkstemp(
                    prefix=desc.prefix, suffix=desc.suffix, dir=parent)[1]
                desc.name = os.path.basename(path)
        
        desc.create(apply_permissions)
        
        self.paths[desc.absolute_path] = desc
        self.paths[desc.relative_path] = desc
        
        return desc.absolute_path
    
    def make_paths(self, *path_descriptors: Sequence[TempPathDescriptor]
                  ) -> Sequence[str]:
        """Create multiple files/directories at once. The paths are created
        before permissions are set, enabling creation of a read-only temporary
        file system.
        
        Args:
            path_descriptors: One or more TempPathDescriptor
        
        Returns:
            A list of the created paths
        """
        # Create files/directories without permissions
        paths = [
            self.make_path(desc, apply_permissions=False)
            for desc in path_descriptors]
        # Now apply permissions after all paths are created
        for desc in path_descriptors:
            desc.set_access()
        return paths
    
    # Convenience methods
    
    def make_file(self, desc: str = None, apply_permissions: bool = True,
                  **kwargs) -> Sequence[str]:
        """Convenience method; calls ``make_path`` with path_type='f'.
        """
        kwargs['path_type'] = 'f'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_fifo(self, desc: str = None, apply_permissions: bool = True,
                  **kwargs) -> Sequence[str]:
        """Convenience method; calls ``make_path`` with path_type='fifo'.
        """
        kwargs['path_type'] = 'fifo'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_directory(self, desc: str = None, apply_permissions: bool = True,
                       **kwargs) -> Sequence[str]:
        """Convenience method; calls ``make_path`` with path_type='d'.
        """
        kwargs['path_type'] = 'd'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_empty_files(self, num_files: int, **kwargs) -> Sequence[str]:
        """Create randomly-named empty files.
        
        Args:
            n: The number of files to create
            kwargs: Arguments to pass to TempPathDescriptor.
        """
        return list(
            self.make_path(TempPathDescriptor(**kwargs))
            for i in range(num_files))
