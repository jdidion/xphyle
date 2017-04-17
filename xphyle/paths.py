# -*- coding: utf-8 -*-
"""Convenience functions for working with file paths.
"""
from abc import ABCMeta, abstractmethod
import errno
import os
import pathlib
import re
import shutil
import stat
import sys
import tempfile
from xphyle.types import (
    ModeAccess, Permission, PermissionSet, PermissionArg, PermissionSetArg, 
    PathType, PathTypeArg, PathLike, PathLikeClass, Sequence, List, Tuple, 
    Callable, Union, Iterable, Dict, Regexp, Pattern, Match, Any, cast)

STDIN = STDOUT = '-'
"""Placeholder for `sys.stdin` or `sys.stdout` (depending on access mode)"""
STDERR = '_'
"""Placeholder for `sys.stderr`"""

def get_permissions(path: PathLike) -> PermissionSet:
    """Get the permissions of a file/directory.
    
    Args:
        path: Path of file/directory.
    
    Returns:
        An PermissionSet.
    
    Raises:
        IOError if the file/directory doesn't exist.
    """
    return PermissionSet(os.stat(str(path)).st_mode)

def set_permissions(
        path: PathLike, permissions: PermissionSetArg) -> PermissionSet:
    """Sets file stat flags (using chmod).
    
    Args:
        path: The file to chmod.
        permissions: Stat flags (any of 'r', 'w', 'x', or an
            :class:`PermissionSet`).
    
    Returns:
        An :class:`PermissionSet`.
    """
    if not isinstance(permissions, PermissionSet):
        permissions = PermissionSet(permissions)
    os.chmod(str(path), permissions.stat_flags)
    return permissions

def check_access(
        path: PathLike, 
        permissions: Union[PermissionArg, PermissionSetArg]) -> PermissionSet:
    """Check that `path` is accessible with the given set of permissions.
    
    Args:
        path: The path to check.
        permissions: Access specifier (string/int/:class:`ModeAccess`).
    
    Raises:
        IOError if the path cannot be accessed according to `permissions`.
    """
    if isinstance(permissions, PermissionSet):
        permission_set = cast(PermissionSet, permissions)
    else:
        permission_set = PermissionSet(
            cast(Union[PermissionArg, Sequence[PermissionArg]], permissions))
    if path in (STDOUT, STDERR):
        if path == STDOUT and not any(flag in permission_set for flag in (
                Permission.READ, Permission.WRITE)):
            raise IOError(
                errno.EACCES, "STDOUT permissions must be r or w", path)
        elif path == STDERR and Permission.WRITE not in permission_set:
            raise IOError(errno.EACCES, "STDERR permissions must be w", path)
    elif not os.access(str(path), permission_set.os_flags):
        raise IOError(errno.EACCES, "{} not accessable".format(path), path)
    return permission_set

def abspath(path: PathLike) -> PathLike:
    """Returns the fully resolved path associated with `path`.
    
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
    return os.path.abspath(os.path.expanduser(str(path)))

def get_root(path: PathLike = None) -> PathLike:
    """Get the root directory.
    
    Args:
        str: A path, or '.' to get the root of the working directory, or None
            to get the root of the path to the script.
    
    Returns:
        A path to the root directory.
    """
    path = str(path) if path else sys.executable
    root = os.path.splitdrive(str(abspath(path)))[0]
    if root == '':
        root = os.sep
    return root

def split_path(
        path: PathLike, keep_seps: bool = True,
        resolve: bool = True) -> Tuple[str, ...]:
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
    parent = os.path.dirname(str(path))
    file_parts = tuple(os.path.basename(str(path)).split(os.extsep))
    if len(file_parts) == 1:
        seps = () # type: Tuple[str, ...]
    else:
        seps = file_parts[1:]
        if keep_seps:
            seps = tuple(
                '{}{}'.format(os.extsep, ext)
                for ext in file_parts[1:])
    return (parent, file_parts[0]) + seps

def filename(path: PathLike) -> str:
    """Equivalent to `split_path(path)[1]`.
    
    Args:
        The path
    
    Returns:
        The filename part of `path`
    """
    return split_path(path)[1]

def resolve_path(path: PathLike, parent: PathLike = None) -> PathLike:
    """Resolves the absolute path of the specified file and ensures that the
    file/directory exists.
    
    Args:
        path: Path to resolve.
        parent: The directory containing `path` if `path` is relative.
    
    Returns:
        The absolute path.
    
    Raises:
        IOError: if the path does not exist or is invalid.
    """
    if path in (STDOUT, STDERR):
        return path
    if parent:
        path = os.path.join(str(abspath(parent)), str(path))
    else:
        path = abspath(path)
    if not os.path.exists(str(path)):
        raise IOError(errno.ENOENT, "{} does not exist".format(path), path)
    return path

def check_path(
        path: PathLike, path_type: PathTypeArg = None,
        permissions: Union[PermissionArg, PermissionSetArg] = None
        ) -> PathLike:
    """Resolves the path (using `resolve_path`) and checks that the path is
    of the specified type and allows the specified access.
    
    Args:
        path: The path to check.
        path_type: A string or :class:`PathType` ('f' or 'd').
        permissions: Access flag (string, int, Permission, or PermissionSet).
    
    Returns:
        The fully resolved path.
    
    Raises:
        IOError if the path does not exist, is not of the specified type,
        or doesn't allow the specified access.
    """
    path = resolve_path(path)
    if path_type:
        if isinstance(path_type, str):
            path_type = PathType(path_type)
        if path_type == PathType.FILE and not (
                path in (STDOUT, STDERR) or os.path.isfile(str(path))):
            raise IOError(errno.EISDIR, "{} not a file".format(path), path)
        elif path_type == PathType.DIR and not os.path.isdir(str(path)):
            raise IOError(
                errno.ENOTDIR, "{} not a directory".format(path), path)
    if permissions is not None:
        check_access(path, permissions)
    return path

def check_readable_file(path: PathLike) -> PathLike:
    """Check that `path` exists and is readable.
    
    Args:
        path: The path to check
    
    Returns:
        The fully resolved path of `path`
    """
    return check_path(path, PathType.FILE, ModeAccess.READ)

def check_writable_file(path: PathLike, mkdirs: bool = True) -> PathLike:
    """If `path` exists, check that it is writable, otherwise check that
    its parent directory exists and is writable.
    
    Args:
        path: The path to check.
        mkdirs: Whether to create any missing directories (True).
    
    Returns:
        The fully resolved path.
    """
    if os.path.exists(str(path)):
        return check_path(path, PathType.FILE, Permission.WRITE)
    else:
        path = abspath(path)
        dirpath = os.path.dirname(str(path))
        if os.path.exists(str(dirpath)):
            check_path(dirpath, PathType.DIR, Permission.WRITE)
        elif mkdirs:
            os.makedirs(str(dirpath))
        return path

# "Safe" versions of the check methods, meaning they return None
# instead of throwing exceptions

def safe_check_path(path: PathLike, *args, **kwargs) -> PathLike:
    """Safe vesion of `check_path`. Returns None rather than throw an
    exception.
    """
    try:
        return check_path(path, *args, **kwargs)
    except IOError:
        return None

def safe_check_readable_file(path: PathLike) -> PathLike:
    """Safe vesion of `check_readable_file`. Returns None rather than throw an
    exception.
    """
    try:
        return check_readable_file(path)
    except IOError:
        return None

def safe_check_writable_file(path: PathLike) -> PathLike:
    """Safe vesion of `check_writable_file`. Returns None rather than throw
    an exception.
    """
    try:
        return check_writable_file(path)
    except IOError:
        return None

def find(
        root: PathLike, pattern: Regexp,
        path_types: Sequence[PathTypeArg] = 'f',
        recursive: bool = True, return_matches: bool = False
        ) -> Union[Sequence[PathLike], Sequence[Tuple[PathLike, Match]]]:
    """Find all paths under `root` that match `pattern`.
    
    Args:
        root: Directory at which to start search.
        pattern: File name pattern to match (string or re object).
        path_types: Types to return -- files ('f'), directories ('d' or
            both ('fd').
        recursive: Whether to search directories recursively.
        return_matches: Whether to return regular expression match for each
            file.
    
    Returns:
        List of matching paths. If `return_matches` is True, each item will be
        a (path, Match) tuple.
    """
    if isinstance(pattern, str):
        pat = re.compile(pattern)
    else:
        pat = cast(Pattern, pattern)
    
    path_type_set = set(
        PathType(p) if isinstance(p, str) else p
        for p in path_types)
    
    # Whether we need to match the full path or just the filename
    fullmatch = os.sep in pat.pattern
    
    def get_matching(
            names: Iterable[str], parent) -> List[Tuple[str, Match[str]]]:
        """Get all names that match the pattern."""
        if fullmatch:
            names = (os.path.join(parent, name) for name in names)
        matching = []
        for name in names:
            match = pat.fullmatch(name)
            if match:
                path = name if fullmatch else os.path.join(parent, name)
                matching.append((path, match))
        return matching
    
    found = [] # type: List[Tuple[str, Match[str]]]
    for parent, dirs, files in os.walk(str(root)):
        if PathType.DIR in path_type_set:
            found.extend(get_matching(dirs, parent))
        if any(t in path_type_set for t in (PathType.FILE, PathType.FIFO)):
            matching_files = get_matching(files, parent)
            if PathType.FILE not in path_type_set:
                found.extend(
                    f for f in matching_files
                    if stat.S_ISFIFO(os.stat(str(f[0])).st_mode))
            else:
                found.extend(matching_files)
        if not recursive:
            break
    
    if return_matches:
       return tuple(found)
    else:
        return tuple(f[0] for f in found)

class ExecutableCache(object):
    """Lookup and cache executable paths.
    
    Args:
        default_path: The default executable path
    """
    def __init__(
            self, default_path: Iterable[PathLike] = os.get_exec_path()
            ) -> None:
        self.cache = {} # type: Dict[str, PathLike]
        self.search_path = None # type: Tuple[str, ...]
        self.reset_search_path(default_path)

    def add_search_path(
            self, paths: Union[PathLike, Iterable[PathLike]]) -> None:
        """Add directories to the beginning of the executable search path.
        
        Args:
            paths: List of paths, or a string with directories separated by
                `os.pathsep`.
        """
        # pylint: disable=global-statement,invalid-name
        if isinstance(paths, str):
            paths = paths.split(os.pathsep)
        elif isinstance(paths, PathLikeClass):
            paths = [paths]
        self.search_path = tuple(str(p) for p in paths) + self.search_path
    
    def reset_search_path(
            self, default_path: Iterable[PathLike] = os.get_exec_path()
            ) -> None:
        """Reset the search path to `default_path`.
        
        Args:
            default_path: The default executable path.
        """
        self.search_path = ()
        if default_path:
            self.add_search_path(default_path)
    
    def get_path(self, executable: str) -> PathLike:
        """Get the full path of `executable`.
        
        Args:
            executable: A executable name.
        
        Returns:
            The full path of `executable`, or None if the path cannot be
            found.
        """
        exe_name = os.path.basename(executable)
        if exe_name in self.cache:
            return self.cache[exe_name]
        
        exe_file = safe_check_path(
            executable, PathType.FILE, Permission.EXECUTE)
        if not exe_file:
            for path in self.search_path:
                exe_file = safe_check_path(
                    os.path.join(path.strip('"'), executable),
                    PathType.FILE, Permission.EXECUTE)
                if exe_file:
                    break
        
        self.cache[exe_name] = exe_file
        return exe_file
    
    def resolve_exe(self, names: Iterable[str]) -> Tuple:
        """Given an iterable of command names, find the first that resolves to
        an executable.
        
        Args:
            names: An iterable of command names.
        
        Returns:
            A tuple (path, name) of the first command to resolve, or None if
            none of the commands resolve.
        """
        for cmd in names:
            exe = self.get_path(cmd)
            if exe:
                return (exe, cmd)
        return None

EXECUTABLE_CACHE = ExecutableCache()
"""Singleton instance of ExecutableCache."""

# Temporary files and directories

class TempPath(metaclass=ABCMeta):
    """Base class for temporary files/directories.
    
    Args:
        parent: The parent directory.
        permissions: The access permissions.
        path_type: 'f' = file, 'd' = directory.
    """
    def __init__(
            self, parent: 'TempPath' = None,
            permissions: PermissionSetArg = 'rwx',
            path_type: PathTypeArg = 'd') -> None:
        self.parent = parent
        if isinstance(path_type, str):
            path_type = PathType(path_type)
        self.path_type = path_type
        self._permissions = None # type: PermissionSet
        if permissions:
            self._set_permissions_value(permissions)
    
    @property
    @abstractmethod
    def absolute_path(self) -> PathLike:
        """The absolute path.
        """
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def relative_path(self) -> PathLike:
        """The relative path.
        """
        raise NotImplementedError()
    
    @property
    def exists(self) -> bool:
        """Whether the directory exists.
        """
        # pylint: disable=no-member
        return os.path.exists(str(self.absolute_path))
    
    @property
    def permissions(self) -> PermissionSet:
        """The permissions of the path. Defaults to the parent's mode.
        """
        if not self._permissions:
            if self.parent:
                self._permissions = self.parent.permissions
            else:
                raise IOError("Cannot determine permissions without 'parent'")
        return self._permissions
    
    def set_permissions(
            self, permissions: PermissionSetArg = None,
            set_parent: bool = False, additive: bool = False) -> PermissionSet:
        """Set the permissions for the path.
        
        Args:
            permissions: The new flags to set. If None, the existing flags are
                used.
            set_parent: Whether to recursively set the permissions of all
                parents. This is done additively.
            additive: Whether permissions should be additive (e.g.
                if `permissions == 'w'` and `self.permissions == 'r'`, the new
                mode is 'rw').
        
        Returns:
            The PermissionSet representing the flags that were set.
        """
        # pylint: disable=no-member
        if not self.exists:
            return None
        if permissions:
            permissions = self._set_permissions_value(permissions, additive)
        else:
            permissions = self.permissions
        if set_parent and self.parent:
            self.parent.set_permissions(permissions, True, True)
        # Always set 'x' on directories, otherwise they are not listable
        if (self.path_type == PathType.DIR and
                Permission.EXECUTE not in permissions):
            permissions.add(Permission.EXECUTE)
        set_permissions(self.absolute_path, permissions)
        return permissions
    
    def _set_permissions_value(
            self, permissions: PermissionSetArg, additive: bool = False
            ) -> PermissionSet:
        if not isinstance(permissions, PermissionSet):
            permissions = PermissionSet(permissions)
        if additive and (self._permissions or self.parent):
            self.permissions.update(permissions)
        else:
            self._permissions = permissions
        return permissions

class TempPathDescriptor(TempPath):
    """Describes a temporary file or directory within a TempDir.
    
    Args:
        name: The file/directory name.
        parent: The parent directory, a TempPathDescriptor.
        permissions: The permissions mode.
        suffix, prefix: The suffix and prefix to use when calling
            `mkstemp` or `mkdtemp`.
        path_type: 'f' (for file), 'd' (for directory), or '|' (for FIFO).
    """
    def __init__(
            self, name: str = None, parent: TempPath = None,
            permissions: PermissionSetArg = None,
            suffix: str = '', prefix: str = '',
            contents: str = '', path_type: PathTypeArg = 'f') -> None:
        if isinstance(path_type, str):
            path_type = PathType(path_type)
        if contents and path_type != PathType.FILE:
            raise ValueError("'contents' only valid for files")
        super().__init__(parent, permissions, path_type)
        self.name = name
        self.prefix = prefix
        self.suffix = suffix
        self.contents = contents
        self._abspath = None # type: str
        self._relpath = None # type: str
    
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
            raise IOError("Cannot determine absolute path without 'root'")
        self._relpath = os.path.join(str(self.parent.relative_path), self.name)
        self._abspath = os.path.join(str(self.parent.absolute_path), self.name)
    
    def create(self, apply_permissions: bool = True) -> None:
        """Create the file/directory.
        
        Args:
            apply_permissions: Whether to set permissions according to
                `self.permissions`.
        """
        if self.path_type != PathType.DIR:
            if self.path_type == PathType.FIFO:
                if os.path.exists(self.absolute_path):
                    os.remove(self.absolute_path)
                os.mkfifo(self.absolute_path)
            # TODO: Opening a FIFO for write blocks. It's possible to get around
            # this using a subprocess to pipe through a buffering program (such
            # as pv) to the FIFO instead
            if self.path_type != PathType.FIFO:
                with open(self.absolute_path, 'wt') as outfile:
                    outfile.write(self.contents or '')
        elif not os.path.exists(self.absolute_path):
            os.mkdir(self.absolute_path)
        if apply_permissions:
            self.set_permissions()
    
    def __str__(self):
        return "TempPathDescriptor({}, {})".format(self.name, self.path_type)

class TempDir(TempPath):
    """Context manager that creates a temporary directory and cleans it up
    upon exit.
    
    Args:
        mode: Access mode to set on temp directory. All subdirectories and
            files will inherit this mode unless explicity set to be different.
        path_descriptors: Iterable of TempPathDescriptors.
        kwargs: Additional arguments passed to tempfile.mkdtemp.
    
    By default all subdirectories and files inherit the mode of the temporary
    directory. If TempPathDescriptors are specified, the paths are created
    before permissions are set, enabling creation of a read-only temporary file
    system.
    """
    def __init__(
            self, permissions: PermissionSetArg = 'rwx',
            path_descriptors: Iterable[TempPathDescriptor] = None, 
            **kwargs) -> None:
        super().__init__(permissions=permissions)
        self._absolute_path = abspath(tempfile.mkdtemp(**kwargs))
        self._relative_path = '' # type: PathLike
        self.paths = {} # type: Dict[PathLike, TempPathDescriptor]
        if path_descriptors:
            self.make_paths(*path_descriptors)
        self.set_permissions()
    
    @property
    def absolute_path(self) -> PathLike:
        return self._absolute_path
    
    @property
    def relative_path(self) -> PathLike:
        return self._relative_path
    
    def __enter__(self) -> 'TempDir':
        return self
    
    def __exit__(self, exception_type, exception_value, traceback) -> None:
        self.close()
    
    def __getitem__(self, path: PathLike) -> TempPathDescriptor:
        return self.paths[path]
    
    def __contains__(self, path: PathLike) -> bool:
        return path in self.paths
    
    def close(self) -> None:
        """Delete the temporary directory and all files/subdirectories within.
        """
        # First need to make all paths removable
        if not self.exists:
            return
        for path in self.paths.values():
            path.set_permissions('rwx', True)
        shutil.rmtree(str(self.absolute_path))
    
    def make_path(
            self, desc: TempPathDescriptor = None,
            apply_permissions: bool = True, **kwargs) -> PathLike:
        """Create a file or directory within the TempDir.
        
        Args:
            desc: A TempPathDescriptor.
            apply_permissions: Whether permissions should be applied to
                the new file/directory.
            kwargs: Arguments to TempPathDescriptor. Ignored unless `desc`
                is None.
        
        Returns:
            The absolute path to the new file/directory.
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
            if desc.path_type == PathType.DIR:
                path = tempfile.mkdtemp(
                    prefix=desc.prefix, suffix=desc.suffix, dir=str(parent))
                desc.name = os.path.basename(path)
            else:
                path = tempfile.mkstemp(
                    prefix=desc.prefix, suffix=desc.suffix, dir=str(parent))[1]
                desc.name = os.path.basename(path)
        
        desc.create(apply_permissions)
        
        self.paths[desc.absolute_path] = desc
        self.paths[desc.relative_path] = desc
        
        return desc.absolute_path
    
    def make_paths(
            self, *path_descriptors: TempPathDescriptor) -> Sequence[PathLike]:
        """Create multiple files/directories at once. The paths are created
        before permissions are set, enabling creation of a read-only temporary
        file system.
        
        Args:
            path_descriptors: One or more TempPathDescriptor.
        
        Returns:
            A list of the created paths.
        """
        # Create files/directories without permissions
        paths = [
            self.make_path(desc, apply_permissions=False)
            for desc in path_descriptors]
        # Now apply permissions after all paths are created
        for desc in path_descriptors:
            desc.set_permissions()
        return paths
    
    # Convenience methods
    
    def make_file(
            self, desc: TempPathDescriptor = None,
            apply_permissions: bool = True, **kwargs) -> PathLike:
        """Convenience method; calls `make_path` with path_type='f'.
        """
        kwargs['path_type'] = 'f'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_fifo(
            self, desc: TempPathDescriptor = None,
            apply_permissions: bool = True, **kwargs) -> PathLike:
        """Convenience method; calls `make_path` with path_type='|'.
        """
        kwargs['path_type'] = '|'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_directory(
            self, desc: TempPathDescriptor = None,
            apply_permissions: bool = True, **kwargs) -> PathLike:
        """Convenience method; calls `make_path` with `path_type='d'`.
        """
        kwargs['path_type'] = 'd'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_empty_files(self, num_files: int, **kwargs) -> Sequence[PathLike]:
        """Create randomly-named empty files.
        
        Args:
            n: The number of files to create.
            kwargs: Arguments to pass to TempPathDescriptor.
        
        Returns:
            A sequence of paths.
        """
        desc = list(TempPathDescriptor(**kwargs) for i in range(num_files))
        return self.make_paths(*desc)

# User-defined path specifications

PATH_CLASS = pathlib.WindowsPath if os.name == 'nt' else pathlib.PosixPath

class PathInst(PATH_CLASS):
    """A path-like that has a slot for variable values.
    """
    __slots__ = ('values')
    
    def joinpath(self, *other: PathLike) -> 'PathInst':
        """Join two path-like objects, including merging 'values' dicts.
        """
        new_path = super().joinpath(*other)
        new_values = dict(self.values)
        for oth in other:
            if isinstance(oth, PathInst):
                new_values.update(oth.values)
        return path_inst(new_path, new_values)
    
    def __getitem__(self, name: str) -> Any:
        return self.values[name]
    
    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, PathInst) and
            super(PathInst, self).__eq__(other) and
            self.values == other.values)

def path_inst(path: PathLike, values: dict = None) -> PathInst:
    """Create a PathInst from a path and values dict.
    
    Args:
        path: The path.
        values: The values dict.
    
    Returns:
        A PathInst.
    """
    pathinst = PathInst(path)
    pathinst.values = values or {} # pylint: disable=attribute-defined-outside-init
    return pathinst

class PathVar(object):
    """Describes part of a path, used in PathSpec.
    
    Args:
        name: Path variable name
        optional: Whether this part of the path is optional
        default: A default value for this path variable
        pattern: A pattern that the value must match
        valid: Iterable of valid values
        invalid: Iterable of invalid values
    
    If `valid` is specified, `invalid` and `pattern` are ignored. Otherwise,
    values are first checked against `pattern` (if one is specified), then
    checked against `invalid` (if specified).
    """
    def __init__(
            self, name: str, optional: bool = False, default: Any = None,
            pattern: Regexp = None, valid: Iterable[Any] = None,
            invalid: Iterable[Any] = None) -> None:
        self.name = name
        self.optional = optional
        self.default = default
        self.valid = self.invalid = self.pattern = None
        if pattern and isinstance(pattern, str):
            self.pattern = re.compile(pattern)
        else:
            self.pattern = cast(Pattern, pattern)
        if valid:
            self.valid = set(valid)
        elif invalid:
            self.invalid = set(invalid)
    
    def __call__(self, value: str = None) -> Any:
        """Validate a value.
        
        Args:
            The value to validate. If None, the default value is used.
        
        Raises:
            ValueError if any validations fail.
        """
        if value is None:
            if self.default:
                value = self.default
            elif self.optional:
                return ''
            else:
                raise ValueError("{} is required".format(self.name))
        if self.valid:
            if value not in self.valid:
                raise ValueError("{} is not in list of valid values".format(
                    value))
        else:
            if self.pattern and not self.pattern.fullmatch(str(value)):
                raise ValueError("{} does not match pattern {}".format(
                    value, self.pattern))
            if self.invalid and value in self.invalid:
                raise ValueError("{} is in list of invalid values".format(
                    value))
        return value
    
    def as_pattern(self) -> str:
        """Format this variable as a regular expression capture group.
        """
        pattern = self.pattern.pattern if self.pattern else '.*'
        return "(?P<{name}>{pattern}){optional}".format(
            name=self.name, pattern=pattern,
            optional='?' if self.optional else '')
    
    def __str__(self) -> str:
        return "PathVar<{}, optional={}, default={}>".format(
            self.name, self.optional, self.default)

def match_to_dict(
        match: Match, path_vars: Dict[str, PathVar],
        errors: bool = True) -> Dict[str, Any]:
    """Convert a regular expression Match to a dict of (name, value) for
    all PathVars.
    
    Args:
        match: A re.Match.
        path_vars: A dict of PathVars.
        errors: If True, raise an exception on validation error, otherwise
            return None.
    
    Returns:
        A (name, value) dict.
    
    Raises:
        ValueError if any values fail validation.
    """
    match_groups = match.groupdict()
    try:
        return dict(
            (name, var(match_groups.get(name, None)))
            for name, var in path_vars.items())
    except ValueError:
        if errors:
            raise
        else:
            return None

# pylint: disable=no-member
class SpecBase(metaclass=ABCMeta):
    """Base class for :class:`DirSpec` and :class:`FileSpec`.
    
    Args:
        path_vars: Named variables with which to associate parts of a path.
        template: Format string for creating paths from variables.
        pattern: Regular expression for identifying matching paths.
    """
    def __init__(
            self, *path_vars: PathVar, template: str = None,
            pattern: Regexp = None) -> None:
        self.path_vars = dict((v.name, v) for v in path_vars)
        
        if template is None:
            template = '{{{}}}'.format(self.default_var_name)
            self.path_vars[self.default_var_name] = PathVar(
                self.default_var_name, pattern=self.default_pattern)
        
        self.template = template
        
        def escape(strng, chars):
            """Escape special characters in a string.
            """
            for char in chars:
                strng = strng.replace(char, "\{}".format(char))
            return strng
        
        def template_to_pattern(template):
            """Convert a template string to a regular expression.
            """
            pattern = escape(
                template,
                ('\\', '.', '*', '+', '?', '[', ']', '(', ')', '<', '>'))
            pattern += '$'
            pattern_args = dict(
                (name, var.as_pattern())
                for name, var in self.path_vars.items())
            pattern = pattern.format(**pattern_args)
            return escape(pattern, ('{', '}'))
        
        if pattern is None:
            pattern = template_to_pattern(template)
        if isinstance(pattern, str):
            pattern = re.compile(pattern)
        self.pattern = pattern
    
    @property
    @abstractmethod
    def default_var_name(self) -> str:
        """The default variable name used for string formatting.
        """
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def default_pattern(self) -> str:
        """The default filename pattern.
        """
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def path_type(self) -> PathType:
        """The PathType.
        """
        raise NotImplementedError()
    
    def construct(self, **kwargs) -> PathInst:
        """Create a new PathInst from this spec using values in `kwargs`.
        
        Args:
            kwargs: Specify values for path variables.
        
        Returns:
            A PathInst.
        """
        values = dict(
            (name, var(kwargs.get(name, None)))
            for name, var in self.path_vars.items())
        path = self.template.format(**values)
        return path_inst(path, values)
    
    __call__ = construct
    
    def parse(self, path: PathLike, fullpath: bool = False) -> PathInst:
        """Extract PathVar values from `path` and create a new PathInst.
        
        Args:
            path: The path to parse.
        
        Returns: a PathInst.
        """
        path = str(path)
        if fullpath:
            path = self.path_part(os.path.expanduser(path))
        match = self.pattern.fullmatch(path)
        if not match:
            raise ValueError("{} does not match {}".format(path, self))
        return path_inst(path, self._match_to_dict(match))
    
    def _match_to_dict(
            self, match: Match, errors: bool = True) -> Dict[str, Any]:
        """Convert a regular expression Match to a dict of (name, value) for
        all PathVars.
        
        Args:
            match: A :class:`re.Match`.
            errors: If True, raise an exception for validation failure,
                otherwise return None.
        
        Returns:
            A (name, value) dict.
        
        Raises:
            ValueError if any values fail validation.
        """
        return match_to_dict(match, self.path_vars, errors)
    
    def find(
            self, root: PathLike = None,
            recursive: bool = False) -> Sequence[PathInst]:
        """Find all paths in `root` matching this spec.
        
        Args:
            root: Directory in which to begin the search.
            recursive: Whether to search recursively.
        
        Returns:
            A sequence of PathInst.
        """
        if root is None:
            root = self.default_search_root()
        find_results = find(
            root, self.pattern, path_types=[self.path_type],
            recursive=recursive, return_matches=True)
        matches = dict(
            (path, self._match_to_dict(match, errors=False))
            for path, match in cast(
                Sequence[Tuple[str, Match[str]]], find_results))
        return [
            path_inst(path, match)
            for path, match in matches.items()
            if match is not None]
    
    def __str__(self) -> str:
        return "{}<{}, template={}, pattern={}>".format(
            self.__class__.__name__,
            ','.join(str(var) for var in self.path_vars.values()),
            self.template, self.pattern)
    
    @abstractmethod
    def path_part(self, path) -> str:
        """Return the part of the absolute path corresponding to the spec type.
        """
        pass
    
    def default_search_root(self) -> PathLike: # pylint: disable=no-self-use
        """Get the default root directory for searcing.
        """
        raise ValueError("'root' must be specified for FileSpec.find()")


class DirSpec(SpecBase):
    """Spec for the directory part of a path.
    """
    @property
    def default_var_name(self) -> str:
        return 'dir'
    
    @property
    def default_pattern(self) -> str:
        return '.*'
    
    @property
    def path_type(self) -> PathType:
        return PathType.DIR
    
    def path_part(self, path) -> str:
        return os.path.dirname(path)
    
    def default_search_root(self) -> PathLike:
        try:
            idx1 = self.template.index('{')
        except ValueError:
            return self.template
        try:
            idx2 = self.template.rindex(os.sep, 0, idx1)
            return self.template[0:idx2]
        except ValueError:
            return get_root()


class FileSpec(SpecBase):
    """Spec for the filename part of a path.
    
    Examples:
        spec = FileSpec(
            PathVar('id', pattern='[A-Z0-9_]+'),
            PathVar('ext', pattern='[^\.]+'),
            template='{id}.{ext}')
        
        # get a single file
        path = spec(id='ABC123', ext='txt') # => PathInst('ABC123.txt')
        print(path['id']) # => 'ABC123'
        
        # get the variable values for a path
        path = spec.parse('ABC123.txt')
        print(path['id']) # => 'ABC123'
        
        # find all files that match a FileSpec in the user's home directory
        all_paths = spec.find('~') # => [PathInst...]
    """
    @property
    def default_var_name(self) -> str:
        return 'file'
    
    @property
    def default_pattern(self) -> str:
        return '[^{}]*'.format(os.sep)
    
    @property
    def path_type(self) -> PathType:
        return PathType.FILE
    
    def path_part(self, path) -> str:
        return os.path.basename(path)


class PathSpec(object):
    """Specifies a path in terms of a template with named components ("path
    variables").
    
    Args:
        dir_spec: A PathLike if the directory is fixed, otherwise a DirSpec.
        file_spec: A string if the filename is fixed, otherwise a FileSpec.
    """
    def __init__(
            self, dir_spec: Union[PathLike, DirSpec],
            file_spec: Union[str, FileSpec]) -> None:
        self.fixed_dir = self.fixed_file = False
        if not isinstance(dir_spec, DirSpec):
            dir_spec = path_inst(dir_spec)
            self.fixed_dir = True
        if not isinstance(file_spec, FileSpec):
            file_spec = path_inst(file_spec)
            self.fixed_file = True
        self.dir_spec = dir_spec
        self.file_spec = file_spec
        
        if self.fixed_dir:
            dir_spec_str = str(dir_spec)
        else:
            dir_spec_str = dir_spec.pattern.pattern
            if dir_spec_str.endswith('$'):
                dir_spec_str = dir_spec_str[:-1]
        self.pattern = os.path.join(
            dir_spec_str,
            str(file_spec) if self.fixed_file else file_spec.pattern.pattern)
        
        self.path_vars = {} # type: Dict[str, PathVar]
        if not self.fixed_dir:
            self.path_vars.update(self.dir_spec.path_vars)
        if not self.fixed_file:
            self.path_vars.update(self.file_spec.path_vars)
    
    def construct(self, **kwargs) -> PathInst:
        """Create a new PathInst from this PathSpec using values in `kwargs`.
        
        Args:
            kwargs: Specify values for path variables.
        
        Returns:
            A PathInst
        """
        if self.fixed_dir:
            dir_part = cast(PathInst, self.dir_spec)
        else:
            dir_part = self.dir_spec.construct(**kwargs)
        if self.fixed_file:
            file_part = cast(PathInst, self.file_spec)
        else:
            file_part = self.file_spec.construct(**kwargs)
        return dir_part.joinpath(file_part)
    
    __call__ = construct
    
    def parse(self, path: PathLike) -> PathInst:
        """Extract PathVar values from `path` and create a new PathInst.
        
        Args:
            path: The path to parse
        
        Returns: a PathInst
        """
        def parse_part(part, spec, fixed):
            """Parse part of path using 'spec'. Returns 'spec' if fixed is True.
            """
            if fixed:
                inst = spec
                if str(inst) != part:
                    raise ValueError("{} doesn't match {}".format(part, spec))
            else:
                inst = spec.parse(part)
            return inst
        
        dir_part, file_part = os.path.split(str(path))
        dir_inst = file_inst = None
        if dir_part:
            dir_inst = parse_part(dir_part, self.dir_spec, self.fixed_dir)
        if file_part:
            file_inst = parse_part(file_part, self.file_spec, self.fixed_file)
        return dir_inst.joinpath(file_inst) if dir_inst else file_inst
    
    def find(
            self, root: PathLike = None, 
            path_types: Sequence[PathTypeArg] = 'f',
            recursive: bool = False) -> Sequence[PathInst]:
        """Find all paths matching this PathSpec. The search starts in 'root'
        if it is not None, otherwise it starts in the deepest fixed directory
        of this PathSpec's DirSpec.
        
        Args:
            root: Directory in which to begin the search.
            path_types: Types to return -- files ('f'), directories ('d') or
                both ('fd').
            recursive: Whether to search recursively.
        
        Returns:
            A sequence of PathInst.
        """
        if root is None:
            if self.fixed_dir:
                root = str(self.dir_spec)
            else:
                root = self.dir_spec.default_search_root()
        
        files = find(
            root, self.pattern, path_types=path_types, recursive=recursive,
            return_matches=True)
        
        return [
            path_inst(path, match_to_dict(match, self.path_vars))
            for path, match in cast(Sequence[Tuple[str, Match[str]]], files)]
