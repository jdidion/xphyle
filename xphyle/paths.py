# -*- coding: utf-8 -*-
"""Convenience functions for working with file paths.
"""
import errno
import os
import pathlib
import re
import shutil
import stat
import sys
import tempfile
from xphyle.types import (
    Sequence, Tuple, Union, Iterable, Dict, Pattern, Match, Any)

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

if sys.version_info >= (3, 6):
    PathLike = Union[str, os.PathLike]
else:
    PathLike = Union[str, pathlib.PurePath]
"""Type representing either a string path or a path-like object. In
python >= 3.6, path-like means is a subclass of os.PathLike, otherwise means
is a subclass of pathlib.PurePath.
"""

def check_file_mode(mode: str) -> int:
    """Check that a file mode string is valid:
    
    Args:
        mode: File mode string to check
    
    Raises:
        ValueError if the file mode string is invalid
    """
    # check that 'mode' contains an access character
    get_access(mode)
    # other checks
    if len(mode) > 2:
        raise ValueError("'mode' can be at most 2 characters")
    diff = set(mode) - (set(ACCESS.keys()) | set(('b', 't', 'U')))
    if diff:
        raise ValueError("'mode' contains invalid character(s): ".format(
            ','.join(diff)))

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

def get_root(path: [str, PathLike] = None) -> str:
    """Get the root directory.
    
    Args:
        str: A path, or '.' to get the root of the working directory, or None
            to get the root of the path to the script.
    
    Returns:
        A path to the root directory.
    """
    path = str(path) if path else sys.executable
    root = os.path.splitdrive(abspath(path))[0]
    if root == '':
        root = os.sep
    return root

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
         recursive: bool = True, return_matches: bool = False
        ) -> Sequence[Union[str, Tuple[str, Match]]]:
    """Find all paths under ``root`` that match ``pattern``.
    
    Args:
        root: Directory at which to start search
        pattern: File name pattern to match (string or re object)
        types: Types to return -- files ("f"), directories ("d") or both ("fd")
        recursive: Whether to search directories recursively
        return_matches: Whether to return regular expression match for each file
    
    Returns:
        List of matching paths. If `return_matches` is True, each item will be
        a (path, Match) tuple.
    """
    if isinstance(pattern, str):
        pattern = re.compile(pattern)
    
    # Whether we need to match the full path or just the filename
    fullmatch = os.sep in pattern.pattern
    
    def get_matching(names, parent):
        if fullmatch:
            names = (os.path.join(parent, name) for name in names)
        matching = []
        for name in names:
            match = pattern.fullmatch(name)
            if match:
                path = name if fullmatch else os.path.join(parent, name)
                if return_matches:
                    matching.append((path, match))
                else:
                    matching.append(path)
        return matching
    
    found = []
    for parent, dirs, files in os.walk(root):
        if types != "f":
            found.extend(get_matching(dirs, parent))
        if types != "d":
            found.extend(get_matching(files, parent))
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
                ``os.pathsep``
        """
        # pylint: disable=global-statement,invalid-name
        if isinstance(paths, str):
            paths = paths.split(os.pathsep)
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
                raise IOError("Cannot determine mode without 'parent'")
        return self._mode
    
    def set_access(self, mode: str = None, set_parent: bool = False,
                   additive: bool = False) -> str:
        """Set the access mode for the path.
        
        Args:
            mode: The new mode to set. If None, the existing mode is used
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
            raise IOError("Cannot determine absolute path without 'root'")
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
    
    def make_path(self, desc: TempPathDescriptor = None,
                  apply_permissions: bool = True, **kwargs) -> str:
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
                  **kwargs) -> str:
        """Convenience method; calls ``make_path`` with path_type='f'.
        """
        kwargs['path_type'] = 'f'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_fifo(self, desc: str = None, apply_permissions: bool = True,
                  **kwargs) -> str:
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

# User-defined path specifications

path_class = pathlib.WindowsPath if os.name == 'nt' else pathlib.PosixPath
class PathInst(path_class):
    __slots__ = ('values')
    
    def joinpath(self, *other):
        new_path = super(PathInst, self).joinpath(*other)
        new_values = dict(self.values)
        for oth in other:
            if isinstance(oth, PathInst):
                new_values.update(oth.values)
        return path_inst(new_path, new_values)
    
    def __getitem__(self, name) -> Any:
        return self.values[name]
    
    def __eq__(self, other):
        return (
            isinstance(other, PathInst) and
            super(PathInst, self).__eq__(other) and
            self.values == other.values)

def path_inst(path: PathLike, values: dict = None) -> PathInst:
    p = PathInst(path)
    p.values = values or {}
    return p

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
    def __init__(self, name: str, optional: bool = False, default: Any = None,
                 pattern: Union[str, Pattern] = None,
                 valid: Iterable[Any] = None, invalid: Iterable[Any] = None):
        self.name = name
        self.optional = optional
        self.default = default
        self.valid = self.invalid = self.pattern = None
        if pattern and isinstance(pattern, str):
            pattern = re.compile(pattern)
        self.pattern = pattern
        if valid:
            self.valid = set(valid)
        elif invalid:
            self.invalid = set(invalid)
    
    def __call__(self, value: str = None) -> Any:
        """Validate a value.
        
        Args:
            The value to validate. If None, the default value is used.
        
        Raises:
            ValueError if any validations fail
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
    
    def __str__(self):
        return "PathVar<{}, optional={}, default={}>".format(
            self.name, self.optional, self.default)

def match_to_dict(match: Match, path_vars: Dict[str, PathVar],
                  errors: bool = True) -> Dict[str, Any]:
    """Convert a regular expression Match to a dict of (name, value) for
    all PathVars .
    
    Args:
        match: A re.Match
        path_vars: A dict of PathVars
        errors: If True, raise an exception on validation error, otherwise
            return None
    
    Returns:
        A (name, value) dict
    
    Raises:
        ValueError if any values fail validation
    """
    match_groups = match.groupdict()
    try:
        return dict(
            (name, var(match_groups.get(name, None)))
            for name, var in path_vars.items())
    except:
        if errors:
            raise
        else:
            return None

class SpecBase(object):
    """
    
    Args:
        path_vars: Named variables with which to associate parts of a path
        template: Format string for creating paths from variables
        pattern: Regular expression for identifying matching paths
    
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
    def __init__(self, *path_vars: Iterable[PathVar], template: str = None,
                 pattern: Union[str, Pattern] = None):
        self.path_vars = dict((v.name, v) for v in path_vars)
        
        if template is None:
            template = '{{{}}}'.format(self.default_var_name)
            self.path_vars[self.default_var_name] = PathVar(
                self.default_var_name, pattern=self.default_pattern)
        
        self.template = template
        
        def escape(s, chars):
            for ch in chars:
                s = s.replace(ch, "\{}".format(ch))
            return s
        
        def template_to_pattern(template):
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
    
    def construct(self, **kwargs) -> PathInst:
        """Create a new PathInst from this spec using values in `kwargs`.
        
        Args:
            kwargs: Specify values for path variables.
        
        Returns:
            A PathInst
        """
        values = dict(
            (name, var(kwargs.get(name, None)))
            for name, var in self.path_vars.items())
        path = self.template.format(**values)
        return path_inst(path, values)
    
    __call__ = construct
    
    def parse(self, path: Union[str, PathLike], fullpath: bool = False
             ) -> PathInst:
        """Extract PathVar values from `path` and create a new PathInst.
        
        Args:
            path: The path to parse
        
        Returns: a PathInst
        """
        path = str(path)
        if fullpath:
            path = self.path_part(os.path.expanduser(path))
        match = self.pattern.fullmatch(path)
        if not match:
            raise ValueError("{} does not match {}".format(path, self))
        return path_inst(path, self._match_to_dict(match))
    
    def _match_to_dict(self, match: Match, errors: bool = True
                      ) -> Dict[str, Any]:
        """Convert a regular expression Match to a dict of (name, value) for
        all PathVars.
        
        Args:
            match: A re.Match
            errors: If True, raise an exception for validation failure,
                otherwise return None
        
        Returns:
            A (name, value) dict
        
        Raises:
            ValueError if any values fail validation
        """
        return match_to_dict(match, self.path_vars, errors)
    
    def find(self, root: Union[str, PathLike] = None, recursive: bool = False
            ) -> Sequence[PathInst]:
        """Find all paths in `root` matching this spec.
        
        Args:
            root: Directory in which to begin the search
            recursive: Whether to search recursively
        
        Returns:
            A sequence of PathInst
        """
        if root is None:
            root = self.default_search_root()
        matches = dict(
            (path, self._match_to_dict(match, errors=False))
            for path, match in find(
                root, self.pattern, types=self.path_type,
                recursive=recursive, return_matches=True))
        return [
            path_inst(path, match)
            for path, match in matches.items()
            if match is not None]
    
    def __str__(self):
        return "{}<{}, template={}, pattern={}>".format(
            self.__class__.__name__,
            ','.join(str(var) for var in self.path_vars.values()),
            self.template, self.pattern)

class DirSpec(SpecBase):
    """Spec for the directory part of a path.
    """
    default_var_name = 'dir'
    default_pattern = '.*'
    path_type = 'd'
    
    def path_part(self, path):
        return os.path.dirname(path)
    
    def default_search_root(self):
        try:
            i1 = self.template.index('{')
        except:
            return self.template
        try:
            i2 = self.template.rindex(os.sep, 0, i1)
            return self.template[0:i2]
        except:
            return get_root()
    
class FileSpec(SpecBase):
    """Spec for the filename part of a path.
    """
    default_var_name = 'file'
    default_pattern = '[^{}]*'.format(os.sep)
    path_type = 'f'
    
    def path_part(self, path):
        return os.path.basename(path)
    
    def default_search_root(self):
        raise ValueError("'root' must be specified for FileSpec.find()")

class PathSpec(object):
    """Specifies a path in terms of a template with named components ("path
    variables").
    
    Args:
        dir_spec: A PathLike if the directory is fixed, otherwise a DirSpec.
        file_spec: A string if the filename is fixed, otherwise a FileSpec.
    """
    def __init__(self, dir_spec: Union[PathLike, DirSpec],
                 file_spec: [str, FileSpec]):
        self.fixed_dir = self.fixed_file = False
        if not isinstance(dir_spec, DirSpec):
            dir_spec = path_inst(dir_spec)
            self.fixed_dir = True
        if not isinstance(file_spec, FileSpec):
            file_spec = path_inst(file_spec)
            self.fixed_file = True
        self.dir_spec = dir_spec
        self.file_spec = file_spec
        
        if not self.fixed_dir:
            dir_spec = dir_spec.pattern.pattern
            if dir_spec.endswith('$'):
                dir_spec = dir_spec[:-1]
        self.pattern = os.path.join(
            str(dir_spec),
            str(file_spec) if self.fixed_file else file_spec.pattern.pattern)
        
        self.path_vars = {}
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
        dir_part = self.dir_spec if self.fixed_dir else self.dir_spec(**kwargs)
        file_part = self.file_spec if self.fixed_file else self.file_spec(**kwargs)
        return dir_part.joinpath(file_part)
    
    __call__ = construct
    
    def parse(self, path: Union[str, PathLike]) -> PathInst:
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
    
    def find(self, root: Union[str, PathLike] = None, types: str = 'f',
             recursive: bool = False) -> Sequence[PathInst]:
        """Find all paths matching this PathSpec. The search starts in 'root'
        if it is not None, otherwise it starts in the deepest fixed directory
        of this PathSpec's DirSpec.
        
        Args:
            root: Directory in which to begin the search
            types: Types to return -- files ("f"), directories ("d") or both
                ("fd")
            recursive: Whether to search recursively
        
        Returns:
            A sequence of PathInst
        """
        if root is None:
            if self.fixed_dir:
                root = str(self.dir_spec)
            else:
                root = self.dir_spec.default_search_root()
        
        files = find(root, self.pattern, types=types, recursive=recursive,
                     return_matches=True)
        return [
            path_inst(path, match_to_dict(match, self.path_vars))
            for path, match in files]
