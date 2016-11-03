# -*- coding: utf-8 -*-
"""Convenience functions for working with file paths.
"""
import errno
import os
import re
import shutil
import stat
import sys
import tempfile

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

def get_access(mode : 'str') -> 'int':
    """Returns the access mode constant associated with given mode string.
    
    Args:
        mode: A mode string
    
    Returns:
        The access mode constant
    
    Raises:
        ValueError, if ``mode`` does not contain a valid mode character.
    
    Examples:
        a = get_access('rb') # -> os.R_OK
    """
    for a, i in ACCESS.items():
        if a in mode:
            return i[0]
    raise ValueError("{} does not contain a valid access mode".format(mode))

def set_access(path, mode):
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

def check_access(path : 'str', access : 'int'):
    """Check that ``path`` is accessible.
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

def abspath(path : 'str') -> 'str':
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

def split_path(path : 'str', keep_seps : 'bool' = True,
               resolve : 'bool' = True) -> 'tuple':
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
        -> ('/current/dir', 'myfile', 'foo', 'txt')
        split_path('/usr/local/foobar.gz', True)
        -> ('/usr/local', 'foobar', '.gz')
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
            seps = tuple('{}{}'.format(os.extsep, ext) for ext in file_parts[1:])
    return (parent, file_parts[0]) + seps

def filename(path : 'str') -> 'str':
    """Returns just the filename part of ``path``. Equivalent to
    ``split_path(path)[1]``.
    """
    return split_path(path)[1]

def resolve_path(path : 'str', parent : 'str' = None) -> 'str':
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

def check_path(path : 'str', ptype : 'str' = None, access=None) -> 'str':
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

def check_readable_file(path : 'str') -> 'str':
    """Check that ``path`` exists and is readable.
    
    Args:
        path: The path to check
    
    Returns:
        The fully resolved path of ``path``
    """
    return check_path(path, 'f', 'r')

def check_writeable_file(path : 'str', mkdirs : 'bool' = True) -> 'str':
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
        else:
            os.makedirs(dirpath)
        return path

### "Safe" versions of the check methods, meaning they return None
### instead of throwing exceptions

def safe_check_path(path : 'str', *args, **kwargs) -> 'str':
    try:
        return check_path(path, *args, **kwargs)
    except IOError:
        return None

def safe_check_readable_file(path : 'str') -> 'str':
    try:
        return check_readable_file(path)
    except IOError:
        return None

def safe_check_writeable_file(path : 'str') -> 'str':
    try:
        return check_writeable_file(path)
    except IOError:
        return None

def find(root : 'str', pattern, types : 'str' = 'f',
         recursive : 'bool' = True) -> 'list':
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
    for root, dirs, files in os.walk(root):
        if types != "f":
            for d in filter(lambda x: pattern.match(x), dirs):
                found.append(os.path.join(root, d))
        if types != "d":
            for f in filter(lambda x: pattern.match(x), files):
                found.append(os.path.join(root, f))
    return found

executable_cache = {}
"""Cache of full paths to executables"""

def get_executable_path(executable : 'str') -> 'str':
    """Get the full path of ``executable``.
    
    Args:
        executable: A executable name
    
    Returns:
        The full path of ``executable``, or None if the path cannot be found.
    """
    exe_name = os.path.basename(executable)
    if exe_name in executable_cache:
        return executable_cache[exe_name]
    
    def check_executable(fpath):
        try:
            return check_path(fpath, 'f', 'x')
        except:
            return None
    
    exe_file = check_executable(executable)
    if not exe_file:
        for path in os.get_exec_path():
            exe_file = check_executable(os.path.join(path.strip('"'), executable))
            if exe_file:
                break
    
    executable_cache[exe_name] = exe_file
    return exe_file

# tempfiles

class TempPath(object):
    def __init__(self, parent=None, mode='rwx', path_type='d'):
        self.parent = parent
        self.path_type = path_type
        self._mode = mode
    
    @property
    def mode(self):
        """Get the access mode of the path. Defaults to the parent's mode.
        """
        if not self._mode:
            if self.parent:
                self._mode = self.parent.mode
            else:
                raise Exception("Cannot determine mode without 'parent'")
        return self._mode
    
    def set_access(self, mode=None, set_parent=False, additive=False):
        """Set the access mode for the path.
        
        Args:
            mode: The new mode to set. If node, the existing mode is used
            set_parent: Whether to recursively set the mode of all parents.
                This is done additively.
            additive: Whether permissions should be additive (e.g.
                if ``mode == 'w'`` and ``self.mode == 'r'``, the new mode
                is 'rw')
        """
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
    def __init__(self, name=None, parent=None, mode=None,
                 suffix='', prefix='', contents='', path_type='f'):
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
    def exists(self):
        return self._abspath is not None and os.path.exists(self._abspath)
    
    @property
    def absolute_path(self):
        if self._abspath is None:
            self._init_path()
        return self._abspath
    
    @property
    def relative_path(self):
        if self._relpath is None:
            self._init_path()
        return self._relpath
    
    def _init_path(self):
        if self.parent is None:
            raise Exception("Cannot determine absolute path without 'root'")
        self._relpath = os.path.join(self.parent.relative_path, self.name)
        self._abspath = os.path.join(self.parent.absolute_path, self.name)
    
    def create(self, apply_permissions=True):
        if self.path_type not in ('d', 'dir'):
            if self.path_type == 'fifo':
                if os.path.exists(self.absolute_path):
                    os.remove(self.absolute_path)
                os.mkfifo(self.absolute_path)
            # TODO: Opening a FIFO for write blocks. It's possible to get around
            # this using a subprocess to pipe through a buffering program (such
            # as pv) to the FIFO instead
            if self.path_type != 'fifo':
                with open(self.absolute_path, 'wt') as fh:
                    fh.write(self.contents or '')
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
    def __init__(self, mode='rwx', path_descriptors=None, **kwargs):
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
    
    def __getitem__(self, path):
        return self.paths[path]
    
    def __contains__(self, path):
        return path in self.paths
    
    @property
    def exists(self):
        return os.path.exists(self.absolute_path)
    
    def close(self):
        """Delete the temporary directory and all files/subdirectories within.
        """
        # First need to make all paths removable
        if not self.exists:
            return
        for path in self.paths.values():
            path.set_access('rwx', True)
        shutil.rmtree(self.absolute_path)
    
    def make_path(self, desc=None, apply_permissions=True, **kwargs):
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
    
    def make_paths(self, *path_descriptors):
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
            result = desc.set_access()
        return paths
    
    # Convenience methods
    
    def make_file(self, desc=None, apply_permissions=True, **kwargs):
        kwargs['path_type'] = 'f'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_fifo(self, desc=None, apply_permissions=True, **kwargs):
        kwargs['path_type'] = 'fifo'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_directory(self, desc=None, apply_permissions=True, **kwargs):
        kwargs['path_type'] = 'd'
        return self.make_path(desc, apply_permissions, **kwargs)
    
    def make_empty_files(self, n, **kwargs):
        """Create randomly-named empty files.
        
        Args:
            n: The number of files to create
            kwargs: Arguments to pass to TempPathDescriptor.
        """
        return list(
            self.make_path(TempPathDescriptor(**kwargs))
            for i in range(n))
