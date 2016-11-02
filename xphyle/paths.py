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

class TempDir(object):
    """Context manager that creates a temporary directory and cleans it up
    upon exit.
    
    Args:
        kwargs: Arguments to pass to tempfile.mkdtemp
    """
    def __init__(self, mode='rwx', parent=".", suffix='', prefix=''):
        self.root = abspath(tempfile.mkdtemp(
            suffix=suffix, prefix=prefix, dir=parent))
        set_access(self.root, mode)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
    
    def close(self):
        shutil.rmtree(self.root)
    
    def get_temp_file(self, prefix='', suffix='', subdir=None, mode='rwx'):
        parent = self.root
        if subdir:
            parent = self.get_directory(subdir, True)
        path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=parent)[1]
        set_access(path, mode)
        return path
    
    def get_temp_dir(self, prefix='', suffix='', subdir=None, mode='rwx'):
        parent = os.path.join(self.root, subdir) if subdir else self.root
        path = tempfile.mkdtemp(prefix=prefix, suffix=suffix, dir=parent)
        set_access(path, mode)
        return path
    
    def get_file(self, name, subdir=None, make_dirs=False, mode='rwx'):
        parent = self.root
        if subdir:
            parent = self.get_directory(subdir, make_dirs, mode)
        return os.path.join(parent, name)
    
    def get_directory(self, path, make_dirs=False, mode='rwx'):
        d = os.path.join(self.root, path)
        if make_dirs:
            os.makedirs(d, exist_ok=True)
        set_access(d, mode)
        return d
        
    def make_fifos(self, *names, subdir=None, mode='rwx', **kwargs):
        fifo_paths = [
            self.get_path(name, subdir, make_dirs=True, mode=mode)
            for name in names]
        for path in fifo_paths:
            os.mkfifo(path, **kwargs)
        return fifo_paths
