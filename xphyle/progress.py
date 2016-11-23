# -*- coding: utf-8 -*-
"""Common interface to enable operations to be wrapped in a progress bar.
By default, tqdm is used. The user can specify a different progress bar wrapper
by setting ``xphyle.progress.wrapper`` to a callable.
"""
import shlex
from subprocess import Popen, PIPE
from xphyle.paths import get_executable_path, check_path

# pylint: disable=R0903
# pylint: disable=C0103

class TqdmWrapper(object):
    """Default python progress bar wrapper.
    """
    def __init__(self):
        import tqdm
        self.wrapper_fn = tqdm.tqdm
    
    def __call__(self, itr, desc, size):
        return self.wrapper_fn(itr, desc=desc, total=size)

_wrapper = None

def set_wrapper(wrapper: 'bool|callable' = True):
    """Set the python progress wrapper.
    
    Args:
        wrapper: True = use default progress wrapper; False or None means turn
            off progress bars; otherwise a callable that takes three arguments,
            itr, desc, size, and returns an iterable.
    """
    # pylint: disable=W0603,redefined-variable-type
    global _wrapper
    if wrapper is True:
        try:
            _wrapper = TqdmWrapper()
        except ImportError:
            _wrapper = None
    elif callable(wrapper):
        _wrapper = wrapper
    else:
        _wrapper = None

def wrap(itr, desc=None, size=None):
    """Wrap an iterable in a progress bar.
    
    Args:
        desc: Optional description
        size: Optional max value of the progress bar
    """
    return _wrapper(itr, desc, size) if _wrapper else itr

def system_progress_command(exe, *args, require=False): # pragma: no-cover
    """Resolve the system-level progress bar command.
    
    Args:
        exe: The executable name or absolute path
        args: A list of additional command line arguments
        require: Whether to raise an exception if the command does not exist
    
    Returns:
        A tuple of (executable_path, *args)
    """
    executable_path = get_executable_path(exe)
    if executable_path is not None:
        check_path(executable_path, 'f', 'x')
    elif require:
        raise IOError("pv is not available on the path")
    return (executable_path,) + tuple(args)

def pv_command(require=False): # pragma: no-cover
    """Default system wrapper command.
    """
    return system_progress_command('pv', '-pre', require=require)

_system_wrapper = None

def set_system_wrapper(wrapper : 'bool|callable' = True):
    """Set the python system progress wrapper.
    
    Args:
        wrapper: True = use default system progress wrapper; False or None means
            turn off system progress bars; a string or list/tuple to provide the
            system command; otherwise a callable that takes returns a command in
            list form.
    """
    # pylint: disable=W0603,redefined-variable-type
    global _system_wrapper
    if wrapper is True:
        try:
            _system_wrapper = pv_command()
        except ImportError:
            _system_wrapper = None
    elif wrapper in (False, None):
        _system_wrapper = None
    elif isinstance(wrapper, str):
        _system_wrapper = shlex.split(wrapper)
    else:
        _system_wrapper = wrapper

def wrap_subprocess(cmd, stdin, stdout, **kwargs): # pragma: no-cover
    """Pipe a system command through a progress bar program.
    """
    if not _system_wrapper or (stdin is None and stdout is None):
        return Popen(cmd, stdin=stdin, stdout=stdout, **kwargs)
    
    if stdin is not None:
        p1 = Popen(_system_wrapper, stdin=stdin, stdout=PIPE)
        p2 = Popen(cmd, stdin=p1.stdout, stdout=stdout)
    else:
        p1 = Popen(cmd, stdout=PIPE)
        p2 = Popen(_system_wrapper, stdin=p1.stdout, stdout=stdout)
    p1.stdout.close()
    return p2
