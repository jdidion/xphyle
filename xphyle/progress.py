# -*- coding: utf-8 -*-
"""Common interface to enable operations to be wrapped in a progress bar.
By default, tqdm is used. The user can specify a different progress bar wrapper
by setting ``xphyle.progress.wrapper`` to a callable.
"""
import shlex
from subprocess import Popen, PIPE
from xphyle.paths import get_executable_path, check_path

wrapper = False
system_wrapper = False

class TqdmWrapper(object): # pragma: no-cover
    """Default python progress bar wrapper.
    """
    def __init__(self):
        import tqdm
        self.fn = tqdm.tqdm
    
    def __call__(self, itr, desc, size):
        return self.fn(itr, desc=desc, total=size)

def wrap(itr, desc=None, size=None):
    """Wrap an iterable in a progress bar.
    
    Args:
        desc: Optional description
        size: Optional max value of the progress bar
    """
    global wrapper
    if wrapper is True:
        try:
            wrapper = TqdmWrapper()
        except:
            wrapper = False
    if wrapper:
        return wrapper(itr, desc, size)
    return itr

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
    system_progress_command('pv', '-pre', require=require)

def wrap_subprocess(cmd, stdin, stdout, **kwargs): # pragma: no-cover
    """Pipe a system command through a progress bar program.
    """
    global system_wrapper
    if system_wrapper is True:
        try:
            system_wrapper = pv_command()
        except:
            system_wrapper = False
    
    if isinstance(system_wrapper, str):
        system_wrapper = shlex.split(system_wrapper)
    
    if not system_wrapper or (stdin is None and stdout is None):
        return Popen(cmd, stdin=stdin, stdout=stdout, **kwargs)
    
    if stdin is not None:
        p1 = Popen(system_wrapper, stdin=stdin, stdout=PIPE)
        p2 = Popen(cmd, stdin=p1.stdout, stdout=stdout)
    else:
        p1 = Popen(cmd, stdout=PIPE)
        p2 = Popen(system_wrapper, stdin=p1.stdout, stdout=stdout)
    p1.stdout.close()
    return p2
