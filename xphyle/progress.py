# -*- coding: utf-8 -*-
"""Common interface to enable operations to be wrapped in a progress bar.
By default, tqdm is used. The user can specify a different progress bar wrapper
by setting ``xphyle.progress.wrapper`` to a callable.
"""
from subprocess import Popen, PIPE
from xphyle.paths import get_executable_path, check_path

wrapper = False
system_wrapper = False

class TqdmWrapper(object): # pragma: no-cover
    def __init__(self):
        import tqdm
        self.fn = tqdm.tqdm
    
    def __call__(self, itr, size):
        return self.fn(itr, total=size)

def wrap(itr, size=None):
    global wrapper
    if wrapper is True:
        try:
            wrapper = TqdmWrapper()
        except:
            wrapper = False
    if wrapper:
        return wrapper(itr, size)
    return itr

def pv_command(): # pragma: no-cover
    if pv is None:
        pv = get_executable_path('pv')
    if pv is None:
        raise IOError("pv is not available on the path")
    check_path(pv, 'f', 'x')
    return "{} -pre".format(pv)

def wrap_subprocess(cmd, stdin, stdout, **kwargs): # pragma: no-cover
    global system_wrapper
    if system_wrapper is True:
        try:
            system_wrapper = pv_command()
        except:
            system_wrapper = False
    
    if not system_wrapper or (stdin is None and stdout is None):
        return Popen(cmd, stdin=stdin, stdout=stdout, **kwargs)
    
    if stdin is not None:
        p1 = Popen(system_wrapper, stdin=stdin, stdout=PIPE)
        p2 = Popen(cmd, stdin=p1.stdout, stdout=stdout)
        p1.stdout.close()
    else:
        p1 = Popen(cmd, stdout=PIPE)
        p2 = Popen(system_wrapper, stdin=p1.stdout, stdout=stdout)
    
    return p2
