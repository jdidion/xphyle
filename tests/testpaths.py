from contextlib import contextmanager
import stat
import tempfile
from xphyle.paths import *

@contextmanager
def make_file(mode, contents=None):
    fh, path = tempfile.mkstemp()
    if contents:
        fh.write(contents)
    fh.close()
    mode_flag = 0
    if 'r' in mode:
        mode_flag |= stat.S_IREAD
    if 'w' in mode:
        mode_flag |= stat.S_IWRITE
    if 'x' in mode:
        mode_flag |= stat.S_IEXEC
    os.chmod(path, mode_flag)
    try:
        yield path
    finally:
        os.remove(path)

def test_check_access_std():
    check_access(STDOUT, 'r')
    check_access(STDOUT, 'w')
    check_access(STDERR, 'w')
    check_access(STDOUT, 'a')
    check_access(STDERR, 'a')
    
