from contextlib import contextmanager
import os
import stat
import tempfile

@contextmanager
def make_file(mode, contents=None):
    path = tempfile.mkstemp()[1]
    if contents:
        fh.write(contents)
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
