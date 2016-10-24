from contextlib import contextmanager
import os
import random
import stat
import tempfile
import shutil

class FileDescriptor(object):
    def __init__(self, mode='rwx', suffix='', prefix='', contents=None):
        self.prefix = prefix
        self.suffix = suffix
        self.mode = mode
        self.contents = contents

@contextmanager
def make_files(*file_descriptors, parent="."):
    parent = os.path.abspath(os.path.expanduser(parent))
    filepaths = []
    for desc in file_descriptors:
        path = tempfile.mkstemp(
            suffix=desc.suffix,
            prefix=desc.prefix,
            dir=parent)[1]
        if desc.contents:
            fh.write(desc.contents)
        chmod(path, desc.mode)
        filepaths.append(path)
    try:
        yield filepaths
    finally:
        for path in filepaths:
            os.remove(path)

@contextmanager
def make_empty_files(n, *args, parent='.', **kwargs):
    descriptors = [FileDescriptor(*args, **kwargs) for i in range(n)]
    with make_files(*descriptors, parent=parent) as filepaths:
        yield filepaths

@contextmanager
def make_file(*args, parent=".", **kwargs):
    desc = FileDescriptor(*args, **kwargs)
    with make_files(desc, parent=parent) as files:
        yield files[0]

@contextmanager
def make_dir(mode='rwx', parent=".", suffix='', prefix=''):
    parent = os.path.abspath(os.path.expanduser(parent))
    dirpath = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=parent)
    chmod(dirpath, mode)
    try:
        yield dirpath
    finally:
        if 'r' not in mode:
            chmod(dirpath, 'rwx') # ensure permission allow deletion
        shutil.rmtree(dirpath)
        
def chmod(path, mode):
    mode_flag = 0
    if 'r' in mode:
        mode_flag |= stat.S_IREAD
    if 'w' in mode:
        mode_flag |= stat.S_IWRITE
    if 'x' in mode:
        mode_flag |= stat.S_IEXEC
    os.chmod(path, mode_flag)

def random_text(n):
    return ''.join(chr(random.randint(32, 126)) for i in range(n))
