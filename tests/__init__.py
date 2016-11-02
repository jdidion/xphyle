from contextlib import contextmanager
from io import StringIO
import os
import random
import stat
import sys
import tempfile
import shutil
from xphyle.paths import TempDir, set_access

class FileDescriptor(object):
    def __init__(self, mode='rwx', suffix='', prefix='', contents=None):
        self.prefix = prefix
        self.suffix = suffix
        self.mode = mode
        self.contents = contents

class TestTempDir(TempDir):
    def make_files(self, *file_descriptors, subdir=None):
        filepaths = []
        for desc in file_descriptors:
            path = self.get_temp_file(desc.prefix, desc.suffix, subdir)
            if desc.contents:
                with open(path, 'wt') as fh:
                    fh.write(desc.contents)
            set_access(path, desc.mode)
            filepaths.append(path)
        return filepaths
    
    def make_empty_files(self, n, *args, subdir=None, **kwargs):
        descriptors = [FileDescriptor(*args, **kwargs) for i in range(n)]
        return self.make_files(*descriptors, subdir=subdir)
    
    def make_file(self, *args, subdir=None, **kwargs):
        desc = FileDescriptor(*args, **kwargs)
        return self.make_files(desc, subdir=subdir)[0]

def random_text(n=1024):
    return ''.join(chr(random.randint(32, 126)) for i in range(n))

@contextmanager
def intercept_stdout(i):
    try:
        sys.stdout = i
        yield
    finally:
        sys.stdout = sys.__stdout__

@contextmanager
def intercept_stderr(i):
    try:
        sys.stderr = i
        yield
    finally:
        sys.stderr = sys.__stderr__

@contextmanager
def intercept_stdin(content):
    i = StringIO()
    i.write(content)
    if not content.endswith('\n'):
        i.write('\n')
    i.seek(0)
    try:
        sys.stdin = i
        yield
    finally:
        sys.stdin = sys.__stdin__
