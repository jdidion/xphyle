from contextlib import contextmanager
from io import StringIO, BytesIO, TextIOWrapper
import os
import random
import stat
import sys
import tempfile
import shutil
import urllib.request

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
def intercept_stdin(content, is_bytes=False):
    if is_bytes:
        i = BytesIO()
    else:
        i = StringIO()
    i.write(content)
    if not is_bytes:
        linesep = b'\n' if is_bytes else '\n'
        if not content.endswith(linesep):
            i.write(linesep)
    i.seek(0)
    if is_bytes:
        i = TextIOWrapper(i)
    try:
        sys.stdin = i
        yield
    finally:
        sys.stdin = sys.__stdin__

def no_internet():
    """Test whether there's no internet connection available.
    """
    try:
        urllib.request.urlopen("https://github.com").info()
        return False
    except:
        return True
