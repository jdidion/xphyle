from contextlib import contextmanager
from io import StringIO
import os
import random
import stat
import sys
import tempfile
import shutil

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
