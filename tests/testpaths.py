from unittest import TestCase
import os
from xphyle.paths import *
from . import *

class PathTests(TestCase):
    def test_check_access_std(self):
        check_access(STDOUT, 'r')
        check_access(STDOUT, 'w')
        check_access(STDERR, 'w')
        check_access(STDOUT, 'a')
        check_access(STDERR, 'a')
        
    def test_check_access_file(self):
        with make_file('rwx') as path:
            check_access(path, 'r')
            check_access(path, 'w')
            check_access(path, 'x')

    def test_no_access(self):
        with self.assertRaises(IOError):
            with make_file('r') as path:
                check_access(path, 'w')
    
    def test_abspath_home(self):
        home = os.path.expanduser("~")
        self.assertEqual(abspath('~/foo'), os.path.join(home, 'foo'))
    
    def test_abspath_rel(self):
        cwd = os.getcwd()
        self.assertEqual(abspath('foo'), os.path.join(cwd, 'foo'))
    
    def test_splitext(self):
        self.assertTupleEqual(
            splitext('/path/to/foo', keep_seps=False),
            ('foo',))
        self.assertTupleEqual(
            splitext('foo.tar.gz', keep_seps=False),
            ('foo', 'tar', 'gz'))
        self.assertTupleEqual(
            splitext('foo.tar.gz', keep_seps=True),
            ('foo', '.tar', '.gz'))
    
    def test_filename(self):
        self.assertEqual(filename('/path/to/foo.tar.gz'), 'foo')
    
    def test_resolve_std(self):
        self.assertEqual(STDOUT, resolve_path(STDOUT))
        self.assertEqual(STDERR, resolve_path(STDERR))
    
    def test_resolve
