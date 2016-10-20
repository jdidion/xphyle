from unittest import TestCase
import os
from xphyle.paths import *
from . import *

class PathTests(TestCase):
    def test_invalid_access(self):
        with self.assertRaises(ValueError):
            get_access('z')
    
    def test_check_access_std(self):
        check_access(STDOUT, 'r')
        check_access(STDOUT, 'w')
        check_access(STDERR, 'w')
        check_access(STDOUT, 'a')
        check_access(STDERR, 'a')
        with self.assertRaises(IOError):
            check_access(STDOUT, 'x')
        with self.assertRaises(IOError):
            check_access(STDERR, 'r')
    
    def test_check_access_file(self):
        with make_file('rwx') as path:
            check_access(path, 'r')
            check_access(path, 'w')
            check_access(path, 'x')

    def test_no_access(self):
        with self.assertRaises(IOError):
            with make_file('r') as path:
                check_access(path, 'w')
    
    def test_abspath_std(self):
        self.assertEqual(abspath(STDOUT), STDOUT)
        self.assertEqual(abspath(STDERR), STDERR)
    
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
    
    def test_resolve_file(self):
        with make_file() as path:
            self.assertEqual(abspath(path), resolve_path(path))
    
    def test_resolve_with_parent(self):
        with make_dir() as parent:
            with make_file(parent=parent) as path:
                name = os.path.basename(path)
                self.assertEqual(path, resolve_path(name, parent))
    
    def test_resolve_missing(self):
        with self.assertRaises(IOError):
            resolve_path('foo')
    
    def test_check_readable_file(self):
        with make_file('r') as path:
            check_readable_file(path)
        with self.assertRaises(IOError):
            with make_file('w') as path:
                check_readable_file(path)
        with self.assertRaises(IOError):
            check_readable_file('foo')
        with make_dir() as path:
            with self.assertRaises(IOError):
                check_readable_file(path)
    
    def test_check_writeable_file(self):
        with make_file('w') as path:
            check_writeable_file(path)
        with self.assertRaises(IOError):
            with make_file('r') as path:
                check_writeable_file(path)
        with make_dir() as parent:
            check_writeable_file(os.path.join(parent, 'foo'))
            subdir_path = os.path.join(parent, 'bar', 'foo')
            check_writeable_file(subdir_path)
            self.assertTrue(os.path.exists(os.path.dirname(subdir_path)))
        with make_dir('r') as parent:
            with self.assertRaises(IOError):
                check_writeable_file(os.path.join(parent, 'foo'))
    
    def test_check_path_std(self):
        check_path(STDOUT, 'f', 'r')
        check_path(STDOUT, 'f', 'w')
        check_path(STDERR, 'f', 'w')
        with self.assertRaises(IOError):
            check_path(STDOUT, 'd', 'r')
    
    def test_find(self):
        with make_dir() as level1:
            with make_dir(prefix='foo', parent=level1) as level2:
                with make_empty_files(3, prefix='bar', parent=level2) as paths:
                    x = find(level1, 'foo.*', 'd')
                    self.assertEqual(1, len(x))
                    self.assertEqual(level2, x[0])
                    y = find(level1, 'bar.*', 'f')
                    self.assertEqual(3, len(y))
                    self.assertListEqual(sorted(paths), sorted(y))
    
    def test_get_executable_path(self):
        with make_file(suffix=".exe") as exe:
            exe_path = get_executable_path(exe)
            self.assertIsNotNone(exe_path)
            self.assertEqual(exe_path, get_executable_path(os.path.basename(exe)))
        # TODO: how to test this fully, since we can't be sure of what
        # executables will be available on the installed system?
