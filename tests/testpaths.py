from unittest import TestCase
import os
from xphyle.paths import *
from . import *

class TempDirTests(TestCase):
    def test_context_manager(self):
        with TempDir() as temp:
            with open(temp.get_file('foo'), 'wt') as o:
                o.write('foo')
        self.assertFalse(os.path.exists(temp.root))
    
    def test_files(self):
            temp = TempDir()
            f = temp.get_file('baz', subdir='foo/bar', make_dirs=True)
            self.assertEqual(f, os.path.join(temp.root, 'foo', 'bar', 'baz'))
            self.assertTrue(os.path.exists(temp.root))
            self.assertTrue(os.path.exists(os.path.join(temp.root, 'foo')))
            self.assertTrue(os.path.exists(os.path.join(temp.root, 'foo', 'bar')))
            temp.close()
            self.assertFalse(os.path.exists(temp.root))
    
    def test_dirs(self):
            temp = TempDir()
            d = temp.get_directory('foo/bar', make_dirs=True)
            self.assertEqual(d, os.path.join(temp.root, 'foo', 'bar'))
            self.assertTrue(os.path.exists(os.path.join(temp.root, 'foo', 'bar')))
            temp.close()
            self.assertFalse(os.path.exists(temp.root))
    
    def test_mode(self):
        with TempDir('r') as temp:
            # Raises error because the tempdir is read-only
            with self.assertRaises(PermissionError):
                f = temp.get_file('bar', 'foo', True)
                
                
            check_access(f, 'r')
            with self.assertRaises(IOError):
                check_access(f, 'w')
            with self.assertRaises(PermissionError):
                f.write('foo')
        
    
    
    def make_fifos(self, *names, subdir=None, mode='rwx', **kwargs):
        fifo_paths = [
            self.get_path(name, subdir, make_dirs=True, mode=mode)
            for name in names]
        for path in fifo_paths:
            os.mkfifo(path, **kwargs)
        return fifo_paths
    
class PathTests(TestCase):
    def setUp(self):
        self.root = TestTempDir()
    
    def tearDown(self):
        self.root.close()

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
        path = self.root.make_file(mode='rwx')
        check_access(path, 'r')
        check_access(path, 'w')
        check_access(path, 'x')
    
    def set_access(self):
        path = self.root.make_file()
        with self.assertRaises(ValueError):
            set_access(path, 'z')
        set_access(path, 'r')
        with self.assertRaises(IOError):
            check_access(path, 'w')
    
    def test_no_access(self):
        with self.assertRaises(IOError):
            path = self.root.make_file(mode='r')
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
    
    def test_split_path(self):
        parent = self.root.get_temp_dir()
        self.assertTupleEqual(
            split_path(os.path.join(parent, 'foo'), keep_seps=False),
            (parent, 'foo'))
        self.assertTupleEqual(
            split_path(os.path.join(parent, 'foo.tar.gz'), keep_seps=False),
            (parent, 'foo', 'tar', 'gz'))
        self.assertTupleEqual(
            split_path(os.path.join(parent, 'foo.tar.gz'), keep_seps=True),
            (parent, 'foo', '.tar', '.gz'))
    
    def test_filename(self):
        self.assertEqual(filename('/path/to/foo.tar.gz'), 'foo')
    
    def test_resolve_std(self):
        self.assertEqual(STDOUT, resolve_path(STDOUT))
        self.assertEqual(STDERR, resolve_path(STDERR))
    
    def test_resolve_file(self):
        path = self.root.make_file()
        self.assertEqual(abspath(path), resolve_path(path))
    
    def test_resolve_with_parent(self):
        path = self.root.make_file(subdir='foo')
        name = os.path.basename(path)
        parent = os.path.dirname(path)
        self.assertEqual(path, resolve_path(name, parent))
    
    def test_resolve_missing(self):
        with self.assertRaises(IOError):
            resolve_path('foo')
    
    def test_check_readable_file(self):
        path = self.root.make_file(mode='r')
        check_readable_file(path)
        with self.assertRaises(IOError):
            path = self.root.make_file(mode='w')
            check_readable_file(path)
        with self.assertRaises(IOError):
            check_readable_file('foo')
        with self.assertRaises(IOError):
            path = self.root.get_temp_dir()
            check_readable_file(path)
    
    def test_check_writeable_file(self):
        path = self.root.make_file(mode='w')
        check_writeable_file(path)
        with self.assertRaises(IOError):
            path = self.root.make_file(mode='r')
            check_writeable_file(path)
        parent = self.root.get_temp_dir()
        check_writeable_file(os.path.join(parent, 'foo'))
        subdir_path = os.path.join(parent, 'bar', 'foo')
        check_writeable_file(subdir_path)
        self.assertTrue(os.path.exists(os.path.dirname(subdir_path)))
        with self.assertRaises(IOError):
            parent = self.root.get_temp_dir(mode='r')
            check_writeable_file(os.path.join(parent, 'foo'))
    
    def test_check_path_std(self):
        check_path(STDOUT, 'f', 'r')
        check_path(STDOUT, 'f', 'w')
        check_path(STDERR, 'f', 'w')
        with self.assertRaises(IOError):
            check_path(STDOUT, 'd', 'r')
    
    def test_safe_checks(self):
        path = self.root.make_file(mode='r')
        self.assertTrue(safe_check_path(path, 'f', 'r'))
        self.assertFalse(safe_check_path(path, 'd', 'r'))
        self.assertFalse(safe_check_path(path, 'f', 'w'))
        self.assertTrue(safe_check_readable_file(path))
        self.assertFalse(safe_check_writeable_file(path))
    
    def test_find(self):
        level1 = self.root.get_temp_dir()
        subdir1 = os.path.basename(level1)
        level2 = self.root.get_temp_dir(prefix='foo', subdir=subdir1)
        subdir2 = os.path.basename(level2)
        paths = self.root.make_empty_files(
            3, prefix='bar', subdir=os.path.join(subdir1, subdir2))
        x = find(level1, 'foo.*', 'd')
        self.assertEqual(1, len(x))
        self.assertEqual(level2, x[0])
        y = find(level1, 'bar.*', 'f')
        self.assertEqual(3, len(y))
        self.assertListEqual(sorted(paths), sorted(y))
    
    def test_get_executable_path(self):
        exe = self.root.make_file(suffix=".exe")
        exe_path = get_executable_path(exe)
        self.assertIsNotNone(exe_path)
        self.assertEqual(exe_path, get_executable_path(os.path.basename(exe)))
        # TODO: how to test this fully, since we can't be sure of what
        # executables will be available on the installed system?
