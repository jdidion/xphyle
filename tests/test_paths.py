from unittest import TestCase
import os
import subprocess
from xphyle.paths import *
from . import *

class TempDirTests(TestCase):
    def test_descriptor(self):
        with self.assertRaises(ValueError):
            TempPathDescriptor(path_type='d', contents='foo')
        with self.assertRaises(IOError):
            TempPathDescriptor().absolute_path
        with TempDir(permissions='rwx') as temp:
            f = temp.make_file(name='foo', permissions=None)
            os.remove(f)
            self.assertIsNone(temp[f].set_permissions('r'))
        with TempDir(permissions='rwx') as temp:
            f = temp.make_file(name='foo', permissions=None)
            self.assertTrue('foo' in temp)
            self.assertTrue(temp[f].exists)
            self.assertEqual('foo', temp[f].relative_path)
            self.assertEqual(
                os.path.join(temp.absolute_path, 'foo'), temp[f].absolute_path)
            self.assertEqual(PermissionSet('rwx'), temp[f].permissions)
            self.assertEquals(PermissionSet('r'), temp[f].set_permissions('r'))
            with self.assertRaises(PermissionError):
                open(f, 'w')
        with TempDir(permissions='rwx') as temp:
            desc = TempPathDescriptor(
                name='foo', path_type='f', parent=temp)
            self.assertEquals('foo', desc.relative_path)
            self.assertEquals(
                os.path.join(temp.absolute_path, 'foo'), desc.absolute_path)
    
    def test_context_manager(self):
        with TempDir() as temp:
            with open(temp.make_file(name='foo'), 'wt') as o:
                o.write('foo')
        self.assertFalse(os.path.exists(temp.absolute_path))
    
    def test_dir(self):
        temp = TempDir()
        foo = temp.make_directory(name='foo')
        self.assertEqual(foo, os.path.join(temp.absolute_path, 'foo'))
        bar = temp.make_directory(name='bar', parent=foo)
        self.assertEqual(bar, os.path.join(temp.absolute_path, 'foo', 'bar'))
        self.assertTrue(os.path.exists(
            os.path.join(temp.absolute_path, 'foo', 'bar')))
        temp.close()
        self.assertFalse(os.path.exists(temp.absolute_path))
        # make sure trying to close again doesn't raise error
        temp.close()
    
    def test_tree(self):
        temp = TempDir()
        foo = temp.make_directory(name='foo')
        bar = temp.make_directory(name='bar', parent=foo)
        f = temp.make_file(name='baz', parent=bar)
        self.assertEqual(
            f, os.path.join(temp.absolute_path, 'foo', 'bar', 'baz'))
        temp.close()
        self.assertFalse(os.path.exists(f))
    
    def test_mode(self):
        with self.assertRaises(IOError):
            with TempDir(permissions=None) as temp:
                temp.mode
        with TempDir('r') as temp:
            # Raises error because the tempdir is read-only
            with self.assertRaises(PermissionError):
                temp.make_file(name='bar')
        # Should be able to create the tempdir with existing read-only files
        with TempDir(
                'r', [TempPathDescriptor(name='foo', contents='foo')]) as d:
            self.assertTrue(os.path.exists(d.absolute_path))
            self.assertTrue(os.path.exists(
                os.path.join(d.absolute_path, 'foo')))
            with open(os.path.join(d.absolute_path, 'foo'), 'rt') as i:
                self.assertEqual('foo', i.read())
    
    def test_fifo(self):
        with TempDir() as temp:
            with self.assertRaises(Exception):
                path = temp.make_fifo(contents='foo')
            path = temp.make_fifo()
            p = subprocess.Popen('echo foo > {}'.format(path), shell=True)
            with open(path, 'rt') as i:
                self.assertEqual(i.read(), 'foo\n')
            p.communicate()

class PathTests(TestCase):
    def setUp(self):
        self.root = TempDir()
    
    def tearDown(self):
        self.root.close()
        EXECUTABLE_CACHE.cache.clear()
    
    def test_get_set_permissions(self):
        path = self.root.make_file(permissions='rw')
        self.assertEquals(PermissionSet('rw'), get_permissions(path))
        set_permissions(path, 'wx')
        self.assertEquals(PermissionSet('wx'), get_permissions(path))
    
    def test_check_access_std(self):
        check_access(STDOUT, 'r')
        check_access(STDOUT, 'w')
        check_access(STDERR, 'w')
        with self.assertRaises(IOError):
            check_access(STDOUT, 'x')
        with self.assertRaises(IOError):
            check_access(STDERR, 'r')
    
    def test_check_access_file(self):
        path = self.root.make_file(permissions='rwx')
        check_access(path, 'r')
        check_access(path, 'w')
        check_access(path, 'x')
    
    def test_set_permissions(self):
        path = self.root.make_file()
        with self.assertRaises(ValueError):
            set_permissions(path, 'z')
        set_permissions(path, 'r')
        with self.assertRaises(IOError):
            check_access(path, 'w')
    
    def test_no_permissions(self):
        with self.assertRaises(IOError):
            path = self.root.make_file(permissions='r')
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
    
    def test_get_root(self):
        # Need to do a different test for posix vs windows
        if os.sep == '/':
            self.assertEquals('/', get_root())
            self.assertEquals('/', get_root('/foo/bar/baz'))
        else:
            script_drive = os.path.splitdrive(sys.executable)[0]
            self.assertEquals(script_drive, get_root())
            self.assertEquals('C:\\', get_root('C:\\foo\\bar\\baz'))
    
    def test_split_path(self):
        parent = self.root.make_directory()
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
        self.root.make_directory(name='foo')
        path = self.root.make_file(parent='foo')
        name = os.path.basename(path)
        parent = os.path.dirname(path)
        self.assertEqual(path, resolve_path(name, parent))
    
    def test_resolve_missing(self):
        with self.assertRaises(IOError):
            resolve_path('foo')
    
    def test_check_readable_file(self):
        readable = self.root.make_file(permissions='r')
        non_readable = self.root.make_file(permissions='w')
        directory = self.root.make_directory()
        check_readable_file(readable)
        with self.assertRaises(IOError):
            check_readable_file(non_readable)
        with self.assertRaises(IOError):
            check_readable_file('foo')
        with self.assertRaises(IOError):
            check_readable_file(directory)
        self.assertTrue(safe_check_readable_file(readable))
        self.assertIsNone(safe_check_readable_file(non_readable))
    
    def test_check_writable_file(self):
        writable = self.root.make_file(permissions='w')
        non_writable = self.root.make_file(permissions='r')
        check_writable_file(writable)
        with self.assertRaises(IOError):
            check_writable_file(non_writable)
        parent = self.root.make_directory()
        check_writable_file(os.path.join(parent, 'foo'))
        subdir_path = os.path.join(parent, 'bar', 'foo')
        check_writable_file(subdir_path)
        self.assertTrue(os.path.exists(os.path.dirname(subdir_path)))
        with self.assertRaises(IOError):
            parent = self.root.make_directory(permissions='r')
            check_writable_file(os.path.join(parent, 'foo'))
        self.assertTrue(safe_check_writable_file(writable))
        self.assertIsNone(safe_check_writable_file(non_writable))
    
    def test_check_path_std(self):
        check_path(STDOUT, 'f', 'r')
        check_path(STDOUT, 'f', 'w')
        check_path(STDERR, 'f', 'w')
        with self.assertRaises(IOError):
            check_path(STDOUT, 'd', 'r')
    
    def test_safe_checks(self):
        path = self.root.make_file(permissions='r')
        self.assertTrue(safe_check_path(path, 'f', 'r'))
        self.assertFalse(safe_check_path(path, 'd', 'r'))
        self.assertFalse(safe_check_path(path, 'f', 'w'))
    
    def test_find(self):
        level1 = self.root.make_directory()
        level2 = self.root.make_directory(prefix='foo', parent=level1)
        paths = self.root.make_empty_files(3, prefix='bar', parent=level2)
        
        # recursive
        x = find(level1, 'foo.*', 'd', recursive=True)
        self.assertEqual(1, len(x))
        self.assertEqual(level2, x[0])
        y = find(level1, 'bar.*', 'f', recursive=True)
        self.assertEqual(3, len(y))
        self.assertListEqual(sorted(paths), sorted(y))
        
        # non-recursive
        x = find(level1, 'foo.*', 'd', recursive=False)
        self.assertEqual(1, len(x))
        self.assertEqual(level2, x[0])
        y = find(level1, 'bar.*', 'f', recursive=False)
        self.assertEqual(0, len(y))
        
        # absolute match
        x = find(level1, os.path.join(level1, 'foo.*', 'bar.*'), 'f', recursive=True)
        self.assertEqual(3, len(x))
        self.assertListEqual(sorted(paths), sorted(x))
        
        # fifo
        path = self.root.make_fifo(prefix='baz', parent=level1)
        x = find(level1, 'baz.*', '|')
        self.assertEquals(1, len(x))
        self.assertEquals(path, x[0])
    
    def test_find_with_matches(self):
        level1 = self.root.make_directory()
        level2 = self.root.make_directory(prefix='foo', parent=level1)
        path = self.root.make_path(name='bar123', parent=level2)
        result = find(level1, 'bar(.*)', 'f', recursive=True, return_matches=True)
        self.assertEqual(1, len(result))
        self.assertEqual(path, result[0][0])
        self.assertEqual('123', result[0][1].group(1))
    
    def test_get_executable_path(self):
        exe = self.root.make_file(suffix=".exe")
        exe_path = EXECUTABLE_CACHE.get_path(exe)
        self.assertIsNotNone(exe_path)
        self.assertEqual(exe_path, EXECUTABLE_CACHE.get_path(os.path.basename(exe)))
        EXECUTABLE_CACHE.cache.clear()
        EXECUTABLE_CACHE.add_search_path(os.path.dirname(exe))
        self.assertEqual(exe_path, EXECUTABLE_CACHE.get_path(os.path.basename(exe)))
        # TODO: how to test this fully, since we can't be sure of what
        # executables will be available on the installed system?
    
    def test_resolve_exe(self):
        exe = self.root.make_file(suffix=".exe")
        exe_name = os.path.basename(exe)
        path = EXECUTABLE_CACHE.resolve_exe([exe_name])
        self.assertIsNone(path)
        EXECUTABLE_CACHE.cache.clear()
        EXECUTABLE_CACHE.add_search_path(os.path.dirname(exe))
        path = EXECUTABLE_CACHE.resolve_exe([exe_name])
        self.assertIsNotNone(path)
        self.assertEquals(exe, path[0])
    
    def test_pathvar(self):
        pv = PathVar('id', pattern='[A-Z0-9_]+', default='ABC123')
        self.assertEquals('ABC123', pv(None))
        
        pv = PathVar('id', pattern='[A-Z0-9_]+', optional=True)
        self.assertEquals('', pv(None))
        
        pv = PathVar('id', pattern='[A-Z0-9_]+')
        with self.assertRaises(ValueError):
            pv(None)
    
    def test_filespec(self):
        null = FileSpec()
        self.assertEquals('{file}', null.template)
        self.assertTrue('file' in null.path_vars)
        
        path = self.root.make_file(name='ABC123.txt')
        base = os.path.basename(path)
        
        spec = FileSpec(
            PathVar('id', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
            PathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
            template='{id}.{ext}')
        
        # get a single file
        pathinst = spec(id='ABC123', ext='txt')
        self.assertEquals(
            path_inst(base, dict(id='ABC123', ext='txt')),
            pathinst)
        self.assertEquals('ABC123', pathinst['id'])
        self.assertEquals('txt', pathinst['ext'])
        
        with self.assertRaises(ValueError):
            spec(id='abc123', ext='txt')
        
        with self.assertRaises(ValueError):
            spec(id='ABC123', ext='foo')
        
        with self.assertRaises(ValueError):
            spec(id='XYZ999', ext='txt')
        
        pathinst = spec.parse(path, fullpath=True)
        self.assertEquals(
            path_inst(os.path.basename(path), dict(id='ABC123', ext='txt')),
            pathinst)
        
        path2 = self.root.make_file(name='abc123.txt')
        with self.assertRaises(ValueError):
            spec.parse(path2)
        
        all_paths = spec.find(self.root.absolute_path)
        self.assertEquals(1, len(all_paths))
        self.assertEquals(
            path_inst(path, dict(id='ABC123', ext='txt')),
            all_paths[0])
    
    def test_dirspec(self):
        null = DirSpec()
        self.assertEquals('{dir}', null.template)
        self.assertTrue('dir' in null.path_vars)
                
        level1 = self.root.make_directory(name='ABC123')
        level2 = self.root.make_directory(parent=level1, name='AAA')
        base = os.path.dirname(level1)
        
        spec = DirSpec(
            PathVar('root'),
            PathVar('subdir', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
            PathVar('leaf', pattern='[^_]+', valid=('AAA', 'BBB')),
            template=os.path.join('{root}', '{subdir}', '{leaf}'))
        
        # get a single dir
        pathinst = spec(root=base, subdir='ABC123', leaf='AAA')
        self.assertEquals(
            path_inst(level2, dict(root=base, subdir='ABC123', leaf='AAA')),
            pathinst)
        self.assertEquals(base, pathinst['root'])
        self.assertEquals('ABC123', pathinst['subdir'])
        self.assertEquals('AAA', pathinst['leaf'])
        
        with self.assertRaises(ValueError):
            spec(root=base, subdir='abc123', leaf='AAA')
        
        with self.assertRaises(ValueError):
            spec(root=base, subdir='ABC123', leaf='CCC')
        
        with self.assertRaises(ValueError):
            spec(root=base, subdir='XYZ999', leaf='AAA')
        
        pathinst = spec.parse(level2)
        self.assertEquals(
            path_inst(level2, dict(root=base, subdir='ABC123', leaf='AAA')),
            pathinst)
        
        path = self.root.make_file(parent=level2)
        pathinst = spec.parse(path, fullpath=True)
        self.assertEquals(
            path_inst(level2, dict(root=base, subdir='ABC123', leaf='AAA')),
            pathinst)
        
        path2 = self.root.make_directory(name='abc123')
        with self.assertRaises(ValueError):
            spec.parse(path2)
    
        all_paths = spec.find(base, recursive=True)
        self.assertEquals(1, len(all_paths))
        self.assertEquals(
            path_inst(level2, dict(root=base, subdir='ABC123', leaf='AAA')),
            all_paths[0])
    
    def test_pathspec(self):
        level1 = self.root.make_directory(name='ABC123')
        level2 = self.root.make_directory(parent=level1, name='AAA')
        path = self.root.make_file(parent=level2, name='FFF555.txt')
        base = os.path.dirname(level1)
        
        spec = PathSpec(
            DirSpec(
                PathVar('root'),
                PathVar('subdir', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
                PathVar('leaf', pattern='[^_]+', valid=('AAA', 'BBB')),
                template=os.path.join('{root}', '{subdir}', '{leaf}')),
            FileSpec(
                PathVar('id', pattern='[A-Z0-9_]+', invalid=('ABC123',)),
                PathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
                template='{id}.{ext}'))
        
        path_var_values = dict(root=base, subdir='ABC123', leaf='AAA',
                               id='FFF555', ext='txt')
        pathinst = spec(**path_var_values)
        self.assertEquals(path_inst(path, path_var_values), pathinst)
        self.assertEquals(base, pathinst['root'])
        self.assertEquals('ABC123', pathinst['subdir'])
        self.assertEquals('AAA', pathinst['leaf'])
        self.assertEquals('FFF555', pathinst['id'])
        self.assertEquals('txt', pathinst['ext'])
        
        fail1 = dict(path_var_values)
        # should fail because expecting all caps
        fail1['id'] = 'abc123'
        with self.assertRaises(ValueError):
            spec(**fail1)
        
        fail2 = dict(path_var_values)
        # should fail because foo is not in the valid list
        fail2['ext'] = 'foo'
        with self.assertRaises(ValueError):
            spec(**fail2)
        
        fail3 = dict(path_var_values)
        # should fail because ABC123 is in the invalid list
        fail3['id'] = 'ABC123'
        with self.assertRaises(ValueError):
            spec(**fail3)
        
        pathinst = spec.parse(path)
        self.assertEquals(path_inst(path, path_var_values), pathinst)
        
        path2 = self.root.make_file(parent=level2, name='fff555.txt')
        with self.assertRaises(ValueError):
            spec.parse(path2)
        
        all_paths = spec.find(base, recursive=True)
        self.assertEquals(1, len(all_paths))
        self.assertEquals(path_inst(path, path_var_values), all_paths[0])
        
        # make sure it works with plain paths
        spec = PathSpec(
            level2,
            FileSpec(
                PathVar('id', pattern='[A-Z0-9_]+', invalid=('ABC123',)),
                PathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
                template='{id}.{ext}'))
        self.assertEquals(
            path_inst(path, dict(id='FFF555', ext='txt')),
            spec.parse(path))
        with self.assertRaises(ValueError):
            bad_path = os.path.join(
                get_root(), 'foo', 'bar', os.path.basename(path))
            spec.parse(bad_path)
        
        spec = PathSpec(
            DirSpec(
                PathVar('root'),
                PathVar('subdir', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
                PathVar('leaf', pattern='[^_]+', valid=('AAA', 'BBB')),
                template=os.path.join('{root}', '{subdir}', '{leaf}')),
            os.path.basename(path))
        self.assertEquals(
            path_inst(path, dict(root=base, subdir='ABC123', leaf='AAA')),
            spec.parse(path))
        
        spec = PathSpec(level2, os.path.basename(path))
        all_paths = spec.find()
        self.assertEquals(1, len(all_paths))
        self.assertEquals(path_inst(path), all_paths[0])
        
    def test_default_search(self):
        spec = FileSpec(
            PathVar('id', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
            PathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
            template='{id}.{ext}')
        with self.assertRaises(ValueError):
            spec.find()
        
        level1 = self.root.make_directory(name='ABC123')
        level2 = self.root.make_directory(parent=level1, name='AAA')
        base = os.path.dirname(level1)
        
        spec = DirSpec(
            PathVar('subdir', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
            PathVar('leaf', pattern='[^_]+', valid=('AAA', 'BBB')),
            template=os.path.join(base, '{subdir}', '{leaf}'))
        
        all_paths = spec.find(recursive=True)
        self.assertEquals(1, len(all_paths))
        self.assertEquals(
            path_inst(level2, dict(subdir='ABC123', leaf='AAA')),
            all_paths[0])
    
    def test_pathspec_default_search(self):
        path = self.root.make_file(name='FFF555.txt')
        base = os.path.dirname(path)
        
        spec = PathSpec(
            DirSpec(template=base),
            FileSpec(
                PathVar('id', pattern='[A-Z0-9_]+', invalid=('ABC123',)),
                PathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
                template='{id}.{ext}'))
        
        all_paths = spec.find()
        self.assertEquals(1, len(all_paths))
        self.assertEquals(
            path_inst(path, dict(id='FFF555', ext='txt')),
            all_paths[0])
