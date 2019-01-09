from unittest import TestCase
import subprocess
from xphyle.paths import *


class TempDirTests(TestCase):
    def test_descriptor(self):
        with self.assertRaises(ValueError):
            TempPathDescriptor(path_type='d', contents='foo')
        with self.assertRaises(IOError):
            _ = TempPathDescriptor().absolute_path
        with TempDir(permissions='rwx') as temp:
            f = temp.make_file(name='foo', permissions=None)
            f.unlink()
            assert temp[f].set_permissions('r') is None
        with TempDir(permissions='rwx') as temp:
            f = temp.make_file(name='foo', permissions=None)
            assert Path('foo') in temp
            assert temp[f].exists
            assert Path('foo') == temp[f].relative_path
            assert temp.absolute_path / 'foo' == temp[f].absolute_path
            assert PermissionSet('rwx') == temp[f].permissions
            assert PermissionSet('r') == temp[f].set_permissions('r')
            with self.assertRaises(PermissionError):
                open(f, 'w')
        with TempDir(permissions='rwx') as temp:
            desc = TempPathDescriptor(
                name='foo', path_type='f', parent=temp)
            assert Path('foo') == desc.relative_path
            assert temp.absolute_path / 'foo' == desc.absolute_path

    def test_context_manager(self):
        with TempDir() as temp:
            with open(temp.make_file(name='foo'), 'wt') as o:
                o.write('foo')
        assert not temp.absolute_path.exists()

    def test_dir(self):
        temp = TempDir()
        foo = temp.make_directory(name='foo')
        assert foo == temp.absolute_path / 'foo'
        bar = temp.make_directory(name='bar', parent=foo)
        assert bar == temp.absolute_path / 'foo' / 'bar'
        assert (temp.absolute_path / 'foo' / 'bar').exists()
        temp.close()
        assert not temp.absolute_path.exists()
        # make sure trying to close again doesn't raise error
        temp.close()

    def test_tree(self):
        temp = TempDir()
        foo = temp.make_directory(name='foo')
        bar = temp.make_directory(name='bar', parent=foo)
        f = temp.make_file(name='baz', parent=bar)
        assert f == temp.absolute_path / 'foo' / 'bar' / 'baz'
        temp.close()
        assert not f.exists()

    def test_mode(self):
        # with self.assertRaises(IOError):
        #    with TempDir(permissions=None) as temp:
        #        _ = temp.mode
        with TempDir('r') as temp:
            # Raises error because the tempdir is read-only
            with self.assertRaises(PermissionError):
                temp.make_file(name='bar')
        # Should be able to create the tempdir with existing read-only files
        with TempDir(
                'r', [TempPathDescriptor(name='foo', contents='foo')]) as d:
            assert d.absolute_path.exists()
            assert (d.absolute_path / 'foo').exists()
            with open(d.absolute_path / 'foo', 'rt') as i:
                assert 'foo' == i.read()

    def test_fifo(self):
        with TempDir() as temp:
            with self.assertRaises(Exception):
                _ = temp.make_fifo(contents='foo')
            path = temp.make_fifo()
            p = subprocess.Popen('echo foo > {}'.format(path), shell=True)
            with open(path, 'rt') as i:
                assert i.read() == 'foo\n'
            p.communicate()


class PathTests(TestCase):
    def setUp(self):
        self.root = TempDir()

    def tearDown(self):
        self.root.close()
        EXECUTABLE_CACHE.cache.clear()

    def test_get_set_permissions(self):
        path = self.root.make_file(permissions='rw')
        assert PermissionSet('rw') == get_permissions(path)
        set_permissions(path, 'wx')
        assert PermissionSet('wx') == get_permissions(path)

    def test_check_access_std(self):
        check_access(STDIN_OR_STDOUT, 'r')
        check_access(STDIN_OR_STDOUT, 'w')
        check_access(STDIN, 'r')
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
        assert abspath(STDOUT) == STDOUT
        assert abspath(STDERR) == STDERR

    def test_abspath_home(self):
        home = os.path.expanduser("~")
        assert abspath(Path('~/foo')) == Path(home) / 'foo'

    def test_abspath_rel(self):
        cwd = os.getcwd()
        assert abspath(Path('foo')) == Path(cwd) / 'foo'

    def test_get_root(self):
        # Need to do a different test for posix vs windows
        if os.sep == '/':
            assert '/' == get_root()
            assert '/' == get_root(PosixPath('/foo/bar/baz'))
        else:
            script_drive = os.path.splitdrive(sys.executable)[0]
            assert script_drive == get_root()
            assert 'C:\\' == get_root(WindowsPath('C:\\foo\\bar\\baz'))

    def test_split_path(self):
        parent = self.root.make_directory()
        assert split_path(parent / 'foo', keep_seps=False) == (parent, 'foo')
        assert split_path(parent / 'foo.tar.gz', keep_seps=False) == \
            (parent, 'foo', 'tar', 'gz')
        assert split_path(parent / 'foo.tar.gz', keep_seps=True) == \
            (parent, 'foo', '.tar', '.gz')

    def test_filename(self):
        assert filename(Path('/path/to/foo.tar.gz')) == 'foo'

    def test_resolve_std(self):
        assert STDOUT == resolve_path(STDOUT)
        assert STDERR == resolve_path(STDERR)

    def test_resolve_file(self):
        path = self.root.make_file()
        assert abspath(path) == resolve_path(path)

    def test_resolve_with_parent(self):
        self.root.make_directory(name='foo')
        path = self.root.make_file(parent=self.root[Path('foo')])
        name = path.name
        parent = path.parent
        assert path == resolve_path(Path(name), parent)

    def test_resolve_missing(self):
        with self.assertRaises(IOError):
            resolve_path(Path('foo'))

    def test_check_readable_file(self):
        readable = self.root.make_file(permissions='r')
        non_readable = self.root.make_file(permissions='w')
        directory = self.root.make_directory()
        check_readable_file(readable)
        with self.assertRaises(IOError):
            check_readable_file(non_readable)
        with self.assertRaises(IOError):
            check_readable_file(Path('foo'))
        with self.assertRaises(IOError):
            check_readable_file(directory)
        assert safe_check_readable_file(readable)
        assert safe_check_readable_file(non_readable) is None

    def test_check_writable_file(self):
        writable = self.root.make_file(permissions='w')
        non_writable = self.root.make_file(permissions='r')
        check_writable_file(writable)
        with self.assertRaises(IOError):
            check_writable_file(non_writable)
        parent = self.root.make_directory()
        check_writable_file(parent / 'foo')
        subdir_path = parent / 'bar' / 'foo'
        check_writable_file(subdir_path)
        assert subdir_path.parent.exists()
        with self.assertRaises(IOError):
            parent = self.root.make_directory(permissions='r')
            check_writable_file(parent / 'foo')
        assert safe_check_writable_file(writable)
        assert safe_check_writable_file(non_writable) is None

    def test_check_path_std(self):
        check_path(STDIN_OR_STDOUT, 'f', 'r')
        check_path(STDIN_OR_STDOUT, 'f', 'w')
        check_path(STDIN, 'f', 'r')
        check_path(STDOUT, 'f', 'w')
        check_path(STDERR, 'f', 'w')
        with self.assertRaises(IOError):
            check_path(STDIN, 'f', 'w')
        with self.assertRaises(IOError):
            check_path(STDOUT, 'f', 'r')
        with self.assertRaises(IOError):
            check_path(STDERR, 'f', 'r')
        with self.assertRaises(IOError):
            check_path(STDOUT, 'd', 'r')

    def test_safe_checks(self):
        path = self.root.make_file(permissions='r')
        assert safe_check_path(path, 'f', 'r')
        assert not safe_check_path(path, 'd', 'r')
        assert not safe_check_path(path, 'f', 'w')

    def test_find(self):
        level1 = self.root.make_directory()
        level2 = self.root.make_directory(prefix='foo', parent=level1)
        paths = self.root.make_empty_files(3, prefix='bar', parent=level2)

        # recursive
        x = find(level1, 'foo.*', 'd', recursive=True)
        assert 1 == len(x)
        assert level2 == x[0]
        y = find(level1, 'bar.*', 'f', recursive=True)
        assert 3 == len(y)
        assert sorted(paths) == sorted(y)

        # non-recursive
        x = find(level1, 'foo.*', 'd', recursive=False)
        assert 1 == len(x)
        assert level2 == x[0]
        y = find(level1, 'bar.*', 'f', recursive=False)
        assert 0 == len(y)

        # absolute match
        x = find(
            level1, os.path.join(str(level1), 'foo.*', 'bar.*'), 'f',
            recursive=True)
        assert 3 == len(x)
        assert sorted(paths) == sorted(x)

        # fifo
        path = self.root.make_fifo(prefix='baz', parent=level1)
        x = find(level1, 'baz.*', '|')
        assert 1 == len(x)
        assert path == x[0]

    def test_find_with_matches(self):
        level1 = self.root.make_directory()
        level2 = self.root.make_directory(prefix='foo', parent=level1)
        path = self.root.make_path(name='bar123', parent=level2)
        result = cast(Sequence[Tuple[PurePath, Match]], find(
            level1, 'bar(.*)', 'f', recursive=True, return_matches=True))
        assert 1 == len(result)
        assert path == result[0][0]
        assert '123' == result[0][1].group(1)

    def test_get_executable_path(self):
        exe = self.root.make_file(suffix=".exe")
        exe_path = EXECUTABLE_CACHE.get_path(exe)
        assert exe_path is not None
        assert exe_path == EXECUTABLE_CACHE.get_path(exe.name)
        EXECUTABLE_CACHE.cache.clear()
        EXECUTABLE_CACHE.add_search_path(exe.parent)
        assert exe_path == EXECUTABLE_CACHE.get_path(exe.name)
        # TODO: how to test this fully, since we can't be sure of what
        # executables will be available on the installed system?

    def test_resolve_exe(self):
        exe = self.root.make_file(suffix=".exe")
        exe_name = exe.name
        path = EXECUTABLE_CACHE.resolve_exe([exe_name])
        assert path is None
        EXECUTABLE_CACHE.cache.clear()
        EXECUTABLE_CACHE.add_search_path(exe.parent)
        path = EXECUTABLE_CACHE.resolve_exe([exe_name])
        assert path is not None
        assert exe == path[0]

    def test_pathvar(self):
        pv = StrPathVar('id', pattern='[A-Z0-9_]+', default='ABC123')
        assert 'ABC123' == pv(None)

        pv = StrPathVar('id', pattern='[A-Z0-9_]+', optional=True)
        assert '' == pv(None)

        pv = StrPathVar('id', pattern='[A-Z0-9_]+')
        with self.assertRaises(ValueError):
            pv(None)

    def test_filespec(self):
        null = FileSpec()
        assert '{file}' == null.template
        assert 'file' in null.path_vars

        path = self.root.make_file(name='ABC123.txt')
        base = path.name

        spec = FileSpec(
            StrPathVar('id', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
            StrPathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
            template='{id}.{ext}')

        # get a single file
        pathinst = spec(id='ABC123', ext='txt')
        assert path_inst(base, dict(id='ABC123', ext='txt')) == pathinst
        assert 'ABC123' == pathinst['id']
        assert 'txt' == pathinst['ext']

        with self.assertRaises(ValueError):
            spec(id='abc123', ext='txt')

        with self.assertRaises(ValueError):
            spec(id='ABC123', ext='foo')

        with self.assertRaises(ValueError):
            spec(id='XYZ999', ext='txt')

        pathinst = spec.parse(path, fullpath=True)
        assert path_inst(path.name, dict(id='ABC123', ext='txt')) == pathinst

        path2 = self.root.make_file(name='abc123.txt')
        with self.assertRaises(ValueError):
            spec.parse(path2)

        all_paths = spec.find(self.root.absolute_path)
        assert 1 == len(all_paths)
        assert path_inst(path, dict(id='ABC123', ext='txt')) == all_paths[0]

    def test_dirspec(self):
        null = DirSpec()
        assert '{dir}' == null.template
        assert 'dir' in null.path_vars

        level1 = self.root.make_directory(name='ABC123')
        level2 = self.root.make_directory(parent=level1, name='AAA')
        base = level1.parent

        spec = DirSpec(
            PathPathVar('root'),
            StrPathVar('subdir', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
            StrPathVar('leaf', pattern='[^_]+', valid=('AAA', 'BBB')),
            template=os.path.join('{root}', '{subdir}', '{leaf}'))

        # get a single dir
        pathinst = spec(root=base, subdir='ABC123', leaf='AAA')
        assert \
            path_inst(level2, dict(root=base, subdir='ABC123', leaf='AAA')) == \
            pathinst
        assert base == pathinst['root']
        assert 'ABC123' == pathinst['subdir']
        assert 'AAA' == pathinst['leaf']

        with self.assertRaises(ValueError):
            spec(root=base, subdir='abc123', leaf='AAA')

        with self.assertRaises(ValueError):
            spec(root=base, subdir='ABC123', leaf='CCC')

        with self.assertRaises(ValueError):
            spec(root=base, subdir='XYZ999', leaf='AAA')

        pathinst = spec.parse(level2)
        assert \
            path_inst(level2, dict(root=base, subdir='ABC123', leaf='AAA')) == \
            pathinst

        path = self.root.make_file(parent=level2)
        pathinst = spec.parse(path, fullpath=True)
        assert \
            path_inst(level2, dict(root=base, subdir='ABC123', leaf='AAA')) == \
            pathinst

        path2 = self.root.make_directory(name='abc123')
        with self.assertRaises(ValueError):
            spec.parse(path2)

        all_paths = spec.find(base, recursive=True)
        assert 1 == len(all_paths)
        assert \
            path_inst(level2, dict(root=base, subdir='ABC123', leaf='AAA')) == \
            all_paths[0]

    def test_pathspec(self):
        level1 = self.root.make_directory(name='ABC123')
        level2 = self.root.make_directory(parent=level1, name='AAA')
        path = self.root.make_file(parent=level2, name='FFF555.txt')
        base = level1.parent

        spec = PathSpec(
            DirSpec(
                PathPathVar('root'),
                StrPathVar('subdir', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
                StrPathVar('leaf', pattern='[^_]+', valid=('AAA', 'BBB')),
                template=os.path.join('{root}', '{subdir}', '{leaf}')),
            FileSpec(
                StrPathVar('id', pattern='[A-Z0-9_]+', invalid=('ABC123',)),
                StrPathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
                template='{id}.{ext}'))

        path_var_values = dict(root=base, subdir='ABC123', leaf='AAA',
                               id='FFF555', ext='txt')
        pathinst = spec(**path_var_values)
        assert path_inst(path, path_var_values) == pathinst
        assert base == pathinst['root']
        assert 'ABC123' == pathinst['subdir']
        assert 'AAA' == pathinst['leaf']
        assert 'FFF555' == pathinst['id']
        assert 'txt' == pathinst['ext']

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
        assert path_inst(path, path_var_values) == pathinst

        path2 = self.root.make_file(parent=level2, name='fff555.txt')
        with self.assertRaises(ValueError):
            spec.parse(path2)

        all_paths = spec.find(base, recursive=True)
        assert 1 == len(all_paths)
        assert path_inst(path, path_var_values) == all_paths[0]

        # make sure it works with plain paths
        spec = PathSpec(
            level2,
            FileSpec(
                StrPathVar('id', pattern='[A-Z0-9_]+', invalid=('ABC123',)),
                StrPathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
                template='{id}.{ext}'))
        assert path_inst(path, dict(id='FFF555', ext='txt')) == spec.parse(path)
        with self.assertRaises(ValueError):
            bad_path = Path(get_root()) / 'foo' / 'bar' / path.name
            spec.parse(bad_path)

        spec = PathSpec(
            DirSpec(
                PathPathVar('root'),
                StrPathVar('subdir', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
                StrPathVar('leaf', pattern='[^_]+', valid=('AAA', 'BBB')),
                template=os.path.join('{root}', '{subdir}', '{leaf}')),
            path.name)
        assert \
            path_inst(path, dict(root=base, subdir='ABC123', leaf='AAA')) == \
            spec.parse(path)

        spec = PathSpec(level2, path.name)
        all_paths = spec.find()
        assert 1 == len(all_paths)
        assert path_inst(path) == all_paths[0]

    def test_default_search(self):
        spec = FileSpec(
            StrPathVar('id', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
            StrPathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
            template='{id}.{ext}')
        with self.assertRaises(ValueError):
            spec.find()

        level1 = self.root.make_directory(name='ABC123')
        level2 = self.root.make_directory(parent=level1, name='AAA')
        base = level1.parent

        spec = DirSpec(
            StrPathVar('subdir', pattern='[A-Z0-9_]+', invalid=('XYZ999',)),
            StrPathVar('leaf', pattern='[^_]+', valid=('AAA', 'BBB')),
            template=os.path.join(base, '{subdir}', '{leaf}'))

        all_paths = spec.find(recursive=True)
        assert 1 == len(all_paths)
        assert \
            path_inst(level2, dict(subdir='ABC123', leaf='AAA')) == \
            all_paths[0]

    def test_pathspec_default_search(self):
        path = self.root.make_file(name='FFF555.txt')
        base = path.parent

        spec = PathSpec(
            DirSpec(template=str(base)),
            FileSpec(
                StrPathVar('id', pattern='[A-Z0-9_]+', invalid=('ABC123',)),
                StrPathVar('ext', pattern='[^\.]+', valid=('txt', 'exe')),
                template='{id}.{ext}'))

        all_paths = spec.find()
        assert 1 == len(all_paths)
        assert path_inst(path, dict(id='FFF555', ext='txt')) == all_paths[0]
