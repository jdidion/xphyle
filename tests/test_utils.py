from unittest import TestCase
from . import *
import gzip
import bz2
from xphyle.formats import THREADS
from xphyle.paths import TempDir, EXECUTABLE_CACHE
from xphyle.progress import ITERABLE_PROGRESS, PROCESS_PROGRESS
from xphyle.utils import *


class UtilsTests(TestCase):
    def setUp(self):
        self.root = TempDir()
        self.system_args = sys.argv

    def tearDown(self):
        self.root.close()
        ITERABLE_PROGRESS.enabled = False
        ITERABLE_PROGRESS.wrapper = None
        PROCESS_PROGRESS.enabled = False
        PROCESS_PROGRESS.wrapper = None
        THREADS.update(1)
        EXECUTABLE_CACHE.reset_search_path()
        EXECUTABLE_CACHE.cache = {}

    def test_read_lines(self):
        self.assertListEqual(list(read_lines(Path('foobar'), errors=False)), [])

        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write("1\n2\n3")
        self.assertListEqual(
            list(read_lines(path)),
            ['1', '2', '3'])
        self.assertListEqual(
            list(read_lines(path, convert=int)),
            [1, 2, 3])

    def test_read_chunked(self):
        self.assertListEqual([], list(read_bytes(Path('foobar'), errors=False)))
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write("1234567890")
        chunks = list(read_bytes(path, 3))
        self.assertListEqual([b'123', b'456', b'789', b'0'], chunks)

    def test_write_lines(self):
        linesep_len = len(os.linesep)
        path = self.root.make_file()
        assert 3 == write_lines(['foo'], path, linesep=None)
        assert list(read_lines(path)) == ['foo']
        path = self.root.make_file()
        self.assertEquals(
            9 + (2*linesep_len),
            write_lines(('foo', 'bar', 'baz'), path, linesep=None))
        self.assertEqual(
            list(read_lines(path)),
            ['foo', 'bar', 'baz'])
        path = self.root.make_file()
        self.assertEquals(
            11, write_lines(('foo', 'bar', 'baz'), path, linesep='|'))
        assert list(read_lines(path)) == ['foo|bar|baz']
        path = self.root.make_file(permissions='r')
        assert -1 == write_lines(['foo'], path, errors=False)

    def test_write_bytes(self):
        path = self.root.make_file()
        linesep_len = len(os.linesep)
        assert 3 == write_bytes([b'foo'], path)
        assert list(read_bytes(path)) == [b'foo']
        path = self.root.make_file()
        assert 9 + (2*linesep_len) == \
            write_bytes(('foo', 'bar', 'baz'), path, sep=None)
        self.assertEqual(
            os.linesep.encode().join((b'foo', b'bar', b'baz')),
            b''.join(read_bytes(path)))
        path = self.root.make_file(permissions='r')
        assert -1 == write_bytes([b'foo'], path, errors=False)

    def test_read_dict(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write("# This is a comment\n")
            o.write("foo=1\n")
            o.write("bar=2\n")
        d = read_dict(path, convert=int, ordered=True)
        assert len(d) == 2
        assert d['foo'] == 1
        assert d['bar'] == 2
        assert list(d.items()) == [('foo', 1), ('bar', 2)]

    def test_write_dict(self):
        path = self.root.make_file()
        write_dict(OrderedDict([('foo', 1), ('bar', 2)]), path, linesep=None)
        assert list(read_lines(path)) == ['foo=1', 'bar=2']

    def test_tsv(self):
        assert [] == list(read_delimited(Path('foobar'), errors=False))

        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('a\tb\tc\n')
            o.write('1\t2\t3\n')
            o.write('4\t5\t6\n')

        with self.assertRaises(ValueError):
            list(read_delimited(path, header=False, converters='int'))
        with self.assertRaises(ValueError):
            list(read_delimited(
                path, header=False, converters=int, row_type='dict',
                yield_header=False))

        assert [
            ['a', 'b', 'c'],
            [1, 2, 3],
            [4, 5, 6]
        ] == list(read_delimited(
            path, header=True, converters=int))
        assert [
            ['a', 'b', 'c'],
            (1, 2, 3),
            (4, 5, 6)
        ] == list(read_delimited(
            path, header=True, converters=int, row_type='tuple'))
        assert [
            ['a', 'b', 'c'],
            (1, 2, 3),
            (4, 5, 6)
        ] == list(read_delimited(
            path, header=True, converters=int, row_type=tuple))
        assert [
            dict(a=1, b=2, c=3),
            dict(a=4, b=5, c=6)
        ] == list(read_delimited(
            path, header=True, converters=int, row_type='dict',
            yield_header=False))

    def test_tsv_dict(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('id\ta\tb\tc\n')
            o.write('row1\t1\t2\t3\n')
            o.write('row2\t4\t5\t6\n')

        with self.assertRaises(ValueError):
            read_delimited_as_dict(path, key='id', header=False)
        with self.assertRaises(ValueError):
            read_delimited_as_dict(path, key=None, header=False)

        assert dict(
                row1=['row1', 1, 2, 3],
                row2=['row2', 4, 5, 6]
        ) == read_delimited_as_dict(
            path, key=0, header=True, converters=(str, int, int, int))
        assert dict(
                row1=['row1', 1, 2, 3],
                row2=['row2', 4, 5, 6]
        ) == read_delimited_as_dict(
            path, key='id', header=True, converters=(str, int, int, int))

        with open(path, 'wt') as o:
            o.write('a\tb\tc\n')
            o.write('1\t2\t3\n')
            o.write('4\t5\t6\n')

        assert dict(
            row1=[1, 2, 3],
            row4=[4, 5, 6]
        ) == read_delimited_as_dict(
            path, key=lambda row: 'row{}'.format(row[0]),
            header=True, converters=int)

    def test_tsv_dict_dups(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('id\ta\tb\tc\n')
            o.write('row1\t1\t2\t3\n')
            o.write('row1\t4\t5\t6\n')

        with self.assertRaises(Exception):
            read_delimited_as_dict(
                path, key='id', header=True, converters=(str, int, int, int))

    def test_compress_file_no_dest(self):
        path = self.root.make_file()

        with self.assertRaises(ValueError):
            compress_file(path, compression=True, keep=True)

        with open(path, 'wt') as o:
            o.write('foo')
        gzfile = compress_file(path, compression='gz', keep=False)
        assert gzfile == Path(str(path) + '.gz')
        assert not path.exists()
        assert gzfile.exists()
        with gzip.open(gzfile, 'rt') as i:
            assert i.read() == 'foo'

    def test_compress_fileobj(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('foo')

        f = open(path, 'rb')
        try:
            gzfile = compress_file(f, compression='gz')
            assert gzfile == Path(str(path) + '.gz')
            assert path.exists()
            assert gzfile.exists()
            with gzip.open(gzfile, 'rt') as i:
                assert i.read() == 'foo'
        finally:
            f.close()

        gzpath = Path(str(path) + '.gz')
        gzfile = gzip.open(gzpath, 'w')
        try:
            assert gzpath == compress_file(path, gzfile, compression=True)
        finally:
            gzfile.close()
        assert path.exists()
        assert gzpath.exists()
        with gzip.open(gzpath, 'rt') as i:
            assert i.read() == 'foo'

    def test_compress_file_no_compression(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('foo')
        gzfile = Path(str(path) + '.gz')
        gzfile2 = compress_file(path, gzfile, keep=True)
        assert gzfile == gzfile2
        assert path.exists()
        assert gzfile.exists()
        with gzip.open(gzfile, 'rt') as i:
            assert i.read() == 'foo'

    def test_decompress_file(self):
        path = self.root.make_file()
        gzfile = Path(str(path) + '.gz')
        with gzip.open(gzfile, 'wt') as o:
            o.write('foo')

        path2 = decompress_file(gzfile, keep=True)
        assert path == path2
        assert path.exists()
        assert gzfile.exists()
        with open(path, 'rt') as i:
            assert i.read() == 'foo'

        with open(gzfile, 'rb') as i:
            path2 = decompress_file(i, keep=True)
            assert path == path2
            assert path.exists()
            assert gzfile.exists()
            with open(path, 'rt') as j:
                assert j.read() == 'foo'

    def test_decompress_file_compression(self):
        path = self.root.make_file()
        gzfile = Path(str(path) + '.foo')
        with gzip.open(gzfile, 'wt') as o:
            o.write('foo')
        with self.assertRaises(ValueError):
            decompress_file(gzfile)
        path2 = decompress_file(gzfile, compression='gz', keep=False)
        assert path == path2
        assert path.exists()
        assert not gzfile.exists()
        with open(path, 'rt') as i:
            assert i.read() == 'foo'

    def test_transcode(self):
        path = self.root.make_file()
        gzfile = Path(str(path) + '.gz')
        with gzip.open(gzfile, 'wt') as o:
            o.write('foo')
        bzfile = Path(str(path) + '.bz2')
        transcode_file(gzfile, bzfile)
        with bz2.open(bzfile, 'rt') as i:
            assert 'foo' == i.read()

    def test_uncompressed_size(self):
        for ext in ('.gz', '.xz'):
            with self.subTest(ext):
                raw = self.root.make_file(contents=random_text(1000))
                compressed = self.root.make_file(suffix=ext)
                compress_file(raw, compressed)
                assert 1000 == uncompressed_size(compressed)

    def test_exec_process(self):
        inp = self.root.make_file(suffix='.gz')
        with gzip.open(inp, 'wt') as o:
            o.write('foo')
        out = self.root.make_file(suffix='.gz')
        exec_process('cat', stdin=inp, stdout=out)
        with gzip.open(out, 'rt') as o:
            assert 'foo' == o.read()

    def test_linecount(self):
        assert -1 == linecount(Path('foobar'), errors=False)
        path = self.root.make_file()
        with open(path, 'wt') as o:
            for i in range(100):
                o.write(random_text())
                if i != 99:
                    o.write('\n')
        with self.assertRaises(ValueError):
            linecount(path, buffer_size=-1)
        with self.assertRaises(ValueError):
            linecount(path, mode='wb')
        assert 100 == linecount(path)

    def test_linecount_empty(self):
        path = self.root.make_file()
        assert 0 == linecount(path)

    def test_file_manager(self):
        paths12 = dict(
            path1=self.root.make_empty_files(1)[0],
            path2=self.root.make_empty_files(1)[0])
        with FileManager(paths12, mode='wt') as f:
            paths34 = self.root.make_empty_files(2)
            for p in paths34:
                f.add(p, mode='wt')
                self.assertTrue(p in f)
                self.assertFalse(f[p].closed)
            path5 = self.root.make_file()
            path5_fh = open(path5, 'wt')
            f.add(path5_fh)
            path6 = self.root.make_file()
            f['path6'] = path6
            assert path6 == f.get_path('path6')
            all_paths = list(paths12.values()) + list(paths34) + [path5, path6]
            self.assertListEqual(all_paths, f.paths)
            assert len(f) == 6
            for key, fh in f.iter_files():
                self.assertFalse(fh.closed)
            assert f['path2'] is not None
            assert f.get('path2') is not None
            assert f['path6'] == f.get(5)
            with self.assertRaises(KeyError):
                _ = f['foo']
            assert f.get('foo') is None
        assert len(f) == 6
        for key, fh in f.iter_files():
            self.assertTrue(fh.closed)

    def test_file_manager_dup_files(self):
        f = FileManager()
        path = self.root.make_file()
        f.add(path)
        with self.assertRaises(ValueError):
            f.add(path)

    def test_compress_on_close(self):
        path = self.root.make_file()
        compressor = CompressOnClose(compression='gz')
        with FileWrapper(path, 'wt') as wrapper:
            wrapper.register_listener('close', compressor)
            wrapper.write('foo')
        gzfile = Path(str(path) + '.gz')
        assert gzfile == compressor.compressed_path
        self.assertTrue(os.path.exists(gzfile))
        with gzip.open(gzfile, 'rt') as i:
            assert i.read() == 'foo'

    def test_move_on_close(self):
        path = self.root.make_file()
        dest = self.root.make_file()
        with FileWrapper(path, 'wt') as wrapper:
            wrapper.register_listener('close', MoveOnClose(dest=dest))
            wrapper.write('foo')
        self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.exists(dest))
        with open(dest, 'rt') as i:
            assert i.read() == 'foo'

    def test_remove_on_close(self):
        path = self.root.make_file()
        with FileWrapper(path, 'wt') as wrapper:
            wrapper.register_listener('close', RemoveOnClose())
            wrapper.write('foo')
        self.assertFalse(os.path.exists(path))

        path = self.root.make_file()
        with FileWrapper(open(path, 'wt')) as wrapper:
            wrapper.register_listener('close', RemoveOnClose())
            wrapper.write('foo')
        self.assertFalse(os.path.exists(path))

    def test_fileinput(self):
        file1 = self.root.make_file(suffix='.gz')
        with gzip.open(file1, 'wt') as o:
            o.write('foo\nbar\n')
        with textinput(file1) as i:
            lines = list(i)
            self.assertListEqual(['foo\n', 'bar\n'], lines)
        file2 = self.root.make_file(suffix='.gz')
        with gzip.open(file2, 'wt') as o:
            o.write('baz\n')
        with textinput((file1, file2)) as i:
            lines = list(i)
            self.assertListEqual(['foo\n', 'bar\n', 'baz\n'], lines)
        with textinput([('key1', file1), ('key2', file2)]) as i:
            assert i.filekey is None
            assert i.filename is None
            assert i.lineno == 0
            assert i.filelineno == 0

            assert next(i) == 'foo\n'
            assert i.filekey == 'key1'
            assert i.filename == file1
            assert i.lineno == 1
            assert i.filelineno == 1

            assert next(i) == 'bar\n'
            assert i.filekey == 'key1'
            assert i.filename == file1
            assert i.lineno == 2
            assert i.filelineno == 2

            assert next(i) == 'baz\n'
            assert i.filekey == 'key2'
            assert i.filename == file2
            assert i.lineno == 3
            assert i.filelineno == 1

    def test_pending(self):
        file1 = self.root.make_file(suffix='.gz')
        with gzip.open(file1, 'wt') as o:
            o.write('foo\nbar\n')
        f = FileInput(char_mode=TextMode)
        self.assertTrue(f._pending)
        f.add(file1)
        list(f)
        self.assertTrue(f.finished)
        self.assertFalse(f._pending)
        file2 = self.root.make_file(suffix='.gz')
        with gzip.open(file2, 'wt') as o:
            o.write('baz\n')
        f.add(file2)
        self.assertTrue(f._pending)
        self.assertFalse(f.finished)
        assert 'baz\n' == f.readline()
        assert '' == f.readline()
        with self.assertRaises(StopIteration):
            next(f)
        self.assertTrue(f.finished)
        self.assertFalse(f._pending)

    def test_fileinput_defaults(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('foo\nbar\n')
        sys.argv = [self.system_args[0], path]
        self.assertEqual(
            ['foo\n', 'bar\n'],
            list(textinput()))
        sys.argv = []
        with intercept_stdin('foo\n'):
            lines = list(textinput([STDIN]))
            assert 1 == len(lines)
            assert 'foo\n' == lines[0]
        with intercept_stdin(b'foo\nbar\n', is_bytes=True):
            assert [b'foo\n', b'bar\n'] == list(byteinput())

    def test_single_fileoutput(self):
        file1 = self.root.make_file(suffix='.gz')
        with textoutput(file1) as o:
            o.writelines(('foo', 'bar', 'baz'))
        with gzip.open(file1, 'rt') as i:
            assert 'foo\nbar\nbaz\n' == i.read()

    def test_tee_fileoutput(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with self.assertRaises(ValueError):
            textoutput((file1, file2), access='z')
        with textoutput((file1, file2)) as o:
            o.writelines(('foo', 'bar', 'baz'))
        with gzip.open(file1, 'rt') as i:
            assert 'foo\nbar\nbaz\n' == i.read()
        with open(file2, 'rt') as i:
            assert 'foo\nbar\nbaz\n' == i.read()

    def test_tee_fileoutput_binary(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with byteoutput(
                (file1, file2),
                file_output_type=TeeFileOutput) as o:
            o.writelines((b'foo', b'bar', b'baz'))
        with gzip.open(file1, 'rb') as i:
            assert b'foo\nbar\nbaz\n' == i.read()
        with open(file2, 'rb') as i:
            assert b'foo\nbar\nbaz\n' == i.read()

        with textoutput((file1, file2), file_output_type=TeeFileOutput) as o:
            o.writelines((b'foo', b'bar', b'baz'))
        with gzip.open(file1, 'rt') as i:
            assert 'foo\nbar\nbaz\n' == i.read()
        with open(file2, 'rt') as i:
            assert 'foo\nbar\nbaz\n' == i.read()

        with byteoutput((file1, file2), file_output_type=TeeFileOutput) as o:
            o.writelines(('foo', b'bar', b'baz'))
        with gzip.open(file1, 'rb') as i:
            assert b'foo\nbar\nbaz\n' == i.read()
        with open(file2, 'rb') as i:
            assert b'foo\nbar\nbaz\n' == i.read()

    def test_tee_fileoutput_no_newline(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with textoutput((file1, file2)) as o:
            o.writeline('foo')
            o.writeline('bar')
            assert 2 == o.num_lines
        with gzip.open(file1, 'rb') as i:
            assert b'foo\nbar\n' == i.read()
        with open(file2, 'rb') as i:
            assert b'foo\nbar\n' == i.read()

    def test_fileoutput_stdout(self):
        path = self.root.make_file()
        sys.argv = [self.system_args, path]
        with textoutput() as o:
            o.writelines(('foo', 'bar', 'baz'))
        with open(path, 'rt') as i:
            assert 'foo\nbar\nbaz\n' == i.read()
        sys.argv = []
        with intercept_stdout(True) as outbuf:
            with byteoutput() as o:
                o.writelines((b'foo', b'bar', b'baz'))
            assert b'foo\nbar\nbaz\n' == outbuf.getvalue()

    def test_cycle_fileoutput(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with textoutput((file1, file2), file_output_type=CycleFileOutput) as o:
            o.writelines(('foo', 'bar', 'baz'))
        with gzip.open(file1, 'rt') as i:
            assert 'foo\nbaz\n' == i.read()
        with open(file2, 'rt') as i:
            assert 'bar\n' == i.read()

    def test_ncycle_fileoutput(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with textoutput(
                (file1, file2), lines_per_file=2,
                file_output_type=NCycleFileOutput) as o:
            o.writelines(('foo', 'bar', 'baz', 'blorf', 'bing'))
        with gzip.open(file1, 'rt') as i:
            assert 'foo\nbar\nbing\n' == i.read()
        with open(file2, 'rt') as i:
            assert 'baz\nblorf\n' == i.read()

    def test_rolling_fileoutput(self):
        path = str(self.root.make_file())
        with RollingFileOutput(
                path + '{index}.txt', char_mode=TextMode, linesep=os.linesep,
                lines_per_file=3) as out:
            for i in range(6):
                out.write(str(i))
        with open(path + '0.txt', 'rt') as infile:
            assert '0\n1\n2\n' == infile.read()
        with open(path + '1.txt', 'rt') as infile:
            assert '3\n4\n5\n' == infile.read()

    def test_fileoutput_with_header(self):
        path = str(self.root.make_file())
        with textoutput(
                path + '{index}.txt', file_output_type=RollingFileOutput,
                header="number\n", lines_per_file=3) as out:
            for i in range(6):
                out.write(str(i))
        with open(path + '0.txt', 'rt') as infile:
            assert 'number\n0\n1\n2\n' == infile.read()
        with open(path + '1.txt', 'rt') as infile:
            assert 'number\n3\n4\n5\n' == infile.read()

    def test_rolling_fileoutput_write(self):
        path = str(self.root.make_file())
        with textoutput(
                path + '{index}.txt', file_output_type=RollingFileOutput,
                lines_per_file=3) as out:
            for i in range(6):
                out.write(i, False)
            for ch in ('a', 'b', 'c'):
                out.write(ch, False)
            out.write("d\ne\nf")
        with open(path + '0.txt', 'rt') as infile:
            assert '0\n1\n2\n' == infile.read()
        with open(path + '1.txt', 'rt') as infile:
            assert '3\n4\n5\n' == infile.read()
        with open(path + '2.txt', 'rt') as infile:
            assert 'a\nb\nc\n' == infile.read()
        with open(path + '3.txt', 'rt') as infile:
            assert 'd\ne\nf\n' == infile.read()

    def test_pattern_file_output(self):
        path = self.root.make_file()

        def get_tokens(line):
            return dict(zip(('a', 'b'), line.split(' ')))

        with textoutput(
                str(path) + '{a}.{b}.txt',
                file_output_type=PatternFileOutput,
                token_func=get_tokens) as out:
            for a in range(2):
                for b in range(2):
                    out.writeline(f'{a} {b}')

        for a in range(2):
            for b in range(2):
                with open(str(path) + f'{a}.{b}.txt', 'rt') as infile:
                    assert f'{a} {b}\n' == infile.read()
