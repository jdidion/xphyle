from unittest import TestCase
from . import *
from collections import OrderedDict
import gzip
import bz2
import os
import xphyle
from xphyle import FileWrapper
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
        self.assertListEqual(list(read_lines('foobar', errors=False)), [])
        
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write("1\n2\n3")
        self.assertListEqual(
            list(read_lines(path)),
            ['1','2','3'])
        self.assertListEqual(
            list(read_lines(path, convert=int)),
            [1,2,3])

    def test_read_chunked(self):
        self.assertListEqual([], list(read_bytes('foobar', errors=False)))
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write("1234567890")
        chunks = list(read_bytes(path, 3))
        self.assertListEqual([b'123', b'456', b'789', b'0'], chunks)

    def test_write_lines(self):
        linesep_len = len(os.linesep)
        path = self.root.make_file()
        self.assertEquals(3, write_lines(['foo'], path, linesep=None))
        self.assertEqual(list(read_lines(path)), ['foo'])
        path = self.root.make_file()
        self.assertEquals(
            9 + (2*linesep_len),
            write_lines(('foo', 'bar', 'baz'), path, linesep=None))
        self.assertEqual(
            list(read_lines(path)),
            ['foo','bar','baz'])
        path = self.root.make_file()
        self.assertEquals(
            11, write_lines(('foo', 'bar', 'baz'), path, linesep='|'))
        self.assertEqual(list(read_lines(path)), ['foo|bar|baz'])
        path = self.root.make_file(mode='r')
        self.assertEqual(-1, write_lines(['foo'], path, errors=False))
    
    def test_write_bytes(self):
        path = self.root.make_file()
        linesep_len = len(os.linesep)
        self.assertEquals(3, write_bytes([b'foo'], path))
        self.assertEqual(list(read_bytes(path)), [b'foo'])
        path = self.root.make_file()
        self.assertEquals(
            9 + (2*linesep_len),
            write_bytes(('foo', 'bar', 'baz'), path, sep=None))
        self.assertEqual(
            os.linesep.encode().join((b'foo',b'bar',b'baz')),
            b''.join(read_bytes(path)))
        path = self.root.make_file(mode='r')
        self.assertEqual(-1, write_bytes([b'foo'], path, errors=False))
    
    def test_read_dict(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write("# This is a comment\n")
            o.write("foo=1\n")
            o.write("bar=2\n")
        d = read_dict(path, convert=int, ordered=True)
        self.assertEqual(len(d), 2)
        self.assertEqual(d['foo'], 1)
        self.assertEqual(d['bar'], 2)
        self.assertEqual(list(d.items()), [('foo',1),('bar',2)])
    
    def test_write_dict(self):
        path = self.root.make_file()
        write_dict(OrderedDict([('foo',1), ('bar',2)]), path, linesep=None)
        self.assertEqual(
            list(read_lines(path)),
            ['foo=1','bar=2'])
    
    def test_tsv(self):
        self.assertListEqual([], list(read_delimited('foobar', errors=False)))
        
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
        
        self.assertListEqual(
            [
                ['a','b','c'],
                [1, 2, 3],
                [4, 5, 6]
            ],
            list(read_delimited(
                path, header=True, converters=int)))
        self.assertListEqual(
            [
                ['a','b','c'],
                (1, 2, 3),
                (4, 5, 6)
            ],
            list(read_delimited(
                path, header=True, converters=int, row_type='tuple')))
        self.assertListEqual(
            [
                ['a','b','c'],
                (1, 2, 3),
                (4, 5, 6)
            ],
            list(read_delimited(
                path, header=True, converters=int, row_type=tuple)))
        self.assertListEqual(
            [
                dict(a=1, b=2, c=3),
                dict(a=4, b=5, c=6)
            ],
            list(read_delimited(
                path, header=True, converters=int, row_type='dict',
                yield_header=False)))
    
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
        
        self.assertDictEqual(
            dict(
                row1=['row1',1,2,3],
                row2=['row2',4,5,6]
            ),
            read_delimited_as_dict(
                path, key=0, header=True, converters=(str,int,int,int)))
        self.assertDictEqual(
            dict(
                row1=['row1',1,2,3],
                row2=['row2',4,5,6]
            ),
            read_delimited_as_dict(
                path, key='id', header=True, converters=(str,int,int,int)))
        
        with open(path, 'wt') as o:
            o.write('a\tb\tc\n')
            o.write('1\t2\t3\n')
            o.write('4\t5\t6\n')
            
        self.assertDictEqual(
            dict(
                row1=[1,2,3],
                row4=[4,5,6]
            ),
            read_delimited_as_dict(
                path, key=lambda row: 'row{}'.format(row[0]),
                header=True, converters=int))
        
    def test_tsv_dict_dups(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('id\ta\tb\tc\n')
            o.write('row1\t1\t2\t3\n')
            o.write('row1\t4\t5\t6\n')
        
        with self.assertRaises(Exception):
            read_delimited_as_dict(
                path, key='id', header=True, converters=(str,int,int,int))
    
    def test_compress_file_no_dest(self):
        path = self.root.make_file()
        
        with self.assertRaises(ValueError):
            compress_file(path, compression=True, keep=True)

        with open(path, 'wt') as o:
            o.write('foo')
        gzfile = compress_file(path, compression='gz', keep=False)
        self.assertEqual(gzfile, path + '.gz')
        self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.exists(gzfile))
        with gzip.open(gzfile, 'rt') as i:
            self.assertEqual(i.read(), 'foo')
    
    def test_compress_file_no_compression(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('foo')
        gzfile = path + '.gz'
        gzfile2 = compress_file(path, gzfile, keep=True)
        self.assertEqual(gzfile, gzfile2)
        self.assertTrue(os.path.exists(path))
        self.assertTrue(os.path.exists(gzfile))
        with gzip.open(gzfile, 'rt') as i:
            self.assertEqual(i.read(), 'foo')
    
    def test_uncompress_file(self):
        path = self.root.make_file()
        gzfile = path + '.gz'
        with gzip.open(gzfile, 'wt') as o:
            o.write('foo')
        
        path2 = uncompress_file(gzfile, keep=True)
        self.assertEqual(path, path2)
        self.assertTrue(os.path.exists(gzfile))
        self.assertTrue(os.path.exists(path))
        with open(path, 'rt') as i:
            self.assertEqual(i.read(), 'foo')
        
        with open(gzfile, 'rb') as i:
            path2 = uncompress_file(i, keep=True)
            self.assertEqual(path, path2)
            self.assertTrue(os.path.exists(gzfile))
            self.assertTrue(os.path.exists(path))
            with open(path, 'rt') as i:
                self.assertEqual(i.read(), 'foo')
    
    def test_uncompress_file_compression(self):
        path = self.root.make_file()
        gzfile = path + '.foo'
        with gzip.open(gzfile, 'wt') as o:
            o.write('foo')
        with self.assertRaises(ValueError):
            uncompress_file(gzfile)
        path2 = uncompress_file(gzfile, compression='gz', keep=False)
        self.assertEqual(path, path2)
        self.assertFalse(os.path.exists(gzfile))
        self.assertTrue(os.path.exists(path))
        with open(path, 'rt') as i:
            self.assertEqual(i.read(), 'foo')
    
    def test_transcode(self):
        path = self.root.make_file()
        gzfile = path + '.gz'
        with gzip.open(gzfile, 'wt') as o:
            o.write('foo')
        bzfile = path + '.bz2'
        transcode_file(gzfile, bzfile)
        with bz2.open(bzfile, 'rt') as i:
            self.assertEqual('foo', i.read())
    
    def test_linecount(self):
        self.assertEqual(-1, linecount('foobar', errors=False))
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
        self.assertEqual(100, linecount(path))
    
    def test_linecount_empty(self):
        path = self.root.make_file()
        self.assertEqual(0, linecount(path))
    
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
            self.assertEqual(path6, f.get_path('path6'))
            all_paths = list(paths12.values()) + paths34 + [path5, path6]
            self.assertListEqual(all_paths, f.paths)
            self.assertEqual(len(f), 6)
            for key, fh in f.iter_files():
                self.assertFalse(fh.closed)
            self.assertIsNotNone(f['path2'])
            self.assertIsNotNone(f.get('path2'))
            self.assertEqual(f['path6'], f.get(5))
            with self.assertRaises(KeyError):
                f['foo']
            self.assertIsNone(f.get('foo'))
        self.assertEqual(len(f), 6)
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
        gzfile = path + '.gz'
        self.assertEqual(gzfile, compressor.compressed_path)
        self.assertTrue(os.path.exists(gzfile))
        with gzip.open(gzfile, 'rt') as i:
            self.assertEqual(i.read(), 'foo')

    def test_move_on_close(self):
        path = self.root.make_file()
        dest = self.root.make_file()
        with FileWrapper(path, 'wt') as wrapper:
            wrapper.register_listener('close', MoveOnClose(dest))
            wrapper.write('foo')
        self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.exists(dest))
        with open(dest, 'rt') as i:
            self.assertEqual(i.read(), 'foo')

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
    
    def test_file_input(self):
        file1 = self.root.make_file(suffix='.gz')
        with gzip.open(file1, 'wt') as o:
            o.write('foo\nbar\n')
        with fileinput(file1) as i:
            lines = list(i)
            self.assertListEqual(['foo\n','bar\n'], lines)
        file2 = self.root.make_file(suffix='.gz')
        with gzip.open(file2, 'wt') as o:
            o.write('baz\n')
        with fileinput((file1, file2)) as i:
            lines = list(i)
            self.assertListEqual(['foo\n','bar\n', 'baz\n'], lines)
        with fileinput([('key1',file1), ('key2', file2)]) as i:
            self.assertEqual(i.filekey, None)
            self.assertEqual(i.filename, None)
            self.assertEqual(i.lineno, 0)
            self.assertEqual(i.filelineno, 0)
            
            self.assertEqual(next(i), 'foo\n')
            self.assertEqual(i.filekey, 'key1')
            self.assertEqual(i.filename, file1)
            self.assertEqual(i.lineno, 1)
            self.assertEqual(i.filelineno, 1)
            
            self.assertEqual(next(i), 'bar\n')
            self.assertEqual(i.filekey, 'key1')
            self.assertEqual(i.filename, file1)
            self.assertEqual(i.lineno, 2)
            self.assertEqual(i.filelineno, 2)
            
            self.assertEqual(next(i), 'baz\n')
            self.assertEqual(i.filekey, 'key2')
            self.assertEqual(i.filename, file2)
            self.assertEqual(i.lineno, 3)
            self.assertEqual(i.filelineno, 1)
    
    def test_pending(self):
        file1 = self.root.make_file(suffix='.gz')
        with gzip.open(file1, 'wt') as o:
            o.write('foo\nbar\n')
        f = FileInput()
        self.assertTrue(f._pending)
        f.add(file1)
        l = list(f)
        self.assertTrue(f.finished)
        self.assertFalse(f._pending)
        file2 = self.root.make_file(suffix='.gz')
        with gzip.open(file2, 'wt') as o:
            o.write('baz\n')
        f.add(file2)
        self.assertTrue(f._pending)
        self.assertFalse(f.finished)
        self.assertEqual('baz\n', f.readline())
        self.assertEqual('', f.readline())
        with self.assertRaises(StopIteration):
            next(f)
        self.assertTrue(f.finished)
        self.assertFalse(f._pending)
    
    def test_file_input_defaults(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('foo\nbar\n')
        sys.argv = [self.system_args[0], path]
        self.assertEqual(
            ['foo\n','bar\n'],
            list(fileinput()))
        sys.argv = []
        with intercept_stdin('foo\n'):
            lines = list(fileinput([STDIN]))
            self.assertEqual(1, len(lines))
            self.assertEqual('foo\n', lines[0])
        with intercept_stdin(b'foo\nbar\n', is_bytes=True) as i:
            self.assertEqual(
            [b'foo\n',b'bar\n'],
            list(fileinput(mode=BinMode)))
    
    def test_tee_file_output(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with fileoutput((file1,file2)) as o:
            o.writelines(('foo','bar','baz'))
        with gzip.open(file1, 'rt') as i:
            self.assertEqual('foo\nbar\nbaz\n', i.read())
        with open(file2, 'rt') as i:
            self.assertEqual('foo\nbar\nbaz\n', i.read())
    
    def test_tee_file_output_binary(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with fileoutput((file1,file2), mode=BinMode,
                        file_output_type=TeeFileOutput) as o:
            o.writelines((b'foo',b'bar',b'baz'))
        with gzip.open(file1, 'rb') as i:
            self.assertEqual(b'foo\nbar\nbaz\n', i.read())
        with open(file2, 'rb') as i:
            self.assertEqual(b'foo\nbar\nbaz\n', i.read())
        
        with fileoutput((file1,file2), mode=TextMode,
                        file_output_type=TeeFileOutput) as o:
            o.writelines((b'foo',b'bar',b'baz'))
        with gzip.open(file1, 'rt') as i:
            self.assertEqual('foo\nbar\nbaz\n', i.read())
        with open(file2, 'rt') as i:
            self.assertEqual('foo\nbar\nbaz\n', i.read())
        
        with fileoutput((file1,file2), mode=BinMode,
                        file_output_type=TeeFileOutput) as o:
            o.writelines(('foo',b'bar',b'baz'))
        with gzip.open(file1, 'rb') as i:
            self.assertEqual(b'foo\nbar\nbaz\n', i.read())
        with open(file2, 'rb') as i:
            self.assertEqual(b'foo\nbar\nbaz\n', i.read())
    
    def test_tee_file_output_no_newline(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with fileoutput((file1,file2)) as o:
            o.writeline('foo', False)
            o.writeline(newline=True)
            o.writeline('bar', True)
            self.assertEqual(3, o.num_lines)
        with gzip.open(file1, 'rb') as i:
            self.assertEqual(b'foo\nbar\n', i.read())
        with open(file2, 'rb') as i:
            self.assertEqual(b'foo\nbar\n', i.read())
    
    def test_file_output_stdout(self):
        path = self.root.make_file()
        sys.argv = [self.system_args, path]
        with fileoutput() as o:
            o.writelines(('foo','bar','baz'))
        with open(path, 'rt') as i:
            self.assertEqual('foo\nbar\nbaz\n', i.read())
        sys.argv = []
        with intercept_stdout(True) as outbuf:
            with fileoutput(mode=BinMode) as o:
                o.writelines((b'foo',b'bar',b'baz'))
            self.assertEqual(b'foo\nbar\nbaz\n', outbuf.getvalue())
    
    def test_cycle_file_output(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with fileoutput((file1,file2), file_output_type=CycleFileOutput) as o:
            o.writelines(('foo','bar','baz'))
        with gzip.open(file1, 'rt') as i:
            self.assertEqual('foo\nbaz\n', i.read())
        with open(file2, 'rt') as i:
            self.assertEqual('bar\n', i.read())
    
    def test_ncycle_file_output(self):
        file1 = self.root.make_file(suffix='.gz')
        file2 = self.root.make_file()
        with fileoutput((file1,file2), lines_per_file=2,
                        file_output_type=NCycleFileOutput) as o:
            o.writelines(('foo','bar','baz','blorf','bing'))
        with gzip.open(file1, 'rt') as i:
            self.assertEqual('foo\nbar\nbing\n', i.read())
        with open(file2, 'rt') as i:
            self.assertEqual('baz\nblorf\n', i.read())
    
    def test_rolling_file_output(self):
        path = self.root.make_file()
        with RollingFileOutput(path + '{0}.txt', lines_per_file=3) as out:
            for i in range(6):
                out.writeline(str(i))
        with open(path + '0.txt', 'rt') as infile:
            self.assertEqual('0\n1\n2\n', infile.read())
        with open(path + '1.txt', 'rt') as infile:
            self.assertEqual('3\n4\n5\n', infile.read())
