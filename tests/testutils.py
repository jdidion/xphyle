from unittest import TestCase
from . import *
from collections import OrderedDict
import gzip
import os
from xphyle.paths import TempDir
from xphyle.utils import *

class UtilsTests(TestCase):
    def setUp(self):
        self.root = TempDir()
    
    def tearDown(self):
        self.root.close()
    
    def test_safe_read(self):
        self.assertEqual(safe_read('foobar'), '')
        self.assertListEqual(list(safe_iter('foobar')), [])
        
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write("1\n2\n3")
        self.assertEqual(safe_read(path), "1\n2\n3")
        self.assertListEqual(
            list(safe_iter(path)),
            ['1','2','3'])
        self.assertListEqual(
            list(safe_iter(path, convert=int)),
            [1,2,3])

    def test_read_chunked(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write("1234567890")
        chunks = list(chunked_iter(path, 3))
        self.assertListEqual([b'123', b'456', b'789', b'0'], chunks)

    def test_write_iterable(self):
        path = self.root.make_file()
        write_iterable(['foo'], path, linesep=None)
        self.assertEqual(safe_read(path), 'foo')
        path = self.root.make_file()
        write_iterable(('foo', 'bar', 'baz'), path, linesep=None)
        self.assertEqual(
            safe_read(path),
            os.linesep.join(('foo','bar','baz')))
        path = self.root.make_file()
        write_iterable(('foo', 'bar', 'baz'), path, linesep='|')
        self.assertEqual(safe_read(path), 'foo|bar|baz')
    
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
            safe_read(path),
            os.linesep.join(('foo=1','bar=2')))
    
    def test_tsv(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('a\tb\tc\n')
            o.write('1\t2\t3\n')
            o.write('4\t5\t6\n')
        
        with self.assertRaises(ValueError):
            list(delimited_file_iter(path, header=False, converters='int'))
        with self.assertRaises(ValueError):
            list(delimited_file_iter(
                path, header=False, converters=int, row_type='dict',
                yield_header=False))
        
        self.assertListEqual(
            [
                ['a','b','c'],
                [1, 2, 3],
                [4, 5, 6]
            ],
            list(delimited_file_iter(
                path, header=True, converters=int)))
        self.assertListEqual(
            [
                ['a','b','c'],
                (1, 2, 3),
                (4, 5, 6)
            ],
            list(delimited_file_iter(
                path, header=True, converters=int, row_type='tuple')))
        self.assertListEqual(
            [
                ['a','b','c'],
                (1, 2, 3),
                (4, 5, 6)
            ],
            list(delimited_file_iter(
                path, header=True, converters=int, row_type=tuple)))
        self.assertListEqual(
            [
                dict(a=1, b=2, c=3),
                dict(a=4, b=5, c=6)
            ],
            list(delimited_file_iter(
                path, header=True, converters=int, row_type='dict',
                yield_header=False)))
    
    def test_tsv_dict(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('id\ta\tb\tc\n')
            o.write('row1\t1\t2\t3\n')
            o.write('row2\t4\t5\t6\n')
        
        with self.assertRaises(ValueError):
            delimited_file_to_dict(path, key='id', header=False)
        with self.assertRaises(ValueError):
            delimited_file_to_dict(path, key=None, header=False)
        
        self.assertDictEqual(
            dict(
                row1=['row1',1,2,3],
                row2=['row2',4,5,6]
            ),
            delimited_file_to_dict(
                path, key=0, header=True, converters=(str,int,int,int)))
        self.assertDictEqual(
            dict(
                row1=['row1',1,2,3],
                row2=['row2',4,5,6]
            ),
            delimited_file_to_dict(
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
            delimited_file_to_dict(
                path, key=lambda row: 'row{}'.format(row[0]),
                header=True, converters=int))
        
    def test_tsv_dict_dups(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            o.write('id\ta\tb\tc\n')
            o.write('row1\t1\t2\t3\n')
            o.write('row1\t4\t5\t6\n')
        
        with self.assertRaises(Exception):
            delimited_file_to_dict(
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
    
    def test_linecount(self):
        path = self.root.make_file()
        with open(path, 'wt') as o:
            for i in range(100):
                o.write(random_text())
                if i != 99:
                    o.write('\n')
        self.assertEqual(100, linecount(path))
    
    def test_linecount_empty(self):
        path = self.root.make_file()
        self.assertEqual(0, linecount(path))
    
    def test_file_manager(self):
        f = FileManager()
        paths = self.root.make_empty_files(3)
        for p in paths:
            f.add(p, mode='wt')
            self.assertTrue(p in f)
            self.assertFalse(f[p].closed)
        path2 = open(self.root.make_file(), 'wt')
        f.add(path2)
        path3 = self.root.make_file()
        f['path3'] = path3
        self.assertEqual(len(f), 5)
        for key, fh in f.items():
            self.assertFalse(fh.closed)
        f.close()
        self.assertEqual(len(f), 5)
        for key, fh in f.items():
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
