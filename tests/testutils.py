from unittest import TestCase
from . import *
from collections import OrderedDict
import os
from xphyle.utils import *

class UtilsTests(TestCase):
    def test_safe_read(self):
        self.assertEqual(safe_read('foobar'), '')
        self.assertListEqual(list(safe_iter('foobar')), [])
        
        with make_file() as path:
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
        with make_file() as path:
            with open(path, 'wt') as o:
                o.write("1234567890")
            chunks = list(chunked_iter(path, 3))
            self.assertListEqual([b'123', b'456', b'789', b'0'], chunks)

    def test_write_iterable(self):
        with make_file() as path:
            write_iterable(['foo'], path, linesep=None)
            self.assertEqual(safe_read(path), 'foo')
        with make_file() as path:
            write_iterable(('foo', 'bar', 'baz'), path, linesep=None)
            self.assertEqual(
                safe_read(path),
                os.linesep.join(('foo','bar','baz')))
        with make_file() as path:
            write_iterable(('foo', 'bar', 'baz'), path, linesep='|')
            self.assertEqual(safe_read(path), 'foo|bar|baz')
    
    def test_read_dict(self):
        with make_file() as path:
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
        with make_file() as path:
            write_dict(OrderedDict([('foo',1), ('bar',2)]), path, linesep=None)
            self.assertEqual(
                safe_read(path),
                os.linesep.join(('foo=1','bar=2')))
    
    def test_tsv(self):
        with make_file() as path:
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
        with make_file() as path:
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
            with make_file() as path:
                with open(path, 'wt') as o:
                    o.write('id\ta\tb\tc\n')
                    o.write('row1\t1\t2\t3\n')
                    o.write('row1\t4\t5\t6\n')
                
                with self.assertRaises(Exception):
                    delimited_file_to_dict(
                        path, key='id', header=True, converters=int)
            
                
                
