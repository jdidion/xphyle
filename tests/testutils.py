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
            self.assertListEqual(
                [
                    ['a','b','c'],
                    [1, 2, 3],
                    [4, 5, 6]
                ],
                list(delimited_file_iter(path, header=True, converters=int)))
