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

    def test_write_strings(self):
        with make_file() as path:
            write_strings('foo', path, linesep=None)
            self.assertEqual(safe_read(path), 'foo')
        with make_file() as path:
            write_strings(('foo', 'bar', 'baz'), path, linesep=None)
            self.assertEqual(
                safe_read(path),
                os.linesep.join(('foo','bar','baz')))
        with make_file() as path:
            write_strings(('foo', 'bar', 'baz'), path, linesep='|')
            self.assertEqual(safe_read(path), 'foo|bar|baz')

    def test_write_dict(self):
        with make_file() as path:
            write_dict(OrderedDict(foo=1, bar=2), path, linesep=None)
            self.assertEqual(
                safe_read(path),
                os.linesep.join(('foo=1','bar=2')))
