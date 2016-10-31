from unittest import TestCase
from . import *
from xphyle import *
from xphyle.paths import STDOUT, STDERR
import gzip

class XphyleTests(TestCase):
    def test_guess_format(self):
        with self.assertRaises(ValueError):
            guess_file_format(STDOUT)
        with self.assertRaises(ValueError):
            guess_file_format(STDERR)
        with make_file(suffix='.gz') as path:
            with gzip.open(path, 'wt') as o:
                o.write('foo')
            self.assertEqual(guess_file_format(path), 'gz')
        with make_file() as path:
            with gzip.open(path, 'wt') as o:
                o.write('foo')
            self.assertEqual(guess_file_format(path), 'gz')
        
    def test_open_(self):
        with make_file(contents='foo') as path:
            with open_(path, compression=False) as fh:
                self.assertEqual(fh.read(), 'foo')
            with open(path) as fh:
                with open_(fh, compression=False) as fh2:
                    self.assertEqual(fh2.read(), 'foo')
    
    def test_xopen(self):
        
    
    
