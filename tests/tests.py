from unittest import TestCase, skipIf
from . import *
import gzip
from io import StringIO, BytesIO, TextIOWrapper
from xphyle import *
from xphyle.paths import TempDir, STDIN, STDOUT, STDERR

class XphyleTests(TestCase):
    def setUp(self):
        self.root = TempDir()
    
    def tearDown(self):
        self.root.close()
    
    def test_guess_format(self):
        with self.assertRaises(ValueError):
            guess_file_format(STDOUT)
        with self.assertRaises(ValueError):
            guess_file_format(STDERR)
        path = self.root.make_file(suffix='.gz')
        with gzip.open(path, 'wt') as o:
            o.write('foo')
        self.assertEqual(guess_file_format(path), 'gz')
        path = self.root.make_file()
        with gzip.open(path, 'wt') as o:
            o.write('foo')
        self.assertEqual(guess_file_format(path), 'gzip')
        
    def test_open_(self):
        path = self.root.make_file(contents='foo')
        with open_(path, compression=False) as fh:
            self.assertEqual(fh.read(), 'foo')
        with open(path) as fh:
            with open_(fh, compression=False) as fh2:
                self.assertEqual(fh2.read(), 'foo')

    def test_xopen_invalid(self):
        # invalid path
        with self.assertRaises(ValueError):
            xopen(1)
        # invalid mode
        with self.assertRaises(ValueError):
            xopen('foo', 'z')
        with self.assertRaises(ValueError):
            xopen('foo', 'rz')
        with self.assertRaises(ValueError):
            xopen('foo', 'rU', newline='\n')
        with self.assertRaises(ValueError):
            xopen(STDOUT, compression=True)
        with self.assertRaises(ValueError):
            xopen('foo.bar', 'w', compression=True)
    
    def test_xopen_std(self):
        # Try stdin
        with intercept_stdin('foo\n'):
            with xopen(STDIN, 'r', context_wrapper=True) as i:
                content = i.read()
                self.assertEqual(content, 'foo\n')
        # Try stdout
        i = StringIO()
        with intercept_stdout(i):
            with xopen(STDOUT, 'w', context_wrapper=True) as o:
                o.write('foo')
            self.assertEqual(i.getvalue(), 'foo')
        # Try stderr
        i = StringIO()
        with intercept_stderr(i):
            with xopen(STDERR, 'w', context_wrapper=True) as o:
                o.write('foo')
            self.assertEqual(i.getvalue(), 'foo')
        
        # Try binary
        i = BytesIO()
        with intercept_stdout(TextIOWrapper(i)):
            with xopen(STDOUT, 'wb', context_wrapper=True) as o:
                o.write(b'foo')
            self.assertEqual(i.getvalue(), b'foo')
        
        # Try compressed
        i = BytesIO()
        with intercept_stdout(TextIOWrapper(i)):
            with xopen(STDOUT, 'wt', compression='gz') as o:
                o.write('foo')
            self.assertEqual(gzip.decompress(i.getvalue()), b'foo')
    
    def test_xopen_file(self):
        with self.assertRaises(IOError):
            xopen('foobar', 'r')
        path = self.root.make_file(suffix='.gz')
        with xopen(path, 'w', compression=True) as o:
            o.write('foo')
        with gzip.open(path, 'rt') as i:
            self.assertEqual(i.read(), 'foo')
    
    @skipIf(no_internet(), "No internet connection")
    def test_xopen_url(self):
        url = 'https://github.com/jdidion/xphyle/blob/master/tests/foo.gz?raw=True'
        with self.assertRaises(ValueError):
            xopen(url, 'w')
        with open_(url, 'rt') as i:
            self.assertEqual(i.read(), 'foo\n')
