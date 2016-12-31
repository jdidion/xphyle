from unittest import TestCase, skipIf
from . import *
import gzip
from io import StringIO, BytesIO, TextIOWrapper
from xphyle import *
from xphyle.paths import TempDir, STDIN, STDOUT, STDERR, EXECUTABLE_CACHE
from xphyle.progress import ITERABLE_PROGRESS, PROCESS_PROGRESS
from xphyle.formats import THREADS

class XphyleTests(TestCase):
    def setUp(self):
        self.root = TempDir()
    
    def tearDown(self):
        self.root.close()
        ITERABLE_PROGRESS.enabled = False
        ITERABLE_PROGRESS.wrapper = None
        PROCESS_PROGRESS.enabled = False
        PROCESS_PROGRESS.wrapper = None
        THREADS.update(1)
        EXECUTABLE_CACHE.reset_search_path()
        EXECUTABLE_CACHE.cache = {}
    
    def test_configure(self):
        import xphyle.progress
        import xphyle.formats
        import xphyle.paths
        def wrapper(a,b,c):
            pass
        configure(progress=True, progress_wrapper=wrapper,
                  system_progress=True, system_progress_wrapper='foo',
                  threads=2, executable_path=['foo'])
        self.assertEqual(wrapper, ITERABLE_PROGRESS.wrapper)
        self.assertEqual(('foo',), PROCESS_PROGRESS.wrapper)
        self.assertEqual(2, THREADS.threads)
        self.assertTrue('foo' in EXECUTABLE_CACHE.search_path)
        
        configure(threads=False)
        self.assertEqual(1, THREADS.threads)
        
        import multiprocessing
        configure(threads=True)
        self.assertEqual(multiprocessing.cpu_count(), THREADS.threads)
    
    def test_guess_format(self):
        with self.assertRaises(ValueError):
            guess_file_format(STDOUT)
        with self.assertRaises(ValueError):
            guess_file_format(STDERR)
        path = self.root.make_file(suffix='.gz')
        with gzip.open(path, 'wt') as o:
            o.write('foo')
        self.assertEqual(guess_file_format(path), 'gzip')
        path = self.root.make_file()
        with gzip.open(path, 'wt') as o:
            o.write('foo')
        self.assertEqual(guess_file_format(path), 'gzip')
        
    def test_open_(self):
        path = self.root.make_file(contents='foo')
        with open_(path, compression=False) as fh:
            self.assertEqual(fh.read(), 'foo')
        with open_(path, compression=False) as fh:
            self.assertEqual(next(fh), 'foo')
        with open(path) as fh:
            with open_(fh, compression=False) as fh2:
                self.assertEqual(fh2.read(), 'foo')
    
    def test_open_safe(self):
        with self.assertRaises(IOError):
            with open_('foobar', mode='r', errors=True) as fh:
                pass
        with self.assertRaises(ValueError):
            with open_(None, mode='r', errors=True) as fh:
                pass
        with open_('foobar', mode='r', errors=False) as fh:
            self.assertIsNone(fh)
        with open_(None, mode='r', errors=False) as fh:
            self.assertIsNone(fh)
    
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
            xopen(STDOUT, 'w', compression=True)
        with self.assertRaises(ValueError):
            xopen('foo.bar', 'w', compression=True)
    
    def test_xopen_std(self):
        # Try stdin
        with intercept_stdin('foo\n'):
            with xopen(STDIN, 'r', context_wrapper=True, compression=False) as i:
                content = i.read()
                self.assertEqual(content, 'foo\n')
        # Try stdout
        with intercept_stdout() as i:
            with xopen(STDOUT, 'w', context_wrapper=True, compression=False) as o:
                o.write('foo')
            self.assertEqual(i.getvalue(), 'foo')
        # Try stderr
        with intercept_stderr() as i:
            with xopen(STDERR, 'w', context_wrapper=True, compression=False) as o:
                o.write('foo')
            self.assertEqual(i.getvalue(), 'foo')
        
        # Try binary
        with intercept_stdout(True) as i:
            with xopen(STDOUT, 'wb', context_wrapper=True, compression=False) as o:
                o.write(b'foo')
            self.assertEqual(i.getvalue(), b'foo')
        
        # Try compressed
        with intercept_stdout(True) as i:
            with xopen(STDOUT, 'wt', compression='gz') as o:
                self.assertEqual(o.compression, 'gzip')
                o.write('foo')
            self.assertEqual(gzip.decompress(i.getvalue()), b'foo')
    
    def test_xopen_compressed_stream(self):
        # Try autodetect compressed
        with intercept_stdin(gzip.compress(b'foo\n'), is_bytes=True):
            with xopen(STDIN, 'rt', compression=True) as i:
                self.assertEqual(i.compression, 'gzip')
                self.assertEqual(i.read(), 'foo\n')
    
    def test_xopen_file(self):
        with self.assertRaises(IOError):
            xopen('foobar', 'r')
        path = self.root.make_file(suffix='.gz')
        with xopen(path, 'w', compression=True) as o:
            self.assertEqual(o.compression, 'gzip')
            o.write('foo')
        with gzip.open(path, 'rt') as i:
            self.assertEqual(i.read(), 'foo')
        with self.assertRaises(ValueError):
            with xopen(path, 'rt', compression='bz2', validate=True):
                pass
    
    @skipIf(no_internet(), "No internet connection")
    def test_xopen_url(self):
        badurl = 'http://google.com/__badurl__'
        with self.assertRaises(ValueError):
            xopen(badurl)
        url = 'https://github.com/jdidion/xphyle/blob/master/tests/foo.gz?raw=True'
        with self.assertRaises(ValueError):
            xopen(url, 'w')
        with open_(url, 'rt') as i:
            self.assertEqual('gzip', i.compression)
            self.assertEqual('foo\n', i.read())
