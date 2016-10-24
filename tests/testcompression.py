from unittest import TestCase, skipIf
import os
from xphyle.compression import *
from xphyle.paths import get_executable_path
from . import *

class CompressionTests(TestCase):
    def test_guess_format(self):
        self.assertEqual('gz', guess_compression_format('gz'))
        self.assertEqual('gz', guess_compression_format('.gz'))
        self.assertEqual('gz', guess_compression_format('foo.gz'))
    
    def test_invalid_format(self):
        self.assertIsNone(guess_compression_format('foo'))
        with self.assertRaises(ValueError):
            get_compression_format('foo')

def get_format(ext):
    return get_compression_format(guess_compression_format(ext))

def write_file(fmt, path, ext, use_system, text):
    with fmt.open_file(path, mode='wt', ext=ext, use_system=use_system) as f:
        f.write(text)

def read_file(fmt, path, ext, use_system):
    with fmt.open_file(path, mode='rt', ext=ext, use_system=use_system) as f:
        return f.read()

class FileTests(TestCase):
    def write_read_file(self, ext, use_system, text=None):
        if text is None:
            text = random_text(1024) # generate 1 kb of random text
        with make_file(suffix=ext) as path:
            fmt = get_format(ext)
            write_file(fmt, path, ext, use_system, text)
            in_text = read_file(fmt, path, ext, use_system)
            self.assertEqual(text, in_text)

    def test_write_read_python(self):
        for fmt in ('.gz','.bz2','.xz'):
            with self.subTest(fmt=fmt):
                self.write_read_file(fmt, False)
    
    # These tests will be skipped if the required system-level executables
    # are not available
    
    @skipIf(get_executable_path('gzip') is None, "'gzip' not available")
    def test_system_gzip(self):
        self.write_read_file('.gz', True)
    
    @skipIf(get_executable_path('gzip') is None, "'gzip' not available")
    def test_iter_system(self):
        with make_file(suffix='.gz') as path:
            text = 'line1\nline2\nline3'
            fmt = get_format('.gz')
            # Have to open in bytes mode, or it will get wrapped in a
            # TextBuffer, which does not use the underlying __iter__
            with fmt.open_file(path, mode='w', ext='.gz', use_system=True) as f:
                f.write(text.encode())
            with fmt.open_file(path, mode='r', ext='.gz', use_system=True) as f:
                lines = list(line.rstrip().decode() for line in iter(f))
            self.assertListEqual(lines, ['line1','line2','line3'])
    
    @skipIf(get_executable_path('bzip2') is None, "'gzip' not available")
    def test_system_gzip(self):
        self.write_read_file('.bz2', True)
    
    @skipIf(
        all(get_executable_path(exe) is None for exe in ('xz', 'lzma')),
        "'gzip' not available")
    def test_system_gzip(self):
        self.write_read_file('.xz', True)

class StringTests(TestCase):
    def test_compress(self):
        fmt = get_format('.gz')
    
    def test_compress(self):
        fmt = get_format('.gz')
        bytes = random_text(1024).encode()
        compressed = fmt.compress(bytes)
        decompressed = fmt.decompress(compressed)
        self.assertEqual(bytes, decompressed)
    
    def test_compress_string(self):
        fmt = get_format('.gz')
        text = random_text(1024)
        compressed = fmt.compress_string(text)
        decompressed = fmt.decompress_string(compressed)
        self.assertEqual(text, decompressed)
    
    def test_compress_iterable(self):
        fmt = get_format('.gz')
        strings = ['line1', 'line2', 'line3']
        compressed = fmt.compress_iterable(strings, delimiter=b'|')
        decompressed = fmt.decompress_string(compressed)
        self.assertListEqual(strings, decompressed.split('|'))
        
