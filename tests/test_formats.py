from unittest import TestCase, skipIf
import gzip
import os
from xphyle.formats import *
from xphyle.paths import TempDir, EXECUTABLE_CACHE
from . import *

def get_format(ext):
    return FORMATS.get_compression_format(FORMATS.guess_compression_format(ext))

def write_file(fmt, path, use_system, content, mode='wt'):
    with fmt.open_file(path, mode=mode, use_system=use_system) as f:
        f.write(content)

def read_file(fmt, path, use_system, mode='rt'):
    with fmt.open_file(path, mode=mode, use_system=use_system) as f:
        return f.read()

gz_path = get_format('gz').executable_path
no_pigz = gz_path is None or get_format('gz').executable_name != 'pigz'
bgz_path = get_format('bgz').executable_path
no_bgzip = gz_path is None or get_format('bgz').executable_name != 'bgzip'
bz_path = get_format('bz2').executable_path
no_pbzip2 = bz_path is None or get_format('bz2').executable_name != 'pbzip2'
xz_path = get_format('xz').executable_path

class ThreadsTests(TestCase):
    def test_threads(self):
        threads = ThreadsVar(default_value=2)
        threads.update(None)
        self.assertEquals(2, threads.threads)
        threads.update(False)
        self.assertEquals(1, threads.threads)
        threads.update(0)
        self.assertEquals(1, threads.threads)
        import multiprocessing
        threads.update(True)
        self.assertEquals(multiprocessing.cpu_count(), threads.threads)
        threads.update(4)
        self.assertEquals(4, threads.threads)

class CompressionTests(TestCase):
    def tearDown(self):
        EXECUTABLE_CACHE.cache = {}
        THREADS.update(1)
    
    def test_list_formats(self):
        self.assertSetEqual(
            set(('gzip','bgzip','bz2','lzma')),
            set(FORMATS.list_compression_formats()))
        self.assertSetEqual(
            set(('gzip','gz','pigz')),
            set(get_format('gzip').aliases))
    
    def test_guess_format(self):
        self.assertEqual('gzip', FORMATS.guess_compression_format('gz'))
        self.assertEqual('gzip', FORMATS.guess_compression_format('.gz'))
        self.assertEqual('gzip', FORMATS.guess_compression_format('foo.gz'))
    
    def test_invalid_format(self):
        self.assertIsNone(FORMATS.guess_compression_format('foo'))
        with self.assertRaises(ValueError):
            FORMATS.get_compression_format('foo')
    
    def test_get_format_from_mime_type(self):
        self.assertEqual(
            'gzip', FORMATS.get_format_for_mime_type('application/gz'))
        self.assertEqual(
            'bz2', FORMATS.get_format_for_mime_type('application/bz2'))
        self.assertEqual(
            'lzma', FORMATS.get_format_for_mime_type('application/lzma'))
    
    # TODO: need a way to force selection of a specific executable to properly
    # test all possible scenarios
    
    def _test_format(self, fmt):
        self.assertEqual(fmt.default_compresslevel, fmt._get_compresslevel(None))
        self.assertEqual(fmt.compresslevel_range[0], fmt._get_compresslevel(-1))
        self.assertEqual(fmt.compresslevel_range[1], fmt._get_compresslevel(100))
            
    def test_gzip(self):
        gz = get_format('gz')
        self._test_format(gz)
        self.assertEqual(gz.default_ext, 'gz')
        self.assertEqual(
            gz.get_command('c', compresslevel=5),
            [gz_path, '-5', '-c'])
        self.assertEqual(
            gz.get_command('c', 'foo.bar', compresslevel=5),
            [gz_path, '-5', '-c', 'foo.bar'])
        self.assertEqual(
            gz.get_command('d'),
            [gz_path, '-d', '-c'])
        self.assertEqual(
            gz.get_command('d', 'foo.gz'),
            [gz_path, '-d', '-c', 'foo.gz'])
    
    @skipIf(no_pigz, "'pigz' not available")
    def test_pigz(self):
        THREADS.update(2)
        gz = get_format('gz')
        self.assertEqual(gz.default_ext, 'gz')
        self.assertEqual(
            gz.get_command('c', compresslevel=5),
            [gz_path, '-5', '-c', '-p', '2'])
        self.assertEqual(
            gz.get_command('c', 'foo.bar', compresslevel=5),
            [gz_path, '-5', '-c', '-p', '2', 'foo.bar'])
        self.assertEqual(
            gz.get_command('d'),
            [gz_path, '-d', '-c', '-p', '2'])
        self.assertEqual(
            gz.get_command('d', 'foo.gz'),
            [gz_path, '-d', '-c', '-p', '2', 'foo.gz'])
    
    @skipIf(no_bgzip, "'bgzip' not available")
    def test_bgzip(self):
        THREADS.update(2)
        bgz = get_format('bgz')
        self.assertEqual(bgz.default_ext, 'bgz')
        self.assertEqual(
            bgz.get_command('c'),
            [bgz_path, '-c', '-@', '2'])
        self.assertEqual(
            bgz.get_command('c', 'foo.bar', compresslevel=5),
            [bgz_path, '-c', '-@', '2', 'foo.bar'])
        self.assertEqual(
            bgz.get_command('d'),
            [bgz_path, '-d', '-c', '-@', '2'])
        self.assertEqual(
            bgz.get_command('d', 'foo.gz'),
            [bgz_path, '-d', '-c', '-@', '2', 'foo.gz'])
    
    def test_bzip2(self):
        bz = get_format('bz2')
        self._test_format(bz)
        self.assertEqual(bz.default_ext, 'bz2')
        self.assertEqual(
            bz.get_command('c', compresslevel=5),
            [bz_path, '-5', '-z', '-c'])
        self.assertEqual(
            bz.get_command('c', 'foo.bar', compresslevel=5),
            [bz_path, '-5', '-z', '-c', 'foo.bar'])
        self.assertEqual(
            bz.get_command('d'),
            [bz_path, '-d', '-c'])
        self.assertEqual(
            bz.get_command('d', 'foo.bz2'),
            [bz_path, '-d', '-c', 'foo.bz2'])
    
    @skipIf(no_pbzip2, "'pbzip2' not available")
    def test_pbzip2(self):
        THREADS.update(2)
        bz = get_format('bz2')
        self.assertEqual(bz.default_ext, 'bz2')
        self.assertEqual(
            bz.get_command('c', compresslevel=5),
            [bz_path, '-5', '-z', '-c', '-p2'])
        self.assertEqual(
            bz.get_command('c', 'foo.bar', compresslevel=5),
            [bz_path, '-5', '-z', '-c', '-p2', 'foo.bar'])
        self.assertEqual(
            bz.get_command('d'),
            [bz_path, '-d', '-c', '-p2'])
        self.assertEqual(
            bz.get_command('d', 'foo.bz2'),
            [bz_path, '-d', '-c', '-p2', 'foo.bz2'])
    
    def test_lzma(self):
        xz = get_format('xz')
        self._test_format(xz)
        self.assertEqual(xz.default_ext, 'xz')
        self.assertEqual(
            xz.get_command('c', compresslevel=5),
            [xz_path, '-5', '-z', '-c'])
        self.assertEqual(
            xz.get_command('c', 'foo.bar', compresslevel=5),
            [xz_path, '-5', '-z', '-c', 'foo.bar'])
        self.assertEqual(
            xz.get_command('d'),
            [xz_path, '-d', '-c'])
        self.assertEqual(
            xz.get_command('d', 'foo.xz'),
            [xz_path, '-d', '-c', 'foo.xz'])
        # Test with threads
        THREADS.update(2)
        self.assertEqual(
            xz.get_command('c', compresslevel=5),
            [xz_path, '-5', '-z', '-c', '-T', '2'])
        self.assertEqual(
            xz.get_command('c', 'foo.bar', compresslevel=5),
            [xz_path, '-5', '-z', '-c', '-T', '2', 'foo.bar'])
        self.assertEqual(
            xz.get_command('d'),
            [xz_path, '-d', '-c', '-T', '2'])

class FileTests(TestCase):
    def setUp(self):
        self.root = TempDir()
    
    def tearDown(self):
        self.root.close()
    
    def test_invalid(self):
        with self.assertRaises(ValueError):
            get_format('gz').open_file('foo', 'n')
        
    def write_read_file(self, ext, use_system, mode='t', content=None):
        if content is None:
            content = random_text() # generate 1 kb of random text
            if mode == 'b':
                content = b''.join(c.encode() for c in content)
        path = self.root.make_file(suffix=ext)
        fmt = get_format(ext)
        write_file(fmt, path, use_system, content, 'w' + mode)
        in_text = read_file(fmt, path, use_system, 'r' + mode)
        self.assertEqual(content, in_text)
    
    def test_write_read_bytes_python(self):
        for fmt in ('.gz','.bz2','.xz'):
            with self.subTest(fmt=fmt):
                self.write_read_file(fmt, False, 'b')
    
    def test_write_read_text_python(self):
        for fmt in ('.gz','.bz2','.xz'):
            with self.subTest(fmt=fmt):
                self.write_read_file(fmt, False, 't')
    
    # These tests will be skipped if the required system-level executables
    # are not available
    
    @skipIf(gz_path is None, "'gzip' not available")
    def test_system_gzip(self):
        self.write_read_file('.gz', True)
    
    @skipIf(gz_path is None, "'gzip' not available")
    def test_iter_system(self):
        path = self.root.make_file(suffix='.gz')
        text = 'line1\nline2\nline3'
        fmt = get_format('.gz')
        # Have to open in bytes mode, or it will get wrapped in a
        # TextBuffer, which does not use the underlying __iter__
        with fmt.open_file(path, mode='wb', ext='.gz', use_system=True) as f:
            f.write(text.encode())
        with fmt.open_file(path, mode='rb', ext='.gz', use_system=True) as f:
            lines = list(line.rstrip().decode() for line in iter(f))
        self.assertListEqual(lines, ['line1','line2','line3'])
    
    @skipIf(bz_path is None, "'bzip2' not available")
    def test_system_bzip(self):
        self.write_read_file('.bz2', True)
    
    @skipIf(xz_path is None, "'xz' not available")
    def test_system_lzma(self):
        self.write_read_file('.xz', True)
    
    def test_compress_path(self):
        b = (True, False) if gz_path else (False,)
        for use_system in b:
            with self.subTest(use_system=use_system):
                path = self.root.make_file()
                with open(path, 'wt') as o:
                    o.write('foo')
                fmt = get_format('.gz')
                dest = fmt.compress_file(path, use_system=use_system)
                gzfile = path + '.gz'
                self.assertEqual(dest, gzfile)
                self.assertTrue(os.path.exists(path))
                self.assertTrue(os.path.exists(gzfile))
                with gzip.open(gzfile, 'rt') as i:
                    self.assertEqual(i.read(), 'foo')
                
                path = self.root.make_file()
                with open(path, 'wt') as o:
                    o.write('foo')
                gzfile = path + '.bar'
                fmt = get_format('.gz')
                dest = fmt.compress_file(
                    path, gzfile, keep=False, use_system=use_system)
                self.assertEqual(dest, gzfile)
                self.assertFalse(os.path.exists(path))
                self.assertTrue(os.path.exists(gzfile))
                with gzip.open(gzfile, 'rt') as i:
                    self.assertEqual(i.read(), 'foo')
    
    def test_compress_file(self):
        b = (True, False) if gz_path else (False,)
        for use_system in b:
            with self.subTest(use_system=use_system):
                path = self.root.make_file()
                with open(path, 'wt') as o:
                    o.write('foo')
                with open(path, 'rb') as i:
                    fmt = get_format('.gz')
                    dest = fmt.compress_file(i, use_system=use_system)
                gzfile = path + '.gz'
                self.assertEqual(dest, gzfile)
                self.assertTrue(os.path.exists(gzfile))
                with gzip.open(gzfile, 'rt') as i:
                    self.assertEqual(i.read(), 'foo')
                
                path = self.root.make_file()
                with open(path, 'wt') as o:
                    o.write('foo')
                gzfile = path + '.bar'
                with open(path, 'rb') as i:
                    fmt = get_format('.gz')
                    dest = fmt.compress_file(
                        i, gzfile, keep=False, use_system=use_system)
                self.assertEqual(dest, gzfile)
                self.assertFalse(os.path.exists(path))
                self.assertTrue(os.path.exists(gzfile))
                with gzip.open(gzfile, 'rt') as i:
                    self.assertEqual(i.read(), 'foo')
    
    def test_decompress_path_error(self):
        path = self.root.make_file()
        with gzip.open(path, 'wt') as o:
            o.write('foo')
        with self.assertRaises(Exception):
            fmt = get_format('.gz')
            fmt.decompress_file(path)
    
    def test_decompress_path(self):
        b = (True, False) if gz_path else (False,)
        for use_system in b:
            with self.subTest(use_system=use_system):
                path = self.root.make_file()
                gzfile = path + '.gz'
                with gzip.open(gzfile, 'wt') as o:
                    o.write('foo')
                fmt = get_format('.gz')
                dest = fmt.decompress_file(gzfile, use_system=use_system)
                self.assertEqual(dest, path)
                self.assertTrue(os.path.exists(path))
                self.assertTrue(os.path.exists(gzfile))
                with open(path, 'rt') as i:
                    self.assertEqual(i.read(), 'foo')
                
                path = self.root.make_file()
                gzfile = path + '.gz'
                with gzip.open(gzfile, 'wt') as o:
                    o.write('foo')
                fmt = get_format('.gz')
                dest = fmt.decompress_file(
                    gzfile, path, keep=False, use_system=use_system)
                self.assertEqual(dest, path)
                self.assertTrue(os.path.exists(path))
                self.assertFalse(os.path.exists(gzfile))
                with open(path, 'rt') as i:
                    self.assertEqual(i.read(), 'foo')
    
    def test_decompress_file(self):
        b = (True, False) if gz_path else (False,)
        for use_system in b:
            with self.subTest(use_system=use_system):
                path = self.root.make_file()
                gzfile = path + '.gz'
                with gzip.open(gzfile, 'wt') as o:
                    o.write('foo')
                with open(gzfile, 'rb') as i:
                    fmt = get_format('.gz')
                    dest = fmt.decompress_file(i, use_system=use_system)
                self.assertEqual(dest, path)
                self.assertTrue(os.path.exists(path))
                self.assertTrue(os.path.exists(gzfile))
                with open(path, 'rt') as i:
                    self.assertEqual(i.read(), 'foo')
                
                with gzip.open(gzfile, 'wt') as o:
                    o.write('foo')
                dest = self.root.make_file()
                with open(gzfile, 'rb') as i, open(dest, 'wb') as o:
                    fmt = get_format('.gz')
                    fmt.decompress_file(source=i, dest=o, use_system=use_system)
                self.assertTrue(os.path.exists(dest))
                self.assertTrue(os.path.exists(gzfile))
                with open(dest, 'rt') as i:
                    self.assertEqual(i.read(), 'foo')
                
                path = self.root.make_file()
                gzfile = path + '.bar'
                with gzip.open(gzfile, 'wt') as o:
                    o.write('foo')
                with open(gzfile, 'rb') as i:
                    fmt = get_format('.gz')
                    dest = fmt.decompress_file(
                        i, path, keep=False, use_system=use_system)
                self.assertEqual(dest, path)
                self.assertFalse(os.path.exists(gzfile))
                self.assertTrue(os.path.exists(path))
                with open(path, 'rt') as i:
                    self.assertEqual(i.read(), 'foo')

class StringTests(TestCase):
    def test_compress(self):
        for ext in ('.gz','.bz2','.xz'):
            with self.subTest(ext=ext):
                fmt = get_format(ext)
                bytes = random_text().encode()
                compressed = fmt.compress(bytes)
                decompressed = fmt.decompress(compressed)
                self.assertEqual(bytes, decompressed)
    
    def test_compress_string(self):
        for ext in ('.gz','.bz2','.xz'):
            with self.subTest(ext=ext):
                fmt = get_format(ext)
                text = random_text()
                compressed = fmt.compress_string(text)
                decompressed = fmt.decompress_string(compressed)
                self.assertEqual(text, decompressed)
    
    def test_compress_iterable(self):
        for ext in ('.gz','.bz2','.xz'):
            with self.subTest(ext=ext):
                fmt = get_format(ext)
                strings = ['line1', 'line2', 'line3']
                compressed = fmt.compress_iterable(strings, delimiter=b'|')
                decompressed = fmt.decompress_string(compressed)
                self.assertListEqual(strings, decompressed.split('|'))
        
