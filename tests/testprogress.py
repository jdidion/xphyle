from unittest import TestCase
from . import *
import xphyle
from xphyle.paths import TempDir, STDOUT
from xphyle.utils import *

class MockProgress(object):
    def __call__(self, itr, desc, size):
        self.desc = desc
        self.size = size
        for i, item in enumerate(itr, 1):
            yield item
        self.count = i

class ProgressTests(TestCase):
    def setUp(self):
        self.root = TempDir()
    
    def tearDown(self):
        self.root.close()
    
    def test_progress(self):
        progress = MockProgress()
        xphyle.configure(progress)
        path = self.root.make_file()
        with open(path, 'wt') as o:
            for i in range(100):
                o.write(random_text())
        gzfile = compress_file(
            path, compression='gz', use_system=False)
        self.assertEqual(100, progress.count)
    
    def test_iter_stream(self):
        progress = MockProgress()
        xphyle.configure(progress)
        with intercept_stdin('foo\nbar\nbaz'):
            with xopen(STDIN, 'rt', context_wrapper=True, compression=False) as o:
                lines = list(o)
                self.assertListEqual(['foo\n','bar\n','baz\n'], lines)
        self.assertEquals(3, progress.count)
