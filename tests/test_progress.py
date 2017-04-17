from unittest import TestCase
from . import *
import xphyle
from xphyle.paths import TempDir
from xphyle.progress import ITERABLE_PROGRESS, PROCESS_PROGRESS
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
        xphyle.configure(progress=False, progress_wrapper=False)
    
    def tearDown(self):
        self.root.close()
        ITERABLE_PROGRESS.enabled = False
        ITERABLE_PROGRESS.wrapper = None
        PROCESS_PROGRESS.enabled = False
        PROCESS_PROGRESS.wrapper = None
    
    def test_progress(self):
        progress = MockProgress()
        xphyle.configure(progress=True, progress_wrapper=progress)
        path = self.root.make_file()
        with open(path, 'wt') as o:
            for i in range(100):
                o.write(random_text())
        compress_file(
            path, compression='gz', use_system=False)
        self.assertEqual(100, progress.count)
    
    def test_progress_delmited(self):
        progress = MockProgress()
        xphyle.configure(progress=True, progress_wrapper=progress)
        path = self.root.make_file()
        with open(path, 'wt') as o:
            for i in range(100):
                o.write('row\t{}\n'.format(i))
        rows = list(read_delimited(path))
        self.assertEqual(100, len(rows))
        self.assertEqual(100, progress.count)
    
    def test_iter_stream(self):
        progress = MockProgress()
        xphyle.configure(progress=True, progress_wrapper=progress)
        with intercept_stdin('foo\nbar\nbaz'):
            with xopen(STDIN, 'rt', context_wrapper=True, compression=False) as o:
                lines = list(o)
                self.assertListEqual(['foo\n','bar\n','baz\n'], lines)
        self.assertEquals(3, progress.count)
