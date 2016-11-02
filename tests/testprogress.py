from unittest import TestCase
from . import *
import xphyle
from xphyle.utils import *

class MockProgress(object):
    def __call__(self, itr, size):
        for i, item in enumerate(itr, 1):
            yield item
        self.count = i

class ProgressTests(TestCase):
    def setUp(self):
        self.root = TestTempDir()
    
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
