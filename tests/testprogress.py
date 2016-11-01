from unittest import TestCase
from . import *
import xphyle

class MockProgress(object):
    def __call__(self, itr, size):
        for i, item in enumerate(itr, 1):
            yield item
        self.count = i

class ProgressTests(TestCase):
    def test_progress(self):
        progress = MockProgress()
        xphyle.configure(progress)
        
