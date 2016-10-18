from unittest import TestCase

from xphyle.paths import *
from . import *

class PathTests(TestCase):
    def test_check_access_std(self):
        check_access(STDOUT, 'r')
        check_access(STDOUT, 'w')
        check_access(STDERR, 'w')
        check_access(STDOUT, 'a')
        check_access(STDERR, 'a')
        
    def test_check_access_file(self):
        with make_file('rwx') as path:
            check_access(path, 'r')
            check_access(path, 'w')
            check_access(path, 'x')

    def test_no_access(self):
        with self.assertRaises(IOError):
            with make_file('r') as path:
                check_access(path, 'w')
