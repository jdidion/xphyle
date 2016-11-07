from unittest import TestCase, skipIf
import gzip
import os
from xphyle.formats import *
from xphyle.urls import *
from xphyle.paths import *
from . import *

good_url = 'https://github.com/jdidion/xphyle/blob/master/tests/foo.gz?raw=True'
bad_url = 'foo'

class TestURLs(TestCase):
    def test_parse(self):
        self.assertEqual(
            tuple(parse_url(good_url)),
            ('https', 'github.com',
             '/jdidion/xphyle/blob/master/tests/foo.gz',
             '', 'raw=True', ''))
        self.assertIsNone(parse_url(bad_url))
    
    def test_open_invalid(self):
        self.assertIsNone(open_url(bad_url))
    
    def test_get_url_file_name(self):
        with TempDir() as temp:
            path = abspath(temp.make_file(name='foo.txt'))
            url = open_url('file://' + path)
            self.assertEqual(get_url_file_name(url), path)
        # TODO: need to find a reliable compressed file URL with a
        # Content-Disposition, or figure out how to mock one up
    
    def test_mime_types(self):
        # TODO: need to find a reliable compressed file URL with a MIME type,
        # or figure out how to mock one up
        pass
