import socket
from unittest import TestCase
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

class TestSockets(TestCase):
    def test_parse_socket(self):
        info = parse_socket('tcp<localhost:5555>google.com:8080')
        assert info.protocol_name == 'tcp'
        assert info.protocol == socket.SOCK_STREAM
        assert info.local_host == 'localhost'
        assert info.local_port == 5555
        assert info.remote_host == 'google.com'
        assert info.remote_port == 8080
        
        info = parse_socket('<localhost:5555>google.com:8080')
        assert info.protocol_name == 'tcp'
        assert info.protocol == socket.SOCK_STREAM
        assert info.local_host == 'localhost'
        assert info.local_port == 5555
        assert info.remote_host == 'google.com'
        assert info.remote_port == 8080
        
        info = parse_socket('<:5555')
        assert info.protocol_name == 'tcp'
        assert info.protocol == socket.SOCK_STREAM
        assert info.local_host == ''
        assert info.local_port == 5555
        assert info.remote_host is None
        assert info.remote_port is None
        
        info = parse_socket('udp>google.com:8080')
        assert info.protocol_name == 'udp'
        assert info.protocol == socket.SOCK_DGRAM
        assert info.local_host is None
        assert info.local_port is None
        assert info.remote_host == 'google.com'
        assert info.remote_port == 8080
    
    def test_open_socket(self):
        server = EchoServer()
        server.start()
        sock = open_socket(parse_socket('>:5555'), 'rw')
        try:
            sock.write('foo\n')
            sock.flush()
            assert sock.readline() == 'foo\n'
        finally:
            sock.close()
            server.kill()
