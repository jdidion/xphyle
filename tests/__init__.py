from contextlib import contextmanager
from io import BytesIO, TextIOWrapper
import random
import socket
from threading import Thread
import time
from unittest.mock import patch
import urllib.request

def random_text(n=1024):
    return ''.join(chr(random.randint(32, 126)) for i in range(n))

class MockStdout(object):
    def __init__(self, name, as_bytes):
        self.bytes_io = BytesIO()
        object.__setattr__(self.bytes_io, 'name', name)
        self.wrapper = TextIOWrapper(self.bytes_io)
        self.wrapper.mode = 'w'
        self.as_bytes = as_bytes
    
    def getvalue(self):
        self.wrapper.flush()
        val = self.bytes_io.getvalue()
        if not self.as_bytes:
            val = val.decode()
        return val

@contextmanager
def intercept_stdout(as_bytes=False):
    i = MockStdout('<stdout>', as_bytes)
    with patch('sys.stdout', i.wrapper):
        yield i

@contextmanager
def intercept_stderr(as_bytes=False):
    i = MockStdout('<stderr>', as_bytes)
    with patch('sys.stderr', i.wrapper):
        yield i

@contextmanager
def intercept_stdin(content, is_bytes=False):
    if not is_bytes:
        content = content.encode()
    i = BytesIO()
    object.__setattr__(i, 'name', '<stdin>')
    i.write(content)
    if not (is_bytes or content.endswith(b'\n')):
        i.write(b'\n')
    i.seek(0)
    i = TextIOWrapper(i)
    i.mode = 'r'
    with patch('sys.stdin', i):
        yield

def no_internet():
    """Test whether there's no internet connection available.
    """
    try:
        urllib.request.urlopen("https://github.com").info()
        return False
    except:
        return True

# A simple socket-based server that echoes back any sent data.
class EchoServer(Thread):
    def __init__(self, port=5555):
        super().__init__()
        self.port = port
    
    def kill(self):
        conn = socket.create_connection(('0.0.0.0', self.port))
        try:
            conn.send(b'quit')
        finally:
            conn.shutdown(1)
            conn.close()
    
    # work around bug in python 3.4 where connect will raise an error
    # if the server isn't immediately ready to accept the connection
    def start(self):
        super().start()
        time.sleep(1)
    
    def run(self, chunk_size=2048):
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        connection.bind(('0.0.0.0', self.port))
        connection.listen(10)
        while True:
            current_connection, address = connection.accept()
            try:
                while True:
                    data = current_connection.recv(chunk_size)
                    if data:
                        if data == b'quit':
                            return
                        else:
                            current_connection.send(data)
                    else:
                        break
            finally:
                try:
                    current_connection.shutdown(1)
                except:
                    pass
                current_connection.close()
