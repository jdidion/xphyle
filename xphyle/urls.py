# -*- coding: utf-8 -*-
"""Methods for handling URLs.

TODO: at the next major version change, this file should be renamed 
'protocols.py'.
"""
import copy
import io
import re
import socket
from http.client import HTTPResponse
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen, Request
from xphyle.types import Url, Range, Any, FileLike, cast

# URLs

def parse_url(url_string: str) -> Url:
    """Attempts to parse a URL.
    
    Args:
        s: String to test.
    
    Returns:
        A 6-tuple, as described in ``urlparse``, or  None if the URL cannot be
        parsed, or if it lacks a minimum set of attributes. Note that a URL may
        be valid and still not be openable (for example, if the scheme is
        recognized by urlopen).
    """
    url = urlparse(url_string)
    if not (url.scheme and (url.netloc or url.path)):
        return None
    return url

def open_url(
        url_string: str, byte_range: Range = None, headers: dict = None,
        **kwargs) -> Any:
    """Open a URL for reading.
    
    Args:
        url: A valid url string.
        byte_range: Range of bytes to read (start, stop).
        headers: dict of request headers.
        kwargs: Additional arguments to pass to `urlopen`.
    
    Returns:
        A response object, or None if the URL is not valid or cannot be opened.
    
    Notes:
        The return value of `urlopen` is only guaranteed to have
        certain methods, not to be of any specific type, thus the `Any`
        return type. Furthermore, the response may be wrapped in an
        `io.BufferedReader` to ensure that a `peek` method is available.
    """
    headers = copy.copy(headers) if headers else {}
    if byte_range:
        headers['Range'] = 'bytes={}-{}'.format(*byte_range)
    try:
        request = Request(url_string, headers=headers, **kwargs)
        response = urlopen(request)
        # HTTPResponse didn't have 'peek' until 3.5
        if response and not hasattr(response, 'peek'):
            # ISSUE: HTTPResponse inherits BufferedIOBase (rather than
            # RawIOBase), but for this purpose it's completely compatible 
            # with BufferedReader. Not sure how to make it type-compatible.
            return io.BufferedReader(cast(HTTPResponse, response))
        else:
            return response
        return response
    except (URLError, ValueError):
        return None

def get_url_mime_type(response: Any) -> str:
    """If a response object has HTTP-like headers, extract the MIME type
    from the Content-Type header.
    
    Args:
        response: A response object returned by `open_url`.
    
    Returns:
        The content type, or None if the response lacks a 'Content-Type' header.
    """
    if hasattr(response, 'headers') and 'Content-Type' in response.headers:
        return response.headers['Content-Type']
    return None

CONTENT_DISPOSITION_RE = re.compile('filename=([^;]+)')

def get_url_file_name(response: Any, parsed_url: Url = None) -> str:
    """If a response object has HTTP-like headers, extract the filename
    from the Content-Disposition header.
    
    Args:
        response: A response object returned by `open_url`.
        parsed_url: The result of calling `parse_url`.
    
    Returns:
        The file name, or None if it could not be determined.
    """
    if (hasattr(response, 'headers') and
            'Content-Disposition' in response.headers):
        match = CONTENT_DISPOSITION_RE.search(
            response.headers['Content-Disposition'])
        if match:
            return match.group(1)
    if not parsed_url:
        parsed_url = parse_url(response.geturl())
    if parsed_url and hasattr(parsed_url, 'path'):
        # ISSUE: ParseResult has named attributes that mypy does not
        # yet recognize
        #return parsed_url.path
        return parsed_url[2]
    return None

# Sockets

SOCKET_PROTOCOLS = dict(
    tcp=socket.SOCK_STREAM,
    udp=socket.SOCK_DGRAM
)

SOCKET_RE = re.compile(
    '(tcp|udp)?(?:<(?:(.*?):)?(\d+))?(?:>(?:(.*?):)?(\d+))?')
"""Regular expression to parse a socket spec, which has at least one and up to
three parts: protocol (tcp or udp), local address (which begins with '<'), and 
remote address (which begins with '>'). Addresses are specified as 'host:port'. 
For example:
    
    tcp<localhost:5555>google.com:8080
"""


class SocketInfo():
    def __init__(self, socket_parts):
        (
            self.protocol_name, self.local_host, self.local_port, 
            self.remote_host, self.remote_port) = socket_parts
        
        if self.protocol_name is None:
            self.protocol_name = 'tcp'
        self.protocol = SOCKET_PROTOCOLS[self.protocol_name]
        
        if self.local_port is not None:
            self.local_port = int(self.local_port)
        
        if self.remote_port is not None:
            self.remote_port = int(self.remote_port)
    
    @property
    def has_local_address(self):
        return self.local_host is not None and self.local_port is not None
    
    @property
    def has_remote_address(self):
        return self.remote_host is not None and self.remote_port is not None
    
    def __repr__(self):
        parts = [self.protocol_name]
        if self.local_host:
            parts.append("<{}:{}".format(self.local_host, self.local_port))
        if self.remote_host:
            parts.append(">{}:{}".format(self.remote_host, self.remote_port))
        return "".join(parts)


def parse_socket(socket_string):
    match = SOCKET_RE.match(socket_string)
    if match:
        socket_parts = match.groups()
        if any((socket_parts[2], socket_parts[4])):
            return SocketInfo(socket_parts)

def open_socket(socket_info: SocketInfo, mode: str) -> FileLike:
    """Opens a socket, opens a file-like interface to the socket,
    substitutes the file's close method for one that closes both the socket
    and the file, and returns the file.
    
    Args:
        socket_info: A parsed socket descriptor (the result of 
            xphyle.urls.parse_socket()).
        mode: The file mode.
    """
    sock = socket.socket(type=socket_info.protocol)
    if socket_info.has_local_address:
        sock.bind(
            (socket_info.local_host, socket_info.local_port))
    if socket_info.has_remote_address:
        sock.connect(
            (socket_info.remote_host, socket_info.remote_port))
    socket_file = sock.makefile(mode)
    socket_file_close = socket_file.close
    def close_socket():
        try:
            sock.shutdown(1)
        except:
            pass
        sock.close()
        socket_file_close()
    socket_file.close = close_socket
    return socket_file
