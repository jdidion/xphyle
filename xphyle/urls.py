# -*- coding: utf-8 -*-
"""Methods for handling URLs.
"""
import copy
import re
from urllib.request import urlopen, Request
from urllib.parse import urlparse

# URLs

def parse_url(s):
    """Attempts to parse a URL.
    
    Args:
        s: String to test
    
    Returns:
        A 6-tuple, as described in ``urlparse``, or  None if the URL cannot be
        parsed, or if it lacks a minimum set of attributes. Note that a URL may
        be valid and still not be openable (for example, if the scheme is
        recognized by urlopen).
    """
    url = urlparse(s)
    if not (url.scheme and (url.netloc or url.path)):
        return None
    return url

def open_url(url, byte_range=None, headers={}, **kwargs):
    """Open a URL for reading.
    
    Args:
        url: A valid url string
        byte_range: Range of bytes to read (start, stop)
        kwargs: Additional arguments to pass to ``urlopen``
    
    Returns:
        A response object, or None if the URL is not valid or cannot be opened
    """
    if byte_range:
        headers = copy.copy(headers)
        headers['Range'] = 'bytes={}-{}'.format(*byte_range)
    try:
        request = Request(url, headers=headers, **kwargs)
        return urlopen(request)
    except:
        return None

def get_url_mime_type(response):
    """If a response object has HTTP-like headers, extract the MIME type
    from the Content-Type header.
    """
    if hasattr(response, 'headers') and 'Content-Type' in response.headers:
        return response.headers['Content-Type']
    return None

content_disposition_re = re.compile('filename=([^;]+)')

def get_url_file_name(response, parsed_url=None):
    """If a response object has HTTP-like headers, extract the filename
    from the Content-Disposition header.
    """
    if hasattr(response, 'headers') and 'Content-Disposition' in response.headers:
        match = content_disposition_re.search(
            response.headers['Content-Disposition'])
        if match:
            return match.groups(1)
    if not parsed_url:
        parsed_url = parse_url(response.geturl())
    if parsed_url and hasattr(parsed_url, 'path'):
        return parsed_url.path
    return None
