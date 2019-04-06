# -*- coding: utf-8 -*-
"""Methods for handling URLs.
"""
import copy
import io
import re
from typing import Optional
from http.client import HTTPResponse
from urllib.error import URLError
from urllib.parse import ParseResult, urlparse
from urllib.request import urlopen, Request
from xphyle.types import Range, Any, cast


# URLs


def parse_url(url_string: str) -> Optional[ParseResult]:
    """Attempts to parse a URL.
    
    Args:
        url_string: String to test.
    
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
    url_string: str,
    byte_range: Optional[Range] = None,
    headers: Optional[dict] = None,
    **kwargs
) -> Any:
    """Open a URL for reading.
    
    Args:
        url_string: A valid url string.
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
        headers["Range"] = "bytes={}-{}".format(*byte_range)
    try:
        request = Request(url_string, headers=headers, **kwargs)
        response = urlopen(request)
        # HTTPResponse didn't have 'peek' until 3.5
        if response and not hasattr(response, "peek"):
            # ISSUE: HTTPResponse inherits BufferedIOBase (rather than
            # RawIOBase), but for this purpose it's completely compatible
            # with BufferedReader. Not sure how to make it type-compatible.
            return io.BufferedReader(cast(HTTPResponse, response))
        else:
            return response
    except (URLError, ValueError):
        return None


def get_url_mime_type(response: Any) -> Optional[str]:
    """If a response object has HTTP-like headers, extract the MIME type
    from the Content-Type header.
    
    Args:
        response: A response object returned by `open_url`.
    
    Returns:
        The content type, or None if the response lacks a 'Content-Type' header.
    """
    if hasattr(response, "headers") and "Content-Type" in response.headers:
        return response.headers["Content-Type"]
    return None


CONTENT_DISPOSITION_RE = re.compile("filename=([^;]+)")


def get_url_file_name(
    response: Any, parsed_url: Optional[ParseResult] = None
) -> Optional[str]:
    """If a response object has HTTP-like headers, extract the filename
    from the Content-Disposition header.
    
    Args:
        response: A response object returned by `open_url`.
        parsed_url: The result of calling `parse_url`.
    
    Returns:
        The file name, or None if it could not be determined.
    """
    if hasattr(response, "headers") and "Content-Disposition" in response.headers:
        match = CONTENT_DISPOSITION_RE.search(response.headers["Content-Disposition"])
        if match:
            return match.group(1)
    if not parsed_url:
        parsed_url = parse_url(response.geturl())
    if parsed_url and hasattr(parsed_url, "path"):
        return parsed_url.path
    return None
