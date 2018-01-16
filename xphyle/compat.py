import functools
import os
from pathlib import Path
from typing import Callable, Union, Sequence, Optional
import warnings


BACKCOMPAT = os.getenv('XPHYLE_BACKCOMPAT') != '0'
"""Whether backward compatibility is enabled. By default, backward compatibility
is enabled unless environment variable XPHYLE_BACKCOMPAT is set to '0'.
"""


IndexOrName = Union[int, str]


def deprecated_str_to_path(
        *args_to_convert: IndexOrName,
        list_args: Optional[Sequence[IndexOrName]] = None,
        dict_args: Optional[Sequence[IndexOrName]] = None) -> Callable:
    """Decorator for a function that used to take paths as strings and now only
    takes them as os.PathLike objects. A deprecation warning is issued, and
    the string arguments are converted to paths before calling the function.

    Backward compatibility can be disabled by the XPHYLE_BACKCOMPAT environment
    variable. If set to false (0), the `func` is returned immediately.
    """
    def decorate(func: Callable):
        if not BACKCOMPAT:
            return func

        @functools.wraps(func)
        def new_func(*args, **kwargs):
            warn = False
            new_args = list(args)
            for idx in args_to_convert:
                if isinstance(idx, int):
                    if len(args) >= idx and isinstance(args[idx], str):
                        warn = True
                        new_args[idx] = Path(args[idx])
                elif (
                        isinstance(idx, str) and
                        idx in kwargs and
                        isinstance(kwargs[idx], str)):
                    warn = True
                    kwargs[idx] = Path(kwargs[idx])
                else:
                    raise ValueError("'args_to_convert' must be ints or strings")
            if list_args is not None:
                for idx in list_args:
                    if isinstance(idx, int):
                        if len(args) >= idx and isinstance(args[idx], list):
                            warn = True
                            new_args[idx] = list(Path(s) for s in args[idx])
                        elif (
                                isinstance(idx, str) and
                                idx in kwargs and
                                isinstance(kwargs[idx], list)):
                            warn = True
                            kwargs[idx] = list(Path(s) for s in kwargs[idx])
                    else:
                        raise ValueError(
                            "'args_to_convert' must be ints or strings")
            if dict_args is not None:
                for idx in dict_args:
                    if isinstance(idx, int):
                        if len(args) >= idx and isinstance(args[idx], dict):
                            warn = True
                            new_args[idx] = dict(
                                (key, Path(val))
                                for key, val in args[idx].items())
                        elif (
                                isinstance(idx, str) and
                                idx in kwargs and
                                isinstance(kwargs[idx], dict)):
                            warn = True
                            kwargs[idx] = dict(
                                (key, Path(val))
                                for key, val in kwargs[idx].items())
                    else:
                        raise ValueError(
                            "'args_to_convert' must be ints or strings")
            if warn:
                deprecated(
                    f"Use of {func.__name__} with string path arguments is "
                    f"deprected")
            return func(*new_args, **kwargs)

        return new_func

    return decorate


def deprecated(msg: str):
    """Issue a deprecation warning:

    Args:
        msg: The warning message to display.
    """
    warnings.simplefilter('always', DeprecationWarning)  # turn off filter
    warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
    warnings.simplefilter('default', DeprecationWarning)  # reset filter
