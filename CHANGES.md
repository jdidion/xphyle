v2.1.2 (dev)
------------

* Add caching for FileMode and PermissionSet

v2.1.1 (2017.02.13)
-------------------

* Minor bug fixes
* Code cleanup (thanks to Codacy)

v2.1.0 (2017.02.11)
-------------------

* Added support for opening buffer types.

v2.0.0 (2017.02.11)
-------------------
* The major version change reflects the introduction of potentially breaking changes:
    1. When a file object is passed to `open_`, it is now wrapped in a `FileLikeWrapper` by default. To avoid this behavior, set `wrap_fileobj=False`, but note that if the file-like object is not a context manager, an error will be raised.
    2. `xopen` no longer wraps files in `FileLikeWrapper` by default. To revert to the old behavior, set `xphyle.configure(default_xopen_context_wrapper=True)`.
    3. For several methods in the `xphyle.paths` module, the `mode` argument has been renamed to `access` to avoid ambiguity.
    4. `xphyle.paths.check_writeable_file` and `xphyle.paths.safe_check_writeable_file` have been changed to 'writable' to be consistent with the spelling used in core python.
    5. In the `xphyle.paths` module:
        * `check_file_mode` is removed.
        * `get_access` is renamed to `get_permissions`.
        * Many attribute and method names changed, mostly due to renaming of 'access' to 'permissions'.
    6. In the context of `FileInput`, `mode` parameters have been changed to `char_mode`.
    7. The `is_iterable` method has moved from `xphyle.utils` to `xphyle.types`.
    8. The `types` parameter of `xphyle.utils.find` is renamed to path_types.
    9. The string name of the FIFO path type has changed from 'fifo' to '|'.
* Added `xphyle.popen`, which opens subprocesses (i.e. `subprocess.Popen` instances) and uses `xopen` to open stdin/stdout/sterr files or wrap PIPEs. This enables sending compressed data to/reading compressed data from subprocesses without knowing in advance what the compression format will be or whether native compression/decompression programs are available.
* `xopen` now accepts two additional argument types: file objects and system commands. The later are specified as a string beginning with '|' (similar to the toolshed `nopen` method). PIPEs are automatically opened for stdin, stdout, and stderr. Additionally, if a compression type is specified, it is used to wrap one of the pipes as follows:
    * If mode is read or readwrite, `xopen` opens a PIPE to stdout.
    * Otherwise, `xopen` opens a PIPE to stdin.
* Enumerated types are now provided (in `xphyle.typing`) for all argument types in which fixed sets of strings were used previously (e.g. file open mode, path type). All methods with these argument types now accept either the string or Enum value.
