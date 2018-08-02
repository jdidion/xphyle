v4.0.0-rc1 (2018.08.02)
-----------------------
* Support non-.gz extensions when decompressing bgzip files.

v4.0.0-rc0 (2018.03.18)
-----------------------
* Starting with v4, xphyle requires python 3.6+
* All path-oriented functions now use pathlib paths by default. Support for string paths is deprecated.
* Moved to pokrok for progress bar management.

v3.1.6 (2018.01.16)
-------------------
* Fix bug when specifying file_type=FileType.FILELIKE.

v3.1.5 (2017.12.11)
-------------------
* Added `close_fileobj` parameters to `xopen()` to allow user to specify whether the file/buffer should be closed when the wrapper is closed.

v3.1.2 (2017.11.18)
------------------- 
* Added `xphyle.utils.uncompressed_size()`.

v3.1.1 (2017.10.13)
-------------------
* Added 'overwrite' parameter to xopen (defaults to True).

v3.1.0 (2017.08.31)
-------------------
* *Possible breaking change*: We discovered that python 3.3 support never fully worked due to some incompatibilities in the backported libraries for features we rely on that were introduced in 3.4. Thus, we are officially dropping support for python 3.3. This also reverts the change made in 3.0.7.
* Please ignore releases 3.0.8 and 3.0.9.

v3.0.7 (2017.07.22)
-------------------
* Add missing pathlib backport dependency for py3.3.

v3.0.6 (2017.07.22)
-------------------
* Added 'list_extensions' method to xphyle.formats.Formats.
* Fixed subtle bug that would cause failure when calling xopen on stdout that has been monkeypatched (as is done by pytest).

v3.0.5 (2017.07.19)
-------------------
* Fixed #13: opening corrupt gzip file fails silently.

v3.0.3 (2017.06.14)
-------------------
* Added basic performance testing.
* Fixed #12: xphyle not recognizing when system-level lzma not installed.

v3.0.2 (2017.05.23)
-------------------
* Forcing use of backports.typing for python < 3.6.

v3.0.1 (2017.04.29)
-------------------
* Added a paper for submission to JOSS.
* Enabled DOI generation using Zenodo.

v3.0.0 (2017.04.18)
-------------------
* Lots of fixes for bugs and type errors using mypy.
* Two breakting changes that necessitate the major version bump:
    * Several methods were erroneously named "uncompress_..." and have been corrected to "decompress_..."
    * Default values were erroneously used for the char_mode and linesep parameters of fileinput(), fileoutput(), FileInput, FileOutput, and all their subclasses. textinput(), textoutput(), byteinput(), and byteoutput() convenience methods were added, and default values were set to None.

v2.2.3 (2017.04.09)
-------------------

* Add get_compression_format_name() method to Formats.
* Validate the compression type in xopen.

v2.2.1 (2017.03.01)
-------------------

* Switch to pytest for testing.
* Bugfixes in fileoutput.
* Add ability to specifiy a file header for each file opened by fileoutput.
* Add ability to pass initializing text/bytes to xopen with file_type==BUFFER to create a readable buffer.

v2.2.0 (2017.02.17)
-------------------

* Add caching for FileMode and PermissionSet
* Add PatternFileOutput subclass of FileOuptut for generating output files from a pattern and tokens derived from lines in the file.

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
