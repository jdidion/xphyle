# xphyle: easier access to compressed files

xphyle is a small python (3.3+) library that makes it easy to open compressed
files for the highest-possible performance available on your system. It also
provides some convenience methods for working with file paths.

xphyle is organized as follows:

* The `xphyle` module (i.e. __init__.py) provides `open_`, a drop-in replacement for the python `open()` method that tries to automatically and transparently handle common compression and archive formats.
* The `xphyle.utils` module provides some useful methods on top of open_ for reading and writing files.
* The `xphyle.formats` module implements the details of different file formats. The goal of this module is to try to use the system-level program/library (which is generally the fastest) when possible, and fall back to a pure-python module.
* The `xphyle.paths` module offers useful functions for locating and resolving files and directories.

# Installation

```
pip install git+git://github.com/jdidion/xphyle.git
```

We are actively working towards 100% test coverage and completion of additional features (see 'TODO' below), at which point we will declare version 1.0 and add the project to pypi.

# Example usages:

```python
from xphyle import *

# Open a compressed file
myfile = xopen('infile.gz')

# Print all lines in a compressed file
with open_('infile.gz') as myfile:
    for line in myfile:
        print(line)

# Read all lines in a compressed file into a list
from xphyle.utils import safe_file_iter
lines = list(safe_iter('infile.gz'))

# Sum the rows in a compressed file where each line is an integer value
total = sum(i for i in safe_iter('infile.gz', convert=int))
```

# TODO

## Version 1.0

* LZW support (via python-lzw)
* Make all file I/O functions work transparently with URLs ()

## After 1.0

* Support multi-threaded gzip compression (via pigz)
* Support multi-threaded lzma compression
* Many other archive formats that might be supported - which are most important?
    * arc, cab (windows-specific), dmg (mac-specific), iso9660, lzh, rar, xar, zz
    * Snappy (via python-snappy): this is problematic since it depends on libsnappy, with no pure python fallback
    * Data type-specific formats (e.g. scalce for nucleotide sequences)
* Add support for archive formats
    * Recognize .tgz, .tbz2, and .tlz extensions
    * Support 7zip archives: this is problematic as it depends on 7zip being installed, with no pure python fallback
    * Optionally use libarchive (if installed) via one of the several available python packages. Will have to do performance testing to determine whether this should be the first option or the second to try.
