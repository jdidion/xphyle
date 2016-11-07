![https://pypi.python.org/pypi/xphyle](https://img.shields.io/pypi/v/xphyle.svg?branch=master)
![https://travis-ci.org/jdidion/xphyle](https://travis-ci.org/jdidion/xphyle.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/jdidion/xphyle/badge.svg?branch=master)](https://coveralls.io/github/jdidion/xphyle?branch=master)
[![Documentation Status](https://readthedocs.org/projects/xphyle/badge/?version=latest)](http://xphyle.readthedocs.io/en/latest/?badge=latest)

# xphyle: extraordinarily simple file handling

xphyle is a small python (3.3+) library that makes it easy to open compressed
files for the highest-possible performance available on your system. It also
provides some convenience methods for working with file paths.

# Installation

```
pip install xphyle
```

# Example usages:

```python
from xphyle import *
from xphyle.paths import STDIN

# Open a compressed file...
myfile = xopen('infile.gz')

# ...or a compressed stream
# e.g. gzip -c afile | python my_program.py
stdin = xopen(STDIN)

# We have to tell xopen what kind of compression
# to use when writing
stdout = xopen(STDOUT, compression='gz')

# Print all lines in a compressed file...
with open_('infile.gz') as myfile:
    for line in myfile:
        print(line)

# ... or a compressed URL
with open_('http://foo.com/myfile.gz') as myfile:
    for line in myfile:
        print(line)

# Transparently handle paths and file objects
def dostuff(path_or_file):
    with open_(path_or_file) as myfile:
        for line in myfile:
            print(line)

# Read all lines in a compressed file into a list
from xphyle.utils import safe_file_iter
lines = list(safe_iter('infile.gz'))

# Sum the rows in a compressed file where each line is an integer value
total = sum(i for i in safe_iter('infile.gz', convert=int))
```

See the [Documentation](https://xphyle.readthedocs.io) for full usage information.

# Roadmap

## 0.8

* Documentation

## 1.0

* Add support for archive formats
    * tar, zip
    * Recognize .tgz, .tbz2, and .tlz extensions

## Beyond

* Support other popular compression formats:
    * LZW: the only decent library is python-lzw, and it doesn't provide an open method
    * Snappy (via python-snappy): this is problematic since it depends on libsnappy, with no pure python fallback
* Support other popular archive formats
    * 7zip archives: this is problematic as it depends on 7zip being installed, with no pure python fallback
    * Consider using libarchive (if installed) via one of the several available python packages. Will have to do performance testing to determine whether this should be the first option or the second to try.
    * Which others? arc, cab (windows-specific), dmg (mac-specific), iso9660, lzh, rar, xar, zz
