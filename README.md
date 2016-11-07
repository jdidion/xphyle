![https://pypi.python.org/pypi/xphyle](https://img.shields.io/pypi/v/xphyle.svg?branch=master)
![https://travis-ci.org/jdidion/xphyle](https://travis-ci.org/jdidion/xphyle.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/jdidion/xphyle/badge.svg?branch=master)](https://coveralls.io/github/jdidion/xphyle?branch=master)
[![Documentation Status](https://readthedocs.org/projects/xphyle/badge/?version=latest)](http://xphyle.readthedocs.io/en/latest/?badge=latest)

# xphyle: extraordinarily simple file handling

<img src="https://github.com/jdidion/xphyle/blob/master/docs/logo.png?raw=true"
     alt="logo" width="200" height="200">

xphyle is a small python (3.3+) library that makes it easy to open compressed
files. Most importantly, xphyle will use the appropriate program (e.g. 'gzip') to compress/uncompress a file if it is available on your system; this is almost always faster than using the corresponding python library. xphyle also provides methods that simplify common file I/O operations.

# Installation

```
pip install xphyle
```

# Example usages:

```python
from xphyle import *
from xphyle.paths import STDIN, STDOUT

# Open a compressed file...
myfile = xopen('infile.gz')

# ...or a compressed stream
# e.g. gzip -c afile | python my_program.py
stdin = xopen(STDIN)

# We have to tell xopen what kind of compression
# to use when writing to stdout
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
from xphyle.utils import safe_iter
lines = list(safe_iter('infile.gz'))

# Sum the rows in a compressed file where each line is an integer value
total = sum(safe_iter('infile.gz', convert=int))
```

See the [Documentation](http://xphyle.readthedocs.io/en/latest/?badge=latest) for full usage information.

# Roadmap

The xphyle public API is now set. The 1.0 release is imminent, pending completion of the user documentation. Future releases will be mapped using [GitHub Projects](https://github.com/jdidion/xphyle/projects).
