![https://pypi.python.org/pypi/xphyle](https://img.shields.io/pypi/v/xphyle.svg?branch=master)
![https://travis-ci.org/jdidion/xphyle](https://travis-ci.org/jdidion/xphyle.svg?branch=master)
[![Coverage Status](https://img.shields.io/coveralls/jdidion/xphyle/master.svg)](https://coveralls.io/github/jdidion/xphyle?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b2c0baa52b604e39a09ed108ac2f53ee)](https://www.codacy.com/app/jdidion/xphyle?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=jdidion/xphyle&amp;utm_campaign=Badge_Grade)
[![Documentation Status](https://readthedocs.org/projects/xphyle/badge/?version=latest)](http://xphyle.readthedocs.io/en/latest/?badge=latest)
[![DOI](https://zenodo.org/badge/71260678.svg)](https://zenodo.org/badge/latestdoi/71260678)

# xphyle: extraordinarily simple file handling

<img src="https://github.com/jdidion/xphyle/blob/master/docs/logo.png?raw=true"
     alt="logo" width="200" height="200">

xphyle is a small python (3.3+) library that makes it easy to open compressed
files. Most importantly, xphyle will use the appropriate program (e.g. 'gzip') to compress/decompress a file if it is available on your system; this is almost always faster than using the corresponding python library. xphyle also provides methods that simplify common file I/O operations.

# Installation

```
pip install xphyle
```

If you are using python 3.3 or 3.4, this will install one dependency: [backports.typing](https://pypi.python.org/pypi/backports.typing).

# Building from source

Clone this repository and run

```
make
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

# Easily write to the stdin of a subprocess
with open_('|cat', 'wt') as process:
    process.write('foo')

# We have to tell xopen what kind of compression
# to use when writing to stdout
stdout = xopen(STDOUT, compression='gz')

# The `open_` method ensures that the file is usable with the `with` keyword.
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
from xphyle.utils import read_lines
lines = list(read_lines('infile.gz'))

# Sum the rows in a compressed file where each line is an integer value
total = sum(read_lines('infile.gz', convert=int))
```

See the [Documentation](http://xphyle.readthedocs.io/en/latest/?badge=latest) for full usage information.

# Roadmap

Future releases are mapped out using [GitHub Projects](https://github.com/jdidion/xphyle/projects).

# Developers

We welcome any contributions via pull requests.

All code must be written in idiomatic python 3. Note that we use [PEP484](https://www.python.org/dev/peps/pep-0484/) type hints. Variable annotations are
defined where needed using the comment syntax. Static code analysis is performed usying [mypy](http://mypy-lang.org/) and pylint.

Style-wise, we try to adhere to the Google python style guidelines. We use Google-style docstrings, which are formatted by the [Napoleon Sphinx Plugin](https://pypi.python.org/pypi/sphinxcontrib-napoleon). 

We enforce the [Contributor Covenant](http://contributor-covenant.org/) code of conduct.
