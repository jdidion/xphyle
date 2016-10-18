# xphyle: easier access to compressed files

xphyle is a small python (3.3+) library that makes it easy to open compressed
files for the highest-possible performance available on your system. It also
provides some convenience methods for working with file paths.

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
lines = list(safe_file_iter('infile.gz'))

# Sum the rows in a compressed file where each line is an integer value
total = sum(i for i in safe_file_iter('infile.gz', convert=int))
```
