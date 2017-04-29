---
title: 'xphyle: Extraordinarily simple file handling'
tags:
  - io
  - files
  - python
authors:
 - name: John P Didion
   orcid: 0000-0002-8111-6261
   affiliation: 1
affiliations:
 - name: National Human Genome Research Institute, NIH, Bethesda, MD, USA
   index: 1
date: 29 April 2017
bibliography: paper.bib
---

# Summary

xphyle [@xphyle] is a small python (3.3+) library that makes it easy to open compressed files. Most importantly, xphyle will use the appropriate program (*e.g.* 'gzip') to compress/decompress a file if it is available on the host system; this is almost always faster than using the corresponding python library. xphyle also provides methods that simplify common file I/O operations.

# References
