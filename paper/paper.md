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

Data compression is commonly used to reduce the storage requirements for large datasets. It is often necessary for software that operates on big data to support several commonly used compression algorithms, including gzip, bzip2, and lzma. Handling these and other types of data sources, such as URLs and in-memory buffers, requires special consideration by software developers. We created xphyle [@xphyle], a small python (3.3+) library, to provide transparent access to files regardless of their source or compression type. Most importantly, xphyle uses the appropriate program (e.g. 'gzip') to compress/decompress a file if the program is available on the host system, which is generally faster than using the corresponding python library. xphyle also provides methods that simplify common file I/O operations.

# References
