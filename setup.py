from setuptools import setup
import os
import sys

if sys.version_info < (3, 3):
    sys.stdout.write("At least Python 3.3 is required.\n")
    sys.exit(1)

setup(
    name='xphyle',
    version='0.1',
    description='Utilities for working with files.',
    url='https://github.com/jdidion/xphyle',
    author='John Didion',
    author_email='john.didion@nih.gov',
    license='Public Domain',
    packages = ['xphyle'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Public Domain',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
)
