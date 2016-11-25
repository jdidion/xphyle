from setuptools import setup
import os
import sys

requirements = []

if sys.version_info < (3, 3):
    sys.stdout.write("At least Python 3.3 is required.\n")
    sys.exit(1)
elif sys.version_info < (3, 5):
    # typing was added in 3.5, but a backport is avaialble for 3.3+
    requirements.append('backports.typing')

import versioneer

setup(
    name='xphyle',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Utilities for working with files.',
    url='https://github.com/jdidion/xphyle',
    author='John Didion',
    author_email='john.didion@nih.gov',
    license='Public Domain',
    packages = ['xphyle'],
    install_requires = requirements,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: Public Domain',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
)
