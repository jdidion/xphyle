from setuptools import setup
import sys

version_info = sys.version_info

if version_info < (3, 4):
    sys.stdout.write("At least Python 3.4 is required.\n")
    sys.exit(1)

install_requirements = []
test_requirements = ['pytest']

if version_info >= (3, 5):
    test_requirements.append('pytest-cov')

if version_info < (3, 6):
    # typing was added in 3.5, and we rely on critical features that were
    # introduced in 3.5.2+, so for versions older than 3.6 we rely on
    # a backport
    install_requirements.append('typing')

import versioneer

setup(
    name='xphyle',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Utilities for working with files.',
    url='https://github.com/jdidion/xphyle',
    author='John Didion',
    author_email='john.didion@nih.gov',
    license='MIT',
    packages = ['xphyle'],
    install_requires = install_requirements,
    extras_require = {
        'performance' : ['lorem']
    },
    tests_require = test_requirements,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'License :: Public Domain',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
