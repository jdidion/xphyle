from setuptools import setup
import sys

requirements = []

if sys.version_info < (3, 3):
    sys.stdout.write("At least Python 3.3 is required.\n")
    sys.exit(1)
elif sys.version_info < (3, 6):
    # typing was added in 3.5, and we rely on critical features that were
    # introduced in 3.5.2+, so for versions older than 3.6 we rely on
    # a backport
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
    license='MIT',
    packages = ['xphyle'],
    install_requires = requirements,
    extras_require = {
        'performance' : ['lorem']
    },
    tests_require = ['pytest', 'pytest-cov', ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'License :: Public Domain',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.6',
    ],
)
