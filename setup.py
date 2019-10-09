import codecs
import os
from setuptools import setup
import sys
import versioneer


version_info = sys.version_info
if version_info < (3, 6):
    sys.stdout.write(
        "xphyle 4+ requires python3.6. Use xphyle 3 with python 3.4 or 3.5.\n")
    sys.exit(1)


setup(
    name="xphyle",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Utilities for working with files.",
    long_description_content_type="text/markdown",
    long_description=codecs.open(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "README.md"
        ),
        "rb",
        "utf-8"
    ).read(),
    url="https://github.com/jdidion/xphyle",
    author="John Didion",
    author_email="github@didion.net",
    license="MIT",
    packages=["xphyle"],
    install_requires=["pokrok"],
    extras_require={
        "performance": ["lorem"],
        "zstd": ["zstandard"]
    },
    tests_require=["pytest", "pytest-cov"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "License :: Public Domain",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6"
    ]
)
