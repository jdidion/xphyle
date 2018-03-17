import sys


version_info = sys.version_info
if version_info < (3, 6):
    sys.stdout.write(
        "xphyle 4+ requires python3.6. Use xphyle 3 with python 3.4 or 3.5.\n")
    sys.exit(1)


from setuptools import setup
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
    install_requires = ['pokrok'],
    extras_require = {
        'performance' : ['lorem']
    },
    tests_require = ['pytest', 'pytest-cov'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'License :: Public Domain',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
    ],
)
