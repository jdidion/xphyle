xphyle: extraordinarily simple file handling
============================================

.. image:: logo.png
   :height: 200px
   :width: 200 px

xphyle is a small python (3.3+) library that makes it easy to open compressed
files and URLs for the highest possible performance available on your system.

* `API <api/modules.html>`_
* `Source code <https://github.com/jdidion/xphyle/>`_
* `Report an issue <https://github.com/jdidion/xphyle/issues>`_

Installation
------------

xphyle is available from pypi::
    
    pip install xphyle

xphyle tries to use the compression programs installed on your local machine (e.g. gzip, bzip2); if it can't, it will use the built-in python libraries (which are slower). Thus, xphyle has no required dependencies, but we recommend that if you install gzip, etc. if you don't already have them.

xphyle will use `pigz <http://zlib.net/pigz/>`_ for multi-threaded gzip compression if it is available. Multithreading support is disabled by default; to set the number of threads that xphyle should use::

    xphyle.configure(threads=4)

or, to automatically set it to the number of cores available on your system::
    
    xphyle.configure(threads=True)

If you have programs installed at a location that is not on your path, you can add those locations to xphyle's executable search::

    xphyle.configure(executable_path=['/path', '/another/path', ...])

If you would like progress bars displayed for file operations, you need to configure one or both of the python-level and system-level progress bars.

For python-level operations, `tqdm <https://pypi.python.org/pypi/tqdm>`_ is used by default. To enable this::

    > pip install tqdm
    
    xphyle.configure(progress=True)

You can also use you own preferred progress bar by passing a callable, which must take a single iterable argument and two optional keyword arguments and return an iterable::

    def my_progress_wrapper(itr, desc='My progress bar', size=None):
        ...
    
    xphyle.configure(progress=my_progress_wrapper)

For system-level operations, an executable is required that reads from stdin and writes to stdout; `pv <http://www.ivarch.com/programs/quickref/pv.shtml>`_ is used by default. To enable this::
    
    xphyle.configure(system_progress=True)

You can also use your own preferred program by passing a tuple with the command and arguments (:py:func:`<xphyle.progress.system_progress_command>` simplifies this)::
    
    xphyle.configure(system_progress=xphyle.progress.system_progress_command(
        'pv', '-pre', require=True))

Working with files
------------------

The heart of xphyle is the simplicity of working with files. There is a single interface -- ``xopen`` -- for opening "file-like objects", regardless of whether they represent local files, remote files (referenced by URLs), or system streams (stdin, stdout, stderr); and regardless of whether they are compressed.

The following are functionally equivalent ways to open a gzip file::
    
    import gzip
    f = gzip.open('input.gz', 'rt')
    
    from xphyle import xopen
    f = xopen('input.gz', 'rt')

So then why use xphyle? Two reasons:

1. The ``gzip.open`` method of opening a gzip file above requires you to know that you are expecting a gzip file and only a gzip file. If your program optionally accepts either a compressed or an uncompressed file, then you'll need several extra lines of code to either detect the file format or to make the user specify the format of the file they are providing. This becomes increasingly cumbersome with each additional format you want to support. On the other hand, ``xopen`` has the same interface regardless of the compression format. Furthermore, if xphyle doesn't currently support a file format that you would like to use, it enables you to add it via a simple API.
2. The ``gzip.open`` method of opening a gzip file uses python code to uncompress the file. It's well written, highly optimized python code, but unfortunately it's still slower than your natively compiled system-level applications (e.g. pigz or gzip). The ``xopen`` method of opening a gzip file first tries to use pigz or gzip to uncompress the file and provides access to the resulting stream of uncompressed data (as a file-like object), and only falls back to ``gzip.open`` if neither program is available.

If you want to be explicit about whether to expect a compressed file, what type of compression to expect, or whether to try and use system programs, you can::
    
    from xphyle import xopen
    from xphyle.paths import STDIN
    
    # Expect the file to not be compressed
    f = xopen('input', 'rb', compression=False)
    
    # Open a remote file. Expect the file to be compressed, and throw an error
    # if it's not, or if the compression format cannot be determined.
    f = xopen('http://foo.com/input.gz', 'rt', compression=True)
    
    # Open stdin. Expect the input to be gzip compressed, and throw an error if
    # it's not
    f = xopen(STDIN, 'rt', compression='gzip')
    
    # Do not try to use the system-level gzip program for decompression
    f = xopen('input.gz', 'rt', compression='gzip', use_system=False)

By default, ``xopen`` wraps the returned file. This wrapper behaves just like a file, but also adds a few additional features:

* A file iterator is wrapped in a progress bar (if they have been enabled via the ``configure`` method described above).
* A simple event system that enables callbacks to be registered for various events. Currently, the only supported event is closing the file. The ``xphyle.utils`` package provides a few useful event listeners, e.g. to compress, move, or delete the file when it is closed.
* ContextManager functionality, such that the file is always compatible with ``with``, e.g.::
    
    def print_lines(path):
        # this works whether path refers to a local file, URL or STDIN
        with xopen(path) as infile:
            for line in infile:
                print(line)

The wrapping behavior can be disabled by passing ``context_wrapper=False`` to ``xopen``.

Another common pattern is to write functions that accept either a path or an open file object. Rather than having to test whether the user passed a path or a file and handle each differently, you can use the ``open_`` convenience method::
    
    from xphyle import open_
    
    def print_lines(path_or_file):
        with open_(path_or_file) as infile:
            for line in infile:
                print(line)


Reading/writing data
~~~~~~~~~~~~~~~~~~~~

The ``xphyle.utils`` module provides methods for many of the common operations that you'll want to perform on files. A few examples are shown below; you can read the `API docs <api/modules.html#module-xphyle.utils>`_ for a full list of methods and more detailed descriptions of each.

There are pairs of methods for reading/writing text and binary data using iterators::
    
    # Copy from one file to another, changing the line separator from
    # unix to windows
    from xphyle.utils import read_lines, write_lines
    write_lines(
        read_lines('linux_file.txt')
        'windows_file.txt',
        linesep='\r')
    
    # Copy from one binary file to another, changing the encoding from
    # ascii to utf-8
    from xphyle.utils import read_bytes, write_bytes
    def ascii2utf8(x):
        if isinstance(x, bytes):
            x = x.decode('ascii')
        return x.encode('utf-8')
    write_bytes(
        read_bytes('ascii_file.txt', convert=ascii2utf8),
        'utf8-file.txt')

There's another pair of methods for reading/writing key=value files::
    
    from xphyle.utils import read_dict, write_dict
    cats = dict(fluffy='calico', droopy='tabby', sneezy='siamese')
    write_dict(cats, 'cats.txt.gz')
    # change from '=' to '\t' delimited
    write_dict(
        read_dict(cats, 'cats.txt.gz'),
        'cats.tsv', sep='\t')

You can also read from delimited files such as csv and tsv::
    
    from xphyle.utils import read_delimited, read_delimited_as_dict
    class Dog(object):
        def __init__(self, name, age, breed):
            self.name = name
            self.age = age
            self.breed = breed
    for dog in read_delimited('dogs.txt.gz', header=True,
                              converters=(str,int,str),
                              row_type=Dog):
        dog.pet()
    
    dogs = read_delimited_as_dict('dogs.txt.gz', header=True,
                                  key='name', converters=(str,int,str),
                                  row_type=Dog):
    dogs['Barney'].say('Good Boy!')

There are convenience methods for compressing and uncompressing files::
    
    from xphyle.utils import compress_file, uncompress_file, transcode_file
    
    # Gzip compress recipes.txt, and delete the original
    compress_file('recipes.txt', compression='gzip', keep=False)
    
    # Uncompress a remote archive to a local file
    uncompress_file('http://recipes.com/allrecipes.txt.gz',
                    'local_recipes.txt')
    
    # Change from gzip to bz2 compression:
    transcode_file('http://recipes.com/allrecipes.txt.gz',
                   'local_recipes.txt.bz2')

There is a replacement for ``fileiinput``::
    
    from xphyle.utils import fileinput
    
    # By default, read from the files specified as command line arguments,
    # or stdin if there are no command line arguments, and autodetect
    # the compression format
    for line in fileinput():
        print(line)
    
    # Read from multiple files as if they were one
    for line in fileinput(('myfile.txt', 'myotherfile.txt.gz')):
        print(line)

There's also a set of classes for writing to multiple files::
    
    from xphyle.utils import fileoutput
    from xphyle.utils import TeeFileOutput, CycleFileOutput, NCycleFileOutput
    
    # write all lines in sourcefile.txt to both file1 and file2.gz
    with fileoutput(('file1', 'file2.gz'), type=TeeFileOutput) as out:
        out.writelines(read_lines('sourcefile.txt'))
    
    # Alternate writing each line in sourcefile.txt to file1 and file2.gz
    with fileoutput(('file1', 'file2.gz'), type=CycleFileOutput) as out:
        out.writelines(read_lines('sourcefile.txt'))
    
    # Alternate writing four lines in sourcefile.txt to file1 and file2.gz
    with fileoutput(('file1', 'file2.gz'), type=NCycleFileOutput, n=4) as out:
        out.writelines(read_lines('sourcefile.txt'))
    
    # Write up to 10,000 lines in each file before opening the next file
    with RollingFileOutput('file{}.gz', n=10000) as out:
        out.writelines(read_lines('sourcefile.txt'))
    
And finally, there's some miscelanenous methods such as linecount::
    
    from xphyle.utils import linecount
    print("There are {} lines in file {}".format(
        linecount(path), path))

File paths
~~~~~~~~~~

The ``xphyle.paths`` module provides methods for working with file paths. The `API docs <api/modules.html#module-xphyle.paths>`_ have a full list of methods and more detailed descriptions of each. Here are a few examples::
    
    from xphyle.paths import *
    
    # Get the absolute path, being smart about STDIN/STDOUT/STDERR and
    # home directory shortcuts
    abspath('/foo/bar/baz') # -> /foo/bar/baz
    abspath('foo') # -> /path/to/current/dir/foo
    abspath('~/foo') # -> /home/myname/foo
    abspath(STDIN) # -> STDIN

    # Splat a path into its component parts
    dir, name, *extensions = split_path('/home/joe/foo.txt.gz') # ->
        # dir = '/home/joe'
        # name = 'foo'
        # extensions = ['txt', 'gz']
    
    # Check that a path exists, is a file, and allows reading
    # Raises IOError if any of the expectations are violated,
    # otherwise returns the fully resolved path
    path = check_path('file.txt.gz', 'f', 'r')
    
    # Shortcuts to check whether a file is readable/writeable
    path = check_readable_file('file.txt')
    path = check_writeable_file('file.txt')
    
    # There are also 'safe' versions of the methods that return
    # None rather than raise IOError
    path = safe_check_readable_file('nonexistant_file.txt') # path = None
    
    # Find all files in a directory (recursively) that match a
    # regular expression pattern
    find('mydir', 'file.*\.txt\.gz')
    
    # Lookup the path to an executable
    gzip_path = get_executable_path('gzip')

`TempDir <api/modules.html#xphyle.paths.TempDir>`_ is a particularly useful class, especially for unit testing. In fact, it us used extensively for unit testing xphyle itself. TempDir can be thought of as a virtual file system. It creates a temporary directory, and it provides methods to create subdirectories and files within that directory. When the ``close()`` method is called, the entire temporary directory is deleted. ``TempDir`` can also be used as a ContextManager::
    
    with TempDir() as temp:
        # create three randomly named files under 'tempdir'
        paths = temp.make_empty_files(3)
        # create directory 'tempdir/foo'
        foo = temp.make_directory('foo')
        # create a randomly named file with the '.gz' suffix
        # within directory 'tempdir/foo'
        gzfile = temp[foo].make_file(suffix='.gz')

Extending xphyle
----------------

You can add support for another compression format by extending one of the base classes in :py:mod:`<xphyle.format>`::
    
    import xphyle.formats
    
    class FooFormat(xphyle.formats.SingleExeCompressionFormat):
        """Implementation of CompressionFormat for bzip2 files.
        """
        name = 'foo' # name of the python library
        exts = ('foo',) # file extension(s) (without the separator)
        system_commands = ('foo',) # name of the system command(s)
        compresslevel_range = (1, 9) # optional, if the level is configurable
        default_compresslevel = 6 # optional
        magic_bytes = ((0xB0, 0x0B, 0x55),) # format-specific header bytes
        mime_types = ('application/foo',) # mime type(s) for this format
        
        # build the system command
        # op = 'c' for compress, 'd' for uncompress
        # src = the source file, or STDIN if input should be read from stdin
        # stdout = True if output should be written to stdout
        # compresslevel = the compression level
        def get_command(self, op, src=STDIN, stdout=True, compresslevel=6):
            cmd = [self.executable_path]
            if op == 'c':
                # adjust the compresslevel to be within the range allowed
                # by the program
                compresslevel = self._get_compresslevel(compresslevel)
                cmd.append('-{}'.format(compresslevel))
                cmd.append('-z')
            elif op == 'd':
                cmd.append('-d')
            if stdout:
                cmd.append('-c')
            if src != STDIN:
                cmd.append(src)
            return cmd
        
        def open_file_python(self, filename, mode, **kwargs):
            # self.lib is a property that lazily imports and returns the
            # python library named in the ``name`` member above
            return self.lib.open_foo(filename, mode, **kwargs)

Then, register your format::

    xphyle.formats.register_compression_format(FooFormat)

Also, note that you can support custom URL schemes by the standard method of adding `urllib <https://docs.python.org/3/library/urllib.request.html#openerdirector-objects>`_ handlers::
    
    import urllib.request
    urllib.request.OpenerDirector.add_handler(my_handler)
