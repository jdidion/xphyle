xphyle: extraordinarily simple file handling
===================================

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

xphyle will use `pigz <http://zlib.net/pigz/>`_ for multi-threaded gzip compression if it is available. Multithreading is support disabled by default; to set the number of threads that xphyle should use::

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

You can also use your own preferred program by passing a tuple with the command and arguments (:ref:`<xphyle.progress.system_progress_command>` simplifies this)::
    
    xphyle.configure(system_progress=xphyle.progress.system_progress_command(
        'pv', '-pre', require=True)

Opening files
-------------

Other useful tools
------------------

Extending xphyle
----------------

You can add support for another compression format by extending one of the base classes in :ref:`<xphyle.format>`::
    
    class FooFormat(SingleExeCompressionFormat):
        """Implementation of CompressionFormat for bzip2 files.
        """
        name = 'foo' # name of the python library
        exts = ('foo',) # file extension(s) (without the separator)
        system_commands = ('foo',) # name of the system command(s)
        compresslevel_range = (1, 9) # optional, if the level is configurable
        default_compresslevel = 6 # optional
        magic_bytes = ((0xB0, 0x0B, 0x55),) # format-specific header bytes
        mime_types = ('application/foo',) # mime type(s) for this format
        
        def get_command(self, op, src=STDIN, stdout=True, compresslevel=6):
            cmd = [self.executable_path]
            if op == 'c':
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
            return self.lib.open_foo(filename, mode, **kwargs)

Then, register your format::

    xphyle.formats.register_compression_format(FooFormat)

Also, note that you can support custom URL schemes by the standard method of adding `urllib <https://docs.python.org/3/library/urllib.request.html#openerdirector-objects>`_ handlers::
    
    import urllib.request
    urllib.request.OpenerDirector.add_handler(my_handler)
