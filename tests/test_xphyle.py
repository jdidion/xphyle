from unittest import TestCase, skipIf
from . import *
import gzip
from io import BytesIO, IOBase
from xphyle import *
from xphyle.paths import TempDir, STDIN, STDOUT, STDERR, EXECUTABLE_CACHE
from xphyle.progress import ITERABLE_PROGRESS, PROCESS_PROGRESS
from xphyle.formats import THREADS
from xphyle.types import EventType


# Note: the casts of StringIO/BytesIO to IOBase are only necessary because of
# pycharm bug PY-28155


class XphyleTests(TestCase):
    def setUp(self):
        self.root = TempDir()

    def tearDown(self):
        self.root.close()
        ITERABLE_PROGRESS.enabled = False
        ITERABLE_PROGRESS.wrapper = None
        PROCESS_PROGRESS.enabled = False
        PROCESS_PROGRESS.wrapper = None
        THREADS.update(1)
        EXECUTABLE_CACHE.reset_search_path()
        EXECUTABLE_CACHE.cache = {}

    def test_configure(self):
        def wrapper(a, b, c) -> Iterable:
            return []

        configure(
            progress=True,
            progress_wrapper=wrapper,
            system_progress=True,
            system_progress_wrapper="foo",
            threads=2,
            executable_path=[Path("foo")],
        )

        assert wrapper == ITERABLE_PROGRESS.wrapper
        assert ("foo",) == PROCESS_PROGRESS.wrapper
        assert 2 == THREADS.threads
        assert Path("foo") in EXECUTABLE_CACHE.search_path

        configure(threads=False)
        assert 1 == THREADS.threads

        import multiprocessing

        configure(threads=True)
        assert multiprocessing.cpu_count() == THREADS.threads

    def test_guess_format(self):
        with self.assertRaises(ValueError):
            guess_file_format(STDOUT)
        with self.assertRaises(ValueError):
            guess_file_format(STDERR)
        path = self.root.make_file(suffix=".gz")
        with gzip.open(path, "wt") as o:
            o.write("foo")
        assert guess_file_format(path) == "gzip"
        path = self.root.make_file()
        with gzip.open(path, "wt") as o:
            o.write("foo")
        assert guess_file_format(path) == "gzip"

    def test_open_(self):
        path = self.root.make_file(contents="foo")
        with self.assertRaises(ValueError):
            with open_(path, wrap_fileobj=False):
                pass
        with open_(path, compression=False) as fh:
            assert fh.read() == "foo"
        with open_(path, compression=False) as fh:
            assert next(fh) == "foo"
        with open(path) as fh:
            with open_(fh, compression=False, context_wrapper=True) as fh2:
                self.assertTrue(isinstance(fh2, FileLikeWrapper))
                assert fh2.read() == "foo"
        with open(path) as fh3:
            with open_(fh, wrap_fileobj=False, context_wrapper=True):
                self.assertFalse(isinstance(fh3, FileLikeWrapper))

    def test_open_safe(self):
        with self.assertRaises(IOError):
            with open_("foobar", mode="r", errors=True) as _:
                pass
        with self.assertRaises(ValueError):
            with open_(cast(IOBase, None), mode="r", errors=True) as _:
                pass
        with open_("foobar", mode="r", errors=False) as fh:
            self.assertIsNone(fh)
        with open_(cast(IOBase, None), mode="r", errors=False) as fh:
            self.assertIsNone(fh)

    def test_xopen_invalid(self):
        # invalid mode
        with self.assertRaises(ValueError):
            xopen("foo", "z")
        with self.assertRaises(ValueError):
            xopen("foo", "rz")
        with self.assertRaises(ValueError):
            xopen("foo", "rU", newline="\n")
        with self.assertRaises(ValueError):
            xopen(STDOUT, "w", compression=True)
        with self.assertRaises(ValueError):
            xopen("foo.bar", "w", compression=True)
        with self.assertRaises(ValueError):
            xopen("foo", file_type=FileType.STDIO)
        with self.assertRaises(ValueError):
            xopen(STDOUT, file_type=FileType.LOCAL)
        with self.assertRaises(ValueError):
            xopen("foo", file_type=FileType.URL)
        with self.assertRaises(IOError):
            xopen("http://foo.com", file_type=FileType.LOCAL)
        with self.assertRaises(ValueError):
            xopen("xyz", file_type=FileType.FILELIKE)
        path = self.root.make_file(contents="foo")
        with open(path, "r") as fh:
            with self.assertRaises(ValueError):
                xopen(fh, "w")
            f = xopen(fh, context_wrapper=True)
            assert "r" == f.mode
        f = xopen(path, context_wrapper=True)
        f.close()
        with self.assertRaises(IOError):
            with f:
                pass
        with self.assertRaises(ValueError):
            with open(path, "rt") as fh:
                xopen(fh, "rt", compression=True)
        # can't guess compression without a name
        with self.assertRaises(ValueError):
            b = BytesIO()
            b.mode = "wb"
            xopen(cast(IOBase, b), "wt")
        # can't read from stderr
        with self.assertRaises(ValueError):
            xopen(STDERR, "rt")

    def test_xopen_std(self):
        # Try stdin
        with intercept_stdin("foo\n"):
            with xopen("-", "r", context_wrapper=True, compression=False) as i:
                content = i.read()
                assert content == "foo\n"
        with intercept_stdin("foo\n"):
            with xopen(STDIN, "r", context_wrapper=True, compression=False) as i:
                content = i.read()
                assert content == "foo\n"
        # Try stdout
        with intercept_stdout() as i:
            with xopen("-", "w", context_wrapper=True, compression=False) as o:
                o.write("foo")
            assert i.getvalue() == "foo"
        with intercept_stdout() as i:
            with xopen(STDOUT, "w", context_wrapper=True, compression=False) as o:
                o.write("foo")
            assert i.getvalue() == "foo"
        # Try stderr
        with intercept_stderr() as i:
            with xopen("_", "w", context_wrapper=True, compression=False) as o:
                o.write("foo")
            assert i.getvalue() == "foo"
        with intercept_stderr() as i:
            with xopen(STDERR, "w", context_wrapper=True, compression=False) as o:
                o.write("foo")
            assert i.getvalue() == "foo"

        # Try binary
        with intercept_stdout(True) as i:
            with xopen(STDOUT, "wb", context_wrapper=True, compression=False) as o:
                o.write(b"foo")
            assert i.getvalue() == b"foo"

        # Try compressed
        with intercept_stdout(True) as i:
            with xopen(STDOUT, "wt", context_wrapper=True, compression="gz") as o:
                assert cast(StdWrapper, o).compression == "gzip"
                o.write("foo")
            assert gzip.decompress(i.getvalue()) == b"foo"

    def test_xopen_compressed_stream(self):
        # Try autodetect compressed
        with intercept_stdin(gzip.compress(b"foo\n"), is_bytes=True):
            with xopen(STDIN, "rt", compression=True, context_wrapper=True) as i:
                assert cast(StdWrapper, i).compression == "gzip"
                assert i.read() == "foo\n"

    def test_xopen_file(self):
        with self.assertRaises(IOError):
            xopen("foobar", "r")
        path = self.root.make_file(suffix=".gz")
        with xopen(path, "rU", context_wrapper=True) as i:
            assert "rt" == i.mode
        with xopen(path, "w", compression=True, context_wrapper=True) as o:
            assert cast(FileLikeWrapper, o).compression == "gzip"
            o.write("foo")
        with gzip.open(path, "rt") as i:
            assert i.read() == "foo"
        with self.assertRaises(ValueError):
            with xopen(path, "rt", compression="bz2", validate=True):
                pass
        existing_file = self.root.make_file(contents="abc")
        with xopen(existing_file, "wt", overwrite=True) as out:
            out.write("def")
        with self.assertRaises(ValueError):
            with xopen(existing_file, "wt", overwrite=False):
                pass

    def test_xopen_fileobj(self):
        path = self.root.make_file(suffix=".gz")
        with open(path, "wb") as out1:
            with open_(out1, "wt") as out2:
                out2.write("foo")
            assert not out1.closed
        with gzip.open(path, "rt") as i:
            assert "foo" == i.read()

    def test_xopen_mmap(self):
        path = self.root.make_file(suffix=".gz")
        with xopen(
            path,
            "w",
            compression=True,
            context_wrapper=True,
            use_system=False,
            memory_map=True,
        ) as o:
            # since we are opening an empty file, memory mapping will fail
            assert not cast(FileWrapper, o).memory_mapped
            o.write("foo")
        with open(path, "rb") as inp:
            with xopen(
                inp,
                "r",
                compression=True,
                context_wrapper=True,
                use_system=False,
                memory_map=True,
            ) as i:
                assert cast(FileWrapper, i).memory_mapped
                assert i.read() == "foo"

    def test_xopen_buffer(self):
        buf = BytesIO(b"foo")
        f = xopen(cast(IOBase, buf), "rb")
        assert b"foo" == f.read(3)
        with self.assertRaises(ValueError):
            xopen(cast(IOBase, buf), "wb")

        with open_(str) as buf:
            buf.write("foo")
        assert "foo" == buf.getvalue()

        with open_(bytes) as buf:
            buf.write(b"foo")
        assert b"foo" == buf.getvalue()

        # with compression
        with self.assertRaises(ValueError):
            with open_(bytes, compression=True):
                pass
        with self.assertRaises(ValueError):
            with open_(str, compression="gzip"):
                pass

        with open_(bytes, mode="wt", compression="gzip") as buf:
            buf.write("foo")
        assert b"foo" == gzip.decompress(buf.getvalue())

        # from string/bytes
        with self.assertRaises(ValueError):
            xopen("foo", "wt", file_type=FileType.BUFFER)
        with self.assertRaises(ValueError):
            xopen("foo", "rb", file_type=FileType.BUFFER)
        with open_("foo", file_type=FileType.BUFFER, context_wrapper=True) as buf:
            assert "foo" == buf.read()

        with self.assertRaises(ValueError):
            xopen(b"foo", "rt", file_type=FileType.BUFFER)
        with open_(b"foo", file_type=FileType.BUFFER, context_wrapper=True) as buf:
            assert b"foo" == buf.read()

    @skipIf(no_internet(), "No internet connection")
    def test_xopen_url(self):
        badurl = "http://google.com/__badurl__"
        with self.assertRaises(ValueError):
            xopen(badurl)
        url = "https://github.com/jdidion/xphyle/blob/master/tests/foo.gz?raw=True"
        with self.assertRaises(ValueError):
            xopen(url, "w")
        with open_(url, "rt") as i:
            assert "gzip" == i.compression
            assert "foo\n" == i.read()

    def test_open_process(self):
        with self.assertRaises(ValueError):
            xopen("|cat", "wt", allow_subprocesses=False)
        with open_("|cat", "wt") as p:
            p.write("foo\n")
        assert b"foo\n" == p.stdout

    def test_peek(self):
        path = self.root.make_file()
        with self.assertRaises(IOError):
            with open_(path, "w") as o:
                o.peek()
        path = self.root.make_file(contents="foo")
        with open_(path, "rb") as i:
            assert b"f" == i.peek(1)
            assert b"foo" == next(i)
        with open_(path, "rt") as i:
            assert "f" == i.peek(1)
            assert "foo" == next(i)
        with intercept_stdin("foo"):
            with open_(STDIN, validate=False, compression=False) as i:
                assert "f" == i.peek(1)
                assert "foo\n" == next(i)

    def test_seek(self):
        path = self.root.make_file(contents="foo")
        with open_(path, "rb") as i:
            i.seek(1)
            assert b"o" == i.peek(1)

    def test_truncate(self):
        path = self.root.make_file(contents="foo")
        with open_(path, "r+") as i:
            i.truncate(1)
            assert i.read() == "f"

    def test_event_listeners(self):
        class MockEventListener(EventListener):
            def __init__(self):
                super().__init__()
                self.executed = False

            def execute(self, file_wrapper: FileLikeWrapper, **kwargs):
                self.executed = True

        std_listener: MockEventListener = MockEventListener()
        with intercept_stdin("foo"):
            f = xopen(STDIN, context_wrapper=True)
            try:
                cast(EventManager, f).register_listener(EventType.CLOSE, std_listener)
            finally:
                f.close()
            self.assertTrue(std_listener.executed)

        file_listener: MockEventListener = MockEventListener()
        path = self.root.make_file()
        f = xopen(path, "w", context_wrapper=True)
        try:
            cast(EventManager, f).register_listener(EventType.CLOSE, file_listener)
        finally:
            f.close()
        self.assertTrue(file_listener.executed)

    def test_process(self):
        with Process("cat", stdin=PIPE, stdout=PIPE, stderr=PIPE) as p:
            self.assertIsNotNone(p.get_writer())
            self.assertIsNotNone(p.get_reader("stdout"))
            self.assertIsNotNone(p.get_reader("stderr"))
            self.assertFalse(p.seekable())
            assert (p.stdout, p.stderr) == p.get_readers()
            p.write(b"foo\n")
            p.flush()
        assert b"foo\n" == p.stdout
        self.assertFalse(p.stderr)

        # wrap pipes
        with Process(("zcat", "-cd"), stdin=PIPE, stdout=PIPE) as p:
            self.assertTrue(p.readable())
            self.assertTrue(p.writable())
            with self.assertRaises(ValueError):
                p.is_wrapped("foo")
            with self.assertRaises(ValueError):
                p.wrap_pipes(foo=dict(mode="wt"))
            p.wrap_pipes(stdin=dict(mode="wt", compression="gzip"))
            self.assertTrue(p.is_wrapped("stdin"))
            p.write("foo")
        assert b"foo" == p.stdout

    def test_process_with_files(self):
        inp = self.root.make_file(suffix=".gz")
        with gzip.open(inp, "wt") as o:
            o.write("foo")
        out = self.root.make_file(suffix=".gz")
        with self.assertRaises(OSError):
            with gzip.open(inp, "rt") as o, open(out, "wt") as i:
                with Process("cat", stdin=o, stdout=i) as p:
                    p.wrap_pipes(stdin=dict(mode="wt"))
        with gzip.open(out, "rt") as i:
            assert "foo" == i.read()
        with popen(("echo", "abc\n123"), stdout=PIPE) as p:
            self.assertListEqual([b"abc\n", b"123\n"], list(line for line in p))
        with popen(("echo", "abc\n123"), stdout=PIPE) as p:
            assert b"abc\n" == next(p)
            assert b"123\n" == next(p)
        with popen(("echo", "abc\n123"), stdout=(PIPE, "rt")) as p:
            assert "abc\n" == next(p)
            assert "123\n" == next(p)

    def test_process_invalid(self):
        with self.assertRaises(ValueError):
            xopen("|cat", "wt", compression=True)

    def test_process_read(self):
        with Process(("echo", "foo"), stdout=PIPE) as p:
            assert b"foo\n" == p.read()
        with open_("|echo foo", "rt") as p:
            assert "foo\n" == p.read()

    def test_process_communicate(self):
        with Process("cat", stdin=PIPE, stdout=PIPE, stderr=PIPE) as p:
            self.assertTupleEqual((b"foo\n", b""), p.communicate(b"foo\n"))

    def test_process_del(self):
        class MockProcessListener(EventListener):
            def __init__(self):
                super().__init__()
                self.executed = False

            def execute(self, process: Process, **kwargs) -> None:
                self.executed = True

        listener: MockProcessListener = MockProcessListener()
        p = Process("cat", stdin=PIPE, stdout=PIPE)
        p.register_listener(EventType.CLOSE, listener)
        del p
        self.assertTrue(listener.executed)

    def test_process_close(self):
        p = Process("cat", stdin=PIPE, stdout=PIPE)
        self.assertFalse(p.closed)
        p.close()
        self.assertTrue(p.closed)
        self.assertIsNone(p.close1(raise_on_error=False))
        with self.assertRaises(IOError):
            p.close1(raise_on_error=True)

    def test_process_close_hung(self):
        p = Process(("sleep", "5"))
        with self.assertRaises(Exception):
            p.close1(timeout=1, terminate=False)
        p = Process(("sleep", "5"))
        p.close1(timeout=1, terminate=True)
        self.assertTrue(p.closed)

    def test_process_error(self):
        p = popen(("exit", "2"), shell=True)
        with self.assertRaises(IOError):
            p.close1(raise_on_error=True)
        self.assertFalse(p.returncode == 0)
