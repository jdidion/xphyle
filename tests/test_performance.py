"""Self-contained performance tests.
"""
from contextlib import contextmanager
import gzip
import time
from xphyle.utils import read_lines
from xphyle.paths import TempDir
import pytest

class TimeKeeper():
    def __init__(self, msg, **kwargs):
        self.msg = msg
        self.msg_args = kwargs
        self.duration = 0
        
    def __enter__(self):
        self.start = time.clock()
        return self
    
    def __exit__(self, exception_type, exception_value, traceback):
        self.stop = time.clock()
        self.duration = self.stop - self.start
        print(self.msg.format(
            duration=self.duration,
            **self.msg_args))

def perftest(name, text_generator, num_iter=10):
    # generate a big text
    msg = """
    Timing of {iter} {name} tests with total size {size:,d} characters and 
    use_system = {use_system}: {duration:0.2f} sec"""
    total_size = 0
    
    with TempDir() as root:
        paths = tuple(
            root.make_file(suffix='.gz')
            for _ in range(num_iter))
        for path in paths:
            txt = text_generator()
            total_size += len(txt)
            with gzip.open(path, 'wt') as out:
                out.write(txt)
        
        with TimeKeeper(
                msg, name=name, iter=num_iter, size=total_size, 
                use_system=None):
            for path in paths:
                list(gzip.open(path))
        
        for use_system in (True, False):
            with TimeKeeper(
                    msg, name=name, iter=num_iter, size=total_size, 
                    use_system=use_system):
                for path in paths:
                    list(read_lines(path, use_system=use_system))

@pytest.mark.perf
def test_lorem_ipsum():
    from lorem.text import TextLorem
    generate_lorem = TextLorem(prange=(500, 1000), trange=(500, 1000))
    return perftest('lorem ipsum', generate_lorem.text)

@pytest.mark.perf
def test_fastq():
    from random import randint, choices
    def generate_fastq(seqlen=100):
        num_records = randint(100000, 500000)
        qualspace = list(chr(i + 33) for i in range(60))
        rand_seq = lambda: "".join(choices(['A','C','G','T'], k=seqlen))
        rand_qual = lambda: "".join(choices(qualspace, k=seqlen))
        return "\n".join(
            "\n".join((
                "read{}".format(i),
                rand_seq(),
                '+',
                rand_qual()))
            for i in range(num_records))
    return perftest('fastq', generate_fastq)
