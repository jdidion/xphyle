"""Self-contained performance tests.
"""
from bisect import bisect
import gzip
from itertools import accumulate
from random import random, randint
import time
from xphyle.utils import read_lines
from xphyle.paths import TempDir
import pytest


class TimeKeeper:
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


def choices(population, weights=None, *, cum_weights=None, k=1):
    """Return a k sized list of population elements chosen with replacement.
    If the relative weights or cumulative weights are not specified,
    the selections are made with equal probability.

    This function is borrowed from the python 3.6 'random' package.
    """
    if cum_weights is None:
        if weights is None:
            _int = int
            total = len(population)
            return [population[_int(random() * total)] for _ in range(k)]
        cum_weights = list(accumulate(weights))
    elif weights is not None:
        raise TypeError('Cannot specify both weights and cumulative weights')
    if len(cum_weights) != len(population):
        raise ValueError('The number of weights does not match the population')
    total = cum_weights[-1]
    return [population[bisect(cum_weights, random() * total)] for _ in range(k)]


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
    def generate_fastq(seqlen=100):
        num_records = randint(100000, 500000)
        qualspace = list(chr(i + 33) for i in range(60))

        def rand_seq():
            return "".join(choices(['A', 'C', 'G', 'T'], k=seqlen))

        def rand_qual():
            return "".join(choices(qualspace, k=seqlen))

        return "\n".join(
            "\n".join((
                "read{}".format(i),
                rand_seq(),
                '+',
                rand_qual()))
            for i in range(num_records))
    return perftest('fastq', generate_fastq)
