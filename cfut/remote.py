"""Tools for executing remote commands."""
from cloud import serialization
import sys
import random
import string

INFILE_FMT = 'cfut.in.%s.pickle'
OUTFILE_FMT = 'cfut.out.%s.pickle'

def random_string(length=32, chars=(string.ascii_letters + string.digits)):
    return ''.join(random.choice(chars) for i in range(length))

def worker(workerid):
    """Called to execute a job on a remote host."""
    try:
        with open(INFILE_FMT % workerid) as f:
            indata = f.read()
        fun, args, kwargs = serialization.deserialize(indata)

        result = True, fun(*args, **kwargs)
        out = serialization.serialize(result, True)

    except BaseException, exc:
        result = False, str(exc)
        out = serialization.serialize(result, False)

    with open(OUTFILE_FMT % workerid, 'w') as f:
        f.write(out)

if __name__ == '__main__':
    worker(*sys.argv[1:])
