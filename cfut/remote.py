"""Tools for executing remote commands."""
from cloud import serialization
import sys
import os
import traceback

INFILE_FMT = 'cfut.in.%s.pickle'
OUTFILE_FMT = 'cfut.out.%s.pickle'

def format_remote_exc():
    typ, value, tb = sys.exc_info()
    tb = tb.tb_next  # Remove root call to worker().
    return ''.join(traceback.format_exception(typ, value, tb))

def worker(workerid):
    """Called to execute a job on a remote host."""
    try:
        with open(INFILE_FMT % workerid) as f:
            indata = f.read()
        fun, args, kwargs = serialization.deserialize(indata)

        result = True, fun(*args, **kwargs)
        out = serialization.serialize(result, True)

    except:
        result = False, format_remote_exc()
        out = serialization.serialize(result, False)

    destfile = OUTFILE_FMT % workerid
    tempfile = destfile + '.tmp'
    with open(tempfile, 'w') as f:
        f.write(out)
    os.rename(tempfile, destfile)

if __name__ == '__main__':
    worker(*sys.argv[1:])
