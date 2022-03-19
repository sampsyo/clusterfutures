"""Tools for executing remote commands."""
import cloudpickle
import sys
import os
import traceback
from .util import INFILE_FMT, OUTFILE_FMT

def format_remote_exc():
    typ, value, tb = sys.exc_info()
    tb = tb.tb_next  # Remove root call to worker().
    return ''.join(traceback.format_exception(typ, value, tb))

def worker(workerid, extra_import_paths="!"):
    """Called to execute a job on a remote host."""
    print("worker")
    if extra_import_paths != '!':
        extra_import_paths = extra_import_paths.split(':')
        print("Prepending %d paths to sys.path:" % len(extra_import_paths))
        for p in extra_import_paths:
            print(" ", p)
        sys.path[:0] = extra_import_paths

    try:
        with open(INFILE_FMT % workerid, 'rb') as f:
            indata = f.read()
        fun, args, kwargs = cloudpickle.loads(indata)
        result = True, fun(*args, **kwargs)
        out = cloudpickle.dumps(result)

    except Exception as e:
        print(traceback.format_exc())

        result = False, format_remote_exc()
        out = cloudpickle.dumps(result)

    destfile = OUTFILE_FMT % workerid
    tempfile = destfile + '.tmp'
    with open(tempfile, 'wb') as f:
        f.write(out)
    os.rename(tempfile, destfile)

if __name__ == '__main__':
    worker(*sys.argv[1:])
