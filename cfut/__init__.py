"""Python futures for Condor clusters."""
from concurrent import futures
import os
import sys
import threading
from . import condor
from .remote import INFILE_FMT, OUTFILE_FMT, random_string
from cloud import serialization

LOGFILE_FMT = 'cfut.log.%s.txt'

class RemoteException(object):
    pass

class CondorExecutor(futures.Executor):
    """Futures executor for executing jobs on a Condor cluster."""
    def __init__(self, debug=False):
        self.debug = debug

        self.logfile = LOGFILE_FMT % random_string()
        self.jobs = {}
        self.jobs_lock = threading.Lock()
        self.jobs_empty_cond = threading.Condition(self.jobs_lock)

        self.wait_thread = condor.WaitThread(self._completion, self.logfile)
        self.wait_thread.start()

    def _completion(self, jobid):
        """Called whenever a job finishes."""
        with self.jobs_lock:
            fut, workerid = self.jobs.pop(jobid)
            if not self.jobs:
                self.jobs_empty_cond.notify_all()
        if self.debug:
            print >>sys.stderr, "job completed: %i" % jobid

        with open(OUTFILE_FMT % workerid) as f:
            outdata = f.read()
        success, result = serialization.deserialize(outdata)

        if success:
            fut.set_result(result)
        else:
            fut.set_exception(RemoteException(result))

        # Clean up communication files.
        os.unlink(INFILE_FMT % workerid)
        os.unlink(OUTFILE_FMT % workerid)
        # Clean up Condor stream files.
        os.unlink(condor.OUTFILE_FMT % str(jobid))
        os.unlink(condor.ERRFILE_FMT % str(jobid))
    
    def submit(self, fun, *args, **kwargs):
        """Submit a job to the pool."""
        fut = futures.Future()

        # Start the job.
        workerid = random_string()
        funcser = serialization.serialize((fun, args, kwargs), True)
        with open(INFILE_FMT % workerid, 'w') as f:
            f.write(funcser)
        jobid = condor.submit(sys.executable, '-m cfut.remote %s' % workerid,
                              log=self.logfile)

        if self.debug:
            print >>sys.stderr, "job submitted: %i" % jobid

        # Thread will wait for it to finish.
        self.wait_thread.wait(jobid)

        with self.jobs_lock:
            self.jobs[jobid] = (fut, workerid)
        return fut

    def shutdown(self, wait=True):
        """Close the pool."""
        if wait:
            with self.jobs_lock:
                if self.jobs:
                    self.jobs_empty_cond.wait()

        self.wait_thread.stop()
        self.wait_thread.join()
        if os.path.exists(self.logfile):
            os.unlink(self.logfile)

def map(func, args, ordered=True, debug=False):
    """Convenience function to map a function over cluster jobs. Given
    a function and an iterable, generates results. (Works like
    ``itertools.imap``.) If ``ordered`` is False, then the values are
    generated in an undefined order, possibly more quickly.
    """
    with CondorExecutor(debug) as etor:
        futs = []
        for arg in args:
            futs.append(etor.submit(func, arg))
        for fut in (futs if ordered else futures.as_completed(futs)):
            yield fut.result()
