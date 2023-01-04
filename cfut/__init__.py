"""Python futures for Condor clusters."""
from concurrent import futures
from itertools import count
import os
import sys
import threading
import time
import traceback

from . import condor
from . import slurm
from .util import (
    random_string, local_filename, INFILE_FMT, OUTFILE_FMT,
)
import cloudpickle

__version__ = '0.5'

LOGFILE_FMT = local_filename('cfut.log.%s.txt')

class RemoteException(Exception):
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return '\n' + self.error.strip()

class JobDied(Exception):
    pass


class FileWaitThread(threading.Thread):
    """A thread that polls the filesystem waiting for a list of files to
    be created. When a specified file is created, it invokes a callback.
    """
    def __init__(self, callback, interval=1):
        """The callable ``callback`` will be invoked with value
        associated with the filename of each file that is created.
        ``interval`` specifies the polling rate.
        """
        threading.Thread.__init__(self, daemon=True)
        self.callback = callback
        self.interval = interval
        self.waiting = {}
        self.lock = threading.Lock()  # To protect the .waiting dict
        self.shutdown = False

    def stop(self):
        """Stop the thread soon."""
        self.shutdown = True

    def wait(self, filename, value):
        """Adds a new filename (and its associated callback value) to
        the set of files being waited upon.
        """
        with self.lock:
            self.waiting[filename] = value

    def run(self):
        for i in count():
            if self.shutdown:
                return

            with self.lock:
                self.check(i)
            time.sleep(self.interval)

    def check(self, i):
        """Do one check for completed jobs

        The i parameter allows subclasses like SlurmWaitThread to do something
        on every Nth check.
        """
        # Poll for each file.
        for filename in list(self.waiting):
            if os.path.exists(filename):
                self.callback(self.waiting[filename])
                del self.waiting[filename]


class ClusterExecutor(futures.Executor):
    """An abstract base class for executors that run jobs on clusters.
    """
    wait_thread_cls = FileWaitThread

    def __init__(self, debug=False, keep_logs=False):
        os.makedirs(local_filename(), exist_ok=True)
        self.debug = debug

        self.jobs = {}
        self.job_outfiles = {}
        self.jobs_lock = threading.Lock()
        self.jobs_empty_cond = threading.Condition(self.jobs_lock)
        self.keep_logs = keep_logs

        self.wait_thread = self.wait_thread_cls(self._completion)
        self.wait_thread.start()

    def _start(self, workerid, additional_setup_lines):
        """Start a job with the given worker ID and return an ID
        identifying the new job. The job should run ``python -m
        cfut.remote <workerid>.
        """
        raise NotImplementedError()

    def _cleanup(self, jobid):
        """Given a job ID as returned by _start, perform any necessary
        cleanup after the job has finished.
        """

    def _completion(self, jobid):
        """Called whenever a job finishes."""
        with self.jobs_lock:
            fut, workerid = self.jobs.pop(jobid)
            if not self.jobs:
                self.jobs_empty_cond.notify_all()
        if self.debug:
            print("job completed: %i" % jobid, file=sys.stderr)

        try:
            with open(OUTFILE_FMT % workerid, 'rb') as f:
                outdata = f.read()
        except FileNotFoundError:
            fut.set_exception(
                JobDied(f"Cluster job {jobid} finished without writing a result")
            )
        else:
            success, result = cloudpickle.loads(outdata)

            if success:
                fut.set_result(result)
            else:
                fut.set_exception(RemoteException(result))

            os.unlink(OUTFILE_FMT % workerid)

        # Clean up communication files.
        os.unlink(INFILE_FMT % workerid)

        self._cleanup(jobid)

    def submit(self, fun, *args, additional_setup_lines=None, **kwargs):
        """Submit a job to the pool.

        If additional_setup_lines is passed, it overrides the lines given
        when creating the executor.
        """
        fut = futures.Future()

        # Start the job.
        workerid = random_string()
        funcser = cloudpickle.dumps((fun, args, kwargs))
        with open(INFILE_FMT % workerid, 'wb') as f:
            f.write(funcser)
        jobid = self._start(workerid, additional_setup_lines)

        if self.debug:
            print("job submitted: %i" % jobid, file=sys.stderr)

        # Thread will wait for it to finish.
        self.wait_thread.wait(OUTFILE_FMT % workerid, jobid)

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

class SlurmWaitThread(FileWaitThread):
    slurm_poll_interval = 30

    def check(self, i):
        super().check(i)
        if i % (self.slurm_poll_interval // self.interval) == 0:
            try:
                finished_jobs = slurm.jobs_finished(self.waiting.values())
            except Exception:
                # Don't abandon completion checking if jobs_finished errors
                traceback.print_exc()
                return

            if not finished_jobs:
                return

            id_to_filename = {v: k for (k, v) in self.waiting.items()}
            for finished_id in finished_jobs:
                self.callback(finished_id)
                self.waiting.pop(id_to_filename[finished_id])


class SlurmExecutor(ClusterExecutor):
    """Futures executor for executing jobs on a Slurm cluster.

    additional_setup_lines is a list of lines to include in the shell script
    passed to sbatch. They may include sbatch options (starting with
    '#SBATCH') and shell commands, e.g. to set environment variables.
    """
    wait_thread_cls = SlurmWaitThread

    def __init__(self, debug=False, keep_logs=False, additional_setup_lines=(),
                 additional_import_paths=()):
        super().__init__(debug, keep_logs)
        self.additional_setup_lines = additional_setup_lines
        self.additional_import_paths = additional_import_paths

    def _start(self, workerid, additional_setup_lines):
        if additional_setup_lines is None:
            additional_setup_lines = self.additional_setup_lines
        if self.additional_import_paths:
            extra_path = ":".join(self.additional_import_paths)
        else:
            extra_path = "!"  # ! for nothing, because '' is valid (CWD)
        return slurm.submit(
            [sys.executable, '-m', 'cfut.remote', workerid, extra_path],
            additional_setup_lines=additional_setup_lines
        )

    def _cleanup(self, jobid):
        if self.keep_logs:
            return

        outf = slurm.OUTFILE_FMT.format(str(jobid))
        try:
            os.unlink(outf)
        except OSError:
            pass

class CondorExecutor(ClusterExecutor):
    """Futures executor for executing jobs on a Condor cluster."""
    def __init__(self, debug=False, keep_logs=False):
        super(CondorExecutor, self).__init__(debug, keep_logs)
        self.logfile = LOGFILE_FMT % random_string()

    def _start(self, workerid, additional_setup_lines):
        return condor.submit(sys.executable, '-m cfut.remote %s' % workerid,
                             log=self.logfile)

    def _cleanup(self, jobid):
        if self.keep_logs:
            return
        os.unlink(condor.OUTFILE_FMT % str(jobid))
        os.unlink(condor.ERRFILE_FMT % str(jobid))

    def shutdown(self, wait=True):
        super(CondorExecutor, self).shutdown(wait)
        if os.path.exists(self.logfile):
            os.unlink(self.logfile)

def map(executor, func, args, ordered=True):
    """Convenience function to map a function over cluster jobs. Given
    a function and an iterable, generates results. (Works like
    ``itertools.imap``.) If ``ordered`` is False, then the values are
    generated in an undefined order, possibly more quickly.
    """
    with executor:
        futs = []
        for arg in args:
            futs.append(executor.submit(func, arg))
        for fut in (futs if ordered else futures.as_completed(futs)):
            yield fut.result()
