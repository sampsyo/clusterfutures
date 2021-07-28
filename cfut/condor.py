"""Abstracts access to a Condor cluster via its command-line tools.
"""
import re
import os
import threading
import time
from .util import call, chcall, local_filename

LOG_FILE = local_filename("condorpy.log")
OUTFILE_FMT = local_filename("condorpy.stdout.%s.log")
ERRFILE_FMT = local_filename("condorpy.stderr.%s.log")


def submit_text(job):
    """Submits a Condor job represented as a job file string. Returns
    the cluster ID of the submitted job.
    """
    out, _ = chcall('condor_submit -v', job.encode('utf-8'))
    jobid = re.search(rb'Proc (\d+)\.0', out).group(1)
    return int(jobid)

def submit(executable, arguments=None, universe="vanilla", log=LOG_FILE,
           outfile = OUTFILE_FMT % "$(Cluster)",
           errfile = ERRFILE_FMT % "$(Cluster)"):
    """Starts a Condor job based on specified parameters. A job
    description is generated. Returns the cluster ID of the new job.
    """
    descparts = [
        "Executable = %s" % executable,
        "Universe = %s" % universe,
        "Log = %s" % log,
        "output = %s" % outfile,
        "error = %s" % errfile,
    ]
    if arguments:
        descparts.append("Arguments = %s" % arguments)
    descparts.append("Queue")

    desc = "\n".join(descparts)
    return submit_text(desc)

def submit_script(script, **kwargs):
    """Like ``submit`` but takes the text of an executable script that
    should be used instead of a filename. Returns the cluster ID along
    with the name of the temporary script file executed. (This should
    probably be removed once the job completes.)
    """
    filename = local_filename('condorpy.jobscript.%s')
    with open(filename, 'w') as f:
        f.write(script)
    os.chmod(filename, 0o755)
    return submit(filename, **kwargs), filename

def wait(jobid, log=LOG_FILE):
    """Waits for a cluster (or specific job) to complete."""
    call("condor_wait %s %s" % (LOG_FILE, str(jobid)))

def getoutput(jobid, log=LOG_FILE, cleanup=True):
    """Waits for a job to complete and then returns its standard output
    and standard error data if the files were given default names.
    Deletes these files after reading them if ``cleanup`` is True.
    """
    wait(jobid, log)
    outfile = OUTFILE_FMT % str(jobid)
    errfile = ERRFILE_FMT % str(jobid)

    with open(outfile) as f:
        stdout = f.read()
    with open(errfile) as f:
        stderr = f.read()

    if cleanup:
        os.unlink(outfile)
        os.unlink(errfile)

    return stdout, stderr

class WaitThread(threading.Thread):
    """A worker that polls Condor log files to observe when jobs
    finish. Each cluster is only waited upon once (after which it is
    "reaped" from the waiting pool).
    """
    def __init__(self, callback, log=LOG_FILE, interval=1):
        """The callable ``callback`` will be invoked with the cluster
        ID of every waited-upon job that finishes. ``interval``
        specifies the polling rate.
        """
        threading.Thread.__init__(self)
        self.callback = callback
        self.log = log
        self.interval = interval
        self.waiting = set()
        self.lock = threading.Lock()
        self.shutdown = False

    def stop(self):
        """Stop the thread soon."""
        with self.lock:
            self.shutdown = True

    def wait(self, clustid):
        """Adds a new job ID to the set of jobs being waited upon."""
        with self.lock:
            self.waiting.add(clustid)

    def run(self):
        while True:
            with self.lock:
                if self.shutdown:
                    return

                # Poll the log file.
                if os.path.exists(self.log):
                    with open(self.log) as f:
                        for line in f:
                            if 'Job terminated.' in line:
                                clustid = re.search(r'\((\d+)\.', line).group(1)
                                clustid = int(clustid)
                                if clustid in self.waiting:
                                    self.callback(clustid)
                                    self.waiting.remove(clustid)

                time.sleep(self.interval)

if __name__ == '__main__':
    jid, jfn = submit_script("#!/bin/sh\necho hey there")
    try:
        print("running job %i" % jid)
        stdout, stderr = getoutput(jid)
        print("job done")
        print(stdout)
    finally:
        os.unlink(jfn)
