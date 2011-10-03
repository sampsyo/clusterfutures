"""Abstracts access to a Condor cluster via its command-line tools.
"""
import subprocess
import re
import os

LOG_FILE = "condorpy.log"
OUTFILE_FMT = "condorpy.stdout.%s.log"
ERRFILE_FMT = "condorpy.stderr.%s.log"

def call(command, stdin=None):
    """Invokes a shell command as a subprocess, optionally with some
    data sent to the standard input. Returns the standard output data,
    the standard error, and the return code.
    """
    if stdin is not None:
        stdin_flag = subprocess.PIPE
    else:
        stdin_flag = None
    proc = subprocess.Popen(command, shell=True, stdin=stdin_flag,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate(stdin)
    return stdout, stderr, proc.returncode

class CommandError(Exception):
    """Raised when a shell command exits abnormally."""
    def __init__(self, command, code, stderr):
        self.command = command
        self.code = code
        self.stderr = stderr

    def __str__(self):
        return "%s exited with status %i: %s" % (repr(self.command),
                                                 self.code,
                                                 repr(self.stderr))

def chcall(command, stdin=None):
    """Like ``call`` but raises an exception when the return code is
    nonzero. Only returns the stdout and stderr data.
    """
    stdout, stderr, code = call(command, stdin)
    if code != 0:
        raise CommandError(command, code, stderr)
    return stdout, stderr

def submit_text(job):
    """Submits a Condor job represented as a job file string. Returns
    the cluster ID of the submitted job.
    """
    out, _ = chcall('condor_submit -v', job)
    jobid = re.search(r'Proc (\d+)\.0', out).group(1)
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
    filename = 'condorpy.jobscript.%s'
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

if __name__ == '__main__':
    jid, jfn = submit_script("#!/bin/sh\necho hey there")
    try:
        print "running job %i" % jid
        stdout, stderr = getoutput(jid)
        print "job done"
        print stdout
    finally:
        os.unlink(jfn)
