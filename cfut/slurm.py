"""Abstracts access to a Slurm cluster via its command-line tools.
"""
import os
from .util import chcall, random_string, local_filename, shlex_join

LOG_FILE = local_filename("slurmpy.log")
OUTFILE_FMT = local_filename("slurmpy.stdout.{}.log")

def submit_text(job):
    """Submits a Slurm job represented as a job file string. Returns
    the job ID.
    """
    filename = local_filename('_temp_{}.sh'.format(random_string()))
    with open(filename, 'w') as f:
        f.write(job)
    jobid, _ = chcall('sbatch --parsable {}'.format(filename))
    os.unlink(filename)
    return int(jobid)

def submit(cmdline, outpat=OUTFILE_FMT.format('%j'), additional_setup_lines=[]):
    """Starts a Slurm job that runs the specified shell command line.
    """
    script_lines = [
        "#!/bin/sh",
        "#SBATCH --output={}".format(outpat),
        *additional_setup_lines,
        shlex_join(['srun', *cmdline]),
    ]
    return submit_text('\n'.join(script_lines))
