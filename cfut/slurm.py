"""Abstracts access to a Slurm cluster via its command-line tools.
"""
import re
import os
import threading
import time
from .util import chcall, random_string

LOG_FILE = "slurmpy.log"
OUTFILE_FMT = "slurmpy.stdout.{}.log"

def submit_text(job):
    """Submits a Slurm job represented as a job file string. Returns
    the job ID.
    """
    filename = '_temp_{}.sh'.format(random_string())
    with open(filename, 'w') as f:
        f.write(job)
    out, _ = chcall('sbatch {}'.format(filename))
    jobid = re.search(r'job (\d+)', out).group(1)
    os.unlink(filename)
    return int(jobid)

def submit(cmdline, outpat=OUTFILE_FMT.format('%j')):
    """Starts a Slurm job that runs the specified shell command line.
    """
    script_lines = [
        "#!/bin/sh",
        "#SBATCH --output={}".format(outpat),
        "srun {}".format(cmdline),
    ]
    return submit_text('\n'.join(script_lines))
