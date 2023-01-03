"""Abstracts access to a Slurm cluster via its command-line tools.
"""
import os
from subprocess import run, PIPE
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

STATES_FINISHED = {  # https://slurm.schedmd.com/squeue.html#lbAG
    'BOOT_FAIL',  'CANCELLED', 'COMPLETED',  'DEADLINE', 'FAILED',
    'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'SPECIAL_EXIT', 'TIMEOUT',
}

def jobs_finished(job_ids):
    """Check which ones of the given Slurm jobs already finished
    """

    # If there is no Slurm job to check, return right away
    if not job_ids:
        return set()

    res = run([
        'squeue', '--noheader', '--format=%i %T',
        '--jobs', ','.join([str(j) for j in job_ids]), '--states=all',
    ], stdout=PIPE, stderr=PIPE, encoding='utf-8', check=True)
    id_to_state = dict([
        l.strip().partition(' ')[::2] for l in res.stdout.splitlines()
    ])
    # Finished jobs only stay in squeue for a few mins (configurable). If
    # a job ID isn't there, we'll assume it's finished.
    return {j for j in job_ids if id_to_state.get(j, 'COMPLETED') in STATES_FINISHED}
