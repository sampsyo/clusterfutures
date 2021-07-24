import glob
import os.path as osp

from cfut.util import local_filename
from cfut.remote import worker

def run_all_outstanding_work():
    in_files = glob.glob(local_filename('cfut.in.*.pickle'))
    worker_ids = [osp.basename(f)[len('cfut.in.'):-len('.pickle')]
                  for f in in_files]
    for wid in worker_ids:
        worker(wid)
