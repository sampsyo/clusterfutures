from testpath import MockCommand

import cfut
from .utils import run_all_outstanding_work

def square(n):
    return n * n


def test_submit():
    with cfut.SlurmExecutor(True, keep_logs=True) as executor:
        with MockCommand.fixed_output('sbatch', stdout='000000') as sbatch:
            fut = executor.submit(square, 2)
        sbatch.assert_called()

        assert not fut.done()
        run_all_outstanding_work()
        assert fut.result(timeout=3) == 4

SBATCH_JOB_COUNT = """
import os.path as osp
counter_file = osp.join(osp.dirname(__file__), 'sbatch_job_id')
if osp.isfile(counter_file):
    with open(counter_file, 'r+') as f:
        count = int(f.read().strip()) + 1
        print(count)
        f.seek(0)
        f.write(str(count))
else:
    print(0)
    with open(counter_file, 'x') as f:
        f.write('0')
"""

def test_map():
    with cfut.SlurmExecutor(True, keep_logs=True) as executor:
        with MockCommand('sbatch', python=SBATCH_JOB_COUNT) as sbatch:
            result_iter = executor.map(square, range(4), timeout=5)
        sbatch.assert_called()

        run_all_outstanding_work()
        assert list(result_iter) == [0, 1, 4, 9]
