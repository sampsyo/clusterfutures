from testpath import MockCommand

import cfut
from .utils import run_all_outstanding_work

def square(n):
    return n * n


def test_submit():
    with MockCommand.fixed_output('squeue', stdout='000000 RUNNING'):
        with cfut.SlurmExecutor(True, keep_logs=True) as executor:
            with MockCommand.fixed_output('sbatch', stdout='000000') as sbatch:
                fut = executor.submit(square, 2)
            sbatch.assert_called()

            assert not fut.done()
            run_all_outstanding_work()
            assert fut.result(timeout=3) == 4

SBATCH_JOB_COUNT = """
from pathlib import Path
counter_file = Path(__file__).parent / 'sbatch_job_id'
if counter_file.is_file():
    count = int(counter_file.read_text().strip()) + 1
else:
    count = 0
counter_file.write_text(str(count))
print(count)
"""

SQUEUE_STILL_GOING = """
jobs_arg_ix = sys.argv.index('--jobs') + 1
job_ids = sys.argv[jobs_arg_ix].split(',')
for jid in job_ids:
    print(jid, 'RUNNING')
"""

def test_map():
    with MockCommand('squeue', python=SQUEUE_STILL_GOING):
        with cfut.SlurmExecutor(True, keep_logs=True) as executor:
            with MockCommand('sbatch', python=SBATCH_JOB_COUNT) as sbatch:
                result_iter = executor.map(square, range(4), timeout=5)
            sbatch.assert_called()

            run_all_outstanding_work()
            assert list(result_iter) == [0, 1, 4, 9]
