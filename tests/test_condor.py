from testpath import MockCommand

import cfut
from .utils import run_all_outstanding_work

def square(n):
    return n * n


def test_submit():
    executor = cfut.CondorExecutor(debug=True, keep_logs=True)
    try:
        with MockCommand.fixed_output('condor_submit', stdout='Proc 0.0') as csub:
            fut = executor.submit(square, 2)
        csub.assert_called()

        assert not fut.done()
        run_all_outstanding_work()
        assert fut.result(timeout=3) == 4
    finally:
        executor.shutdown(wait=False)

CONDOR_JOB_COUNT = """
from pathlib import Path
counter_file = Path(__file__).parent / 'condor_job_id'
if counter_file.is_file():
    count = int(counter_file.read_text().strip()) + 1
else:
    count = 0
counter_file.write_text(str(count))
print("Proc {}.0".format(count))
"""

def test_map():
    executor = cfut.CondorExecutor(debug=True, keep_logs=True)
    try:
        with MockCommand('condor_submit', python=CONDOR_JOB_COUNT) as csub:
            result_iter = executor.map(square, range(4), timeout=5)
        csub.assert_called()

        run_all_outstanding_work()
        assert list(result_iter) == [0, 1, 4, 9]
    finally:
        executor.shutdown(wait=False)
