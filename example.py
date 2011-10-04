import cfut
import concurrent.futures

# Shared "worker" functions.
def square(n):
    return n * n
def pid():
    import os
    return os.getpid()
def hostinfo():
    import subprocess
    return subprocess.check_output('hostname; uname -a', shell=True)

def example_1():
    """Square some numbers on remote hosts!
    """
    with cfut.CondorExecutor() as executor:
        futures = [executor.submit(square, n) for n in range(5)]
        for future in concurrent.futures.as_completed(futures):
            print future.result()

if __name__ == '__main__':
    example_1()
