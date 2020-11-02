import cfut
import subprocess
import concurrent.futures

# "Worker" functions.
def square(n):
    return n * n
def square_sum(n,m):
    return n * n + m * m
def hostinfo():
    return subprocess.check_output('uname -a', shell=True)

def example_1():
    """Square some numbers on remote hosts!
    """
    with cfut.SlurmExecutor(True, keep_logs=True) as executor:
        job_count = 5
        futures = [executor.submit(square, n) for n in range(job_count)]
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

def example_2():
    """Get host identifying information about the servers running
    our jobs.
    """
    with cfut.SlurmExecutor(False) as executor:
        futures = [executor.submit(hostinfo) for n in range(15)]
        for future in concurrent.futures.as_completed(futures):
            print(future.result().strip())

def example_3():
    """Demonstrates the use of the map() convenience function.
    """
    exc = cfut.SlurmExecutor(True)
    print(list(cfut.map(exc, square, [5, 7, 11])))

def example_4():
    """Demonstrates the use of the map_array() convenience function.
    """
    exc = cfut.SlurmExecutor(True)
    additional_setup_lines = []
    print(list(cfut.map_array(exc, square, [5, 7, 11],additional_setup_lines)))

def example_5(): # TODO: implement variable *args and **kwargs, currently doesn't work
    """Demonstrates the use of the map_array() convenience function with two arguments.
    """
    exc = cfut.SlurmExecutor(True)
    additional_setup_lines = []
    print(list(cfut.map_array(exc, square_sum, [1, 2, 3],[2,3,4],additional_setup_lines)))

if __name__ == '__main__':
    example_1()
    example_2()
    example_3()
    example_4()
    # example_5()
