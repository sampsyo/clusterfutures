Cluster Futures
===============

This module provides a Python `concurrent.futures`_ executor that lets you run
functions on remote systems in your `HTCondor`_ or `Slurm`_ cluster. Stop worrying
about writing job files, scattering/gathering, and serialization---this module
does it all for you.

It uses the `cloudpickle`_ library to allow (most) closures to be used
transparently, so you're not limited to "pure" functions.

Installation::

    pip install clusterfutures

Usage:

.. code-block:: python

    import cfut
    def square(n):
        return n * n

    with cfut.SlurmExecutor() as executor:
        for result in executor.map(square, [5, 7, 11]):
            print(result)

See `slurm_example.py`_ and `condor_example.py`_ for further examples.
The easiest way to get started is to
ignore the fact that futures are being used at all and just use the provided
``map`` function, which behaves like `itertools.imap`_ but transparently
distributes your work across the cluster.

Goals & design
--------------

*clusterfutures* is a simple wrapper to run Python functions in batch jobs on
an HPC cluster. Each future corresponds to one batch job. The functions
that you run through clusterfutures should normally run for at least a few
seconds each: running smaller functions will be inefficient because of the
overhead of launching jobs and moving data.

Functions, parameters and return values are sent by creating files; this assumes
that the control process and the worker nodes have a shared filesystem.
This mechanism is convenient for relatively small amounts of data; it's probably
not the best way to transfer large amounts of data to & from workers.

.. _concurrent.futures:
    https://docs.python.org/3/library/concurrent.futures.html
.. _HTCondor: https://research.cs.wisc.edu/htcondor/
.. _cloudpickle: https://github.com/cloudpipe/cloudpickle
.. _itertools.imap: https://docs.python.org/3/library/itertools.html#itertools.imap
.. _Slurm: https://slurm.schedmd.com/
.. _slurm_example.py: https://github.com/sampsyo/clusterfutures/blob/master/slurm_example.py
.. _condor_example.py: https://github.com/sampsyo/clusterfutures/blob/master/condor_example.py
