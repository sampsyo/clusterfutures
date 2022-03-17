import os
import random
import shlex
import string
import subprocess

def local_filename(filename=""):
    return os.path.join(os.getenv("CFUT_DIR", ".cfut"), filename)

INFILE_FMT = local_filename('cfut.in.%s.pickle')
OUTFILE_FMT = local_filename('cfut.out.%s.pickle')

def random_string(length=32, chars=(string.ascii_letters + string.digits)):
    return ''.join(random.choice(chars) for i in range(length))

def call(command, stdin=None):
    """Invokes a shell command as a subprocess, optionally with some
    data sent to the standard input. Returns the standard output data,
    the standard error, and the return code.
    """
    res = subprocess.run(command, shell=True, input=stdin,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return res.stdout, res.stderr, res.returncode

class CommandError(Exception):
    """Raised when a shell command exits abnormally."""
    def __init__(self, command, code, stderr):
        self.command = command
        self.code = code
        self.stderr = stderr

    def __str__(self):
        return "%s exited with status %i: %s" % (repr(self.command),
                                                 self.code,
                                                 repr(self.stderr))

def chcall(command, stdin=None):
    """Like ``call`` but raises an exception when the return code is
    nonzero. Only returns the stdout and stderr data.
    """
    stdout, stderr, code = call(command, stdin)
    if code != 0:
        raise CommandError(command, code, stderr)
    return stdout, stderr


# This is part of the shlex module from Python 3.8. This is copied from 3.10:
def shlex_join(split_command):
    """Return a shell-escaped string from *split_command*."""
    return ' '.join(shlex.quote(arg) for arg in split_command)
