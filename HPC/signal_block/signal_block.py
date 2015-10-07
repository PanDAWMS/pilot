#!/usr/bin/python

from ctypes import *
import signal
import time

# Get the size of the array used to
# represent the signal mask
SIGSET_NWORDS = 1024 / (8 * sizeof(c_ulong))

# Define the sigset_t structure
class SIGSET(Structure):
    _fields_ = [
        ('val', c_ulong * SIGSET_NWORDS)
    ]

# Create a new sigset_t to mask out SIGINT
sigs = (c_ulong * SIGSET_NWORDS)()
SIG_BLOCK = 0

try:
    libc = CDLL('libc.so.6')
except:
    print "libc.so.6 cannot be loaded"
    libc = None

def block_sig(sig):
    '''Mask the sig'''
    SIG_BLOCK = 0
    sigs[0] = 2 ** (sig - 1)
    mask = SIGSET(sigs)
    if libc:
        libc.sigprocmask(SIG_BLOCK, pointer(mask), 0)

def unblock_sig(sig):
    SIG_UNBLOCK = 1
    sigs[0] = 2 ** (sig - 1)
    mask = SIGSET(sigs)
    if libc:
        libc.sigprocmask(SIG_UNBLOCK, pointer(mask), 0)


if __name__ == "__main__":
    print "block signal"
    block_sig(signal.SIGTERM)
    for i in range(100):
        time.sleep(1)
    print "unblock signal"
    unblock_sig(signal.SIGTERM)
    for i in range(100):
        time.sleep(1)
