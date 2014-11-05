#!/usr/bin/env python

_version_info = "$Id: timed_command.py,v 1.13 2011/07/22 14:37:25 cgw Exp $"

# Author:  Charles G Waldman
# Date: Nov 10 2007
# Email: cgw@hep.uchicago.edu

"""This module exports a single function "timed_command" which runs a shell
   command with an optional timeout.
See that function's docstring for more info."""

import sys, os, time, select, fcntl, signal, errno

# commented out everything concerning closing of all possible  file  descriptors
# this can be very time consuming for sites where SC_OPEN_MAX is even 32k   
# and there is no proof this helps in killing rogue commands.
# ivukotic@cern.ch
 
#try:
#    MAXFD = os.sysconf('SC_OPEN_MAX')
#except (AttributeError, ValueError):
#    MAXFD = 256

_verbose = False

GRACE_PERIOD=2 ##if SIGTERM fails, wait this long before
               ##sending SIGKILL

def _child(stdin, stdout, stderr, cmd):
    # Do a little file descriptor plumbing
    os.dup2(stdin, 0)
    os.dup2(stdout, 1)
    os.dup2(stderr, 2)
#    for i in xrange(3, MAXFD):
#        try:
#            os.close(i)
#        except OSError:
#            pass

    # Turn a command string into a vector, if needed
#    if isinstance(cmd, str):
    if isinstance(cmd, basestring):
        cmd = ['/bin/sh', '-c', cmd]
    # Run it!
    try:
        os.execvp(cmd[0], cmd)
    finally:
        os._exit(1)

def _sighandler(sig, frame):
    if _verbose:
        print "sighandler!"
    pass
    
def _parent(child_stdout, child_stderr, child_pid, timeout):
    t0 = time.time()
    if timeout is not None:
        end_time = t0 + timeout
    timed_out = False
    exit_status = None
    child_alive = True
    # Set up for non-blocking reads
    fcntl.fcntl(child_stdout, fcntl.F_SETFL, os.O_NONBLOCK)
    fcntl.fcntl(child_stderr, fcntl.F_SETFL, os.O_NONBLOCK)
    blocksize = 4096
    buf = {child_stdout:'', child_stderr:''}
    # Watch over the child process, collecting output
    #  as available.  We count on SIGCHLD to break
    #  us out of the key `select' syscall.
    # Do not select for more than a minute, in case
    #  the child process hangs without producing
    #  any output, or the SIGCHLD is not handled
    #  reliably
    
    while child_alive and not timed_out:
        if timeout is not None:
            remaining = end_time - time.time()
            if remaining <= 0:
                timed_out = True
                break
            remaining = min(remaining, 1)
        else:
            remaining = 1

        # Check child process still running before calling "select"
        done_pid, exit_status = os.waitpid(child_pid, os.WNOHANG)
        if done_pid == child_pid:
            child_alive = False
            remaining = 0
        else:
            exit_status = None # this forces re-check later

            fds = [child_stdout, child_stderr]
            try:
                fds, ignore, ignore = select.select(fds, [], [], remaining)
            except: # child exited, select interrupted by signal
                # Go through loop one more time to pick up any final outputs
                pass
        
        # Collect any ready output, making sure we do not go beyond end_time
        for fd in fds:
            while True:
                try:
                    text = os.read(fd, blocksize)
                except OSError: # no data available, non-blocking read
                    text = ''
                if not text:
                    break
                buf[fd] += text
                if timeout is not None and time.time() > end_time:
                    timed_out = True
                    break

    # Do not include time taken to kill child
    elapsed_time = time.time() - t0
    # Kill child if needed
    if timed_out:

        if _verbose:
            print "Command timed out, sending SIGTERM"
        sig_used = signal.SIGTERM
        try:
            os.kill(-child_pid, signal.SIGTERM)
        except OSError, ex:
           if ex.errno==3:
               print "process timed out and was killed by SIGTERM."
               return errno.ETIMEDOUT<<8 | sig_used, elapsed_time, buf[child_stdout], buf[child_stderr] 
           else:
               print "wasn't able to kill the process using SIGTERM. error number:", ex.errno," -- ",ex.strerror
               raise

        grace_end = time.time() + GRACE_PERIOD
        while time.time() < grace_end:
            done_pid, exit_status = os.waitpid(child_pid, os.WNOHANG)
            if _verbose:
                print "exit_status=", done_pid, exit_status
            if done_pid == child_pid:
                break
            else:
                exit_status = None
            time.sleep(0.1)
        else: # Normal signal did not work, need to kill -9
            if _verbose:
                print "Sending SIGKILL"
            try:
                os.kill(-child_pid, signal.SIGKILL)
            except OSError, ex:
                if ex.errno==3:
                    print "process timed out and was killed by SIGKILL."
                    return errno.ETIMEDOUT<<8 | sig_used, elapsed_time, buf[child_stdout], buf[child_stderr]
                else:
                    print "wasn't able to kill the process using SIGKILL. error number:", ex.errno," -- ",ex.strerror
                    raise
            done_pid, exit_status = os.waitpid(child_pid, os.WNOHANG)
            sig_used = signal.SIGKILL
        # Exit status will be "ETIMEDOUT" 
        exit_status = errno.ETIMEDOUT<<8 | sig_used
    if exit_status is None:
        done_pid, exit_status = os.waitpid(child_pid, os.WNOHANG)
    return exit_status, elapsed_time, buf[child_stdout], buf[child_stderr]

def timed_command(cmd, timeout=None):
    '''timed_command(cmd, timeout=None)
    Run a shell command, with an optional timeout
    If command takes longer than specified time it will be killed,
        first with SIGTERM then 2 seconds later with SIGKILL if needed.
        (The 2 second value can be adjusted by changing GRACE_PERIOD).
    Return (exit_status, time_used, command_output, error_output)
    If the program was terminated due to timeout, the exit status will be
        artificially set as though the program returned ETIMEDOUT.'''

    if timeout==0:
        timeout=None
        
    stdin_r, stdin_w = os.pipe()
    stdout_r, stdout_w = os.pipe()
    stderr_r, stderr_w = os.pipe()

    prev_sighandler = signal.getsignal(signal.SIGCHLD)
    signal.signal(signal.SIGCHLD, _sighandler)
    pid = os.fork()
    if pid == 0: # Child
        _child(stdin_r, stdout_w, stderr_w, cmd)
    else: # Parent
        ret = _parent(stdout_r, stderr_r, pid, timeout)
        # Close all file descriptors we opened
        for fd in stdin_r, stdin_w, stdout_r, stdout_w, stderr_r, stderr_w:
            try:
                os.close(fd)
            except:
                pass
        # Restore default behavior on SIGCHLD
        signal.signal(signal.SIGCHLD, prev_sighandler)
        return ret


def test():
    global _verbose
    _verbose=True

    test_cases =  (
        ('/bin/true', 100), ('/bin/false', 100), 
        ('/bin/true', 0), ('/bin/false', 0),
        ('sleep 10', 0), ('sleep 10', 5),
        ('cat /dev/zero', 5), ('cat /dev/null', 0),
        ('ls /tmp', 100), ('ls -lR /', 3))


    for cmd, t in test_cases:
        print "testing", cmd, "with timeout", t
        status, used, out, err = timed_command(cmd, t)
        print "  exit status:", status
        exit_status, term_sig = os.WEXITSTATUS(status), os.WTERMSIG(status)
        print "    program returned", exit_status
        if term_sig:
            print "    program terminated by signal", term_sig
        print "  elapsed time:", used, "sec"
        print " ", len(out), 'bytes of stdout'
        print " ", len(err), 'bytes of stderr'

if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) < 1:
        print _version_info
        sys.exit(0)
    if args[0] in ('-test', '--test'):
        test()
        sys.exit(0)
    if args[0] in ('-v', '-verbose', '--verbose'):
        _verbose = True
        args.pop(0)
    if len(args) < 2:
        print "Usage: %s [--test] [-v|--verbose] timeval cmd [args]" % sys.argv[0]
        sys.exit(1)
    t = args[0]
    try:
        t = float(t)
    except ValueError:
        print "invalid time", t
        sys.exit(1)
    s, t, out, err = timed_command(' '.join(args[1:]), t)
    if out:
        print "stdout=", out
    if err:
        print "stderr=", err
    print "$?=%s t=%s" % (s,t)
    
