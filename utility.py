import threading
import subprocess
import logging
import time
import os
import signal
import psutil
import pipes

log = logging.getLogger("Utility")

# TODO: Create proper stream collectors


def touch(fname, times=None):
    """
    Polyfill for touch(1).

    :param fname: file to touch
    :param times: time to set
    :return:
    """
    with open(fname, 'a'):
        os.utime(fname, times)


class CollectStream(threading.Thread):
    def __init__(self, stream, child):
        threading.Thread.__init__(self)
        self.stream = stream
        self.child = child
        self.buffer = ''

    def run(self):
        while True:
            out = self.stream.read(1)
            if out == '' and self.child.poll() is not None:
                break
            if out != '':
                self.buffer += out
                # print(out)

        self.stream.close()


terminator = signal.SIGTERM if os.name != 'nt' else signal.CTRL_BREAK_EVENT


class Popen(psutil.Popen):

    def __init__(self, args, timeout=None, terminate_timeout=5):
        psutil.Popen.__init__(self, args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        o = CollectStream(self.stdout, self)
        e = CollectStream(self.stderr, self)

        o.start()
        e.start()

        if timeout:
            end = time.time() + timeout
        while self.is_running():
            if timeout and end < time.time():
                log.info("child timed out, terminating")
                self.terminate_graceful()
                end = time.time() + terminate_timeout
                break

        while self.is_running():
            if terminate_timeout and end < time.time():
                log.info("child termination timed out, killing")
                self.kill()
                break

    def terminate_graceful(self):
        self.send_signal(terminator)


class Utility(object):

    def __init__(self):
        pass

    def call(self, arguments, timeout=None, terminate_timeout=5):
        log.info("calling " + " ".join(pipes.quote(x) for x in arguments))
        child = psutil.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        o = CollectStream(child.stdout, child)
        e = CollectStream(child.stderr, child)

        o.start()
        e.start()

        if timeout:
            end = time.time() + timeout
        while child.is_running():
            if timeout and end < time.time():
                log.info("child timed out, terminating")
                self.terminate_child(child)
                end = time.time() + terminate_timeout
                break

        while child.is_running():
            if terminate_timeout and end < time.time():
                log.info("child termination timed out, killing")
                self.kill_child(child)
                break

        rc = child.wait()

        return rc, o.buffer, e.buffer


if __name__ == "__main__":
    u = Utility()
    logging.basicConfig()
    log.setLevel(logging.DEBUG)
    c, o, e = u.call(["bash", "trap.sh"], timeout=1, terminate_timeout=1)
    print("%d\n____________________\n" % c)
    print(o)
    print("\n____________________\n")
    print(e)
    print("\n____________________\n")
