import threading
import subprocess
import logging
import time
import os
import signal
import psutil
import pipes
import hashlib
import zlib

log = logging.getLogger("Utility")

# TODO: Create proper stream collectors

def timeStamp():
    """ return ISO-8601 compliant date/time format """

    tmptz = time.timezone
    if tmptz > 0:
        signstr = '-'
    else:
        signstr = '+'
    tmptz_hours = int(tmptz/3600)

    return str("%s%s%02d%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()), signstr, tmptz_hours, int(tmptz/60-tmptz_hours*60)))

def getCPUmodel():
    """ Get cpu model and cache size from /proc/cpuinfo """
    # model name      : Intel(R) Xeon(TM) CPU 2.40GHz
    # cache size      : 512 KB
    # gives the return string "Intel(R) Xeon(TM) CPU 2.40GHz 512 KB"

    cpumodel = ''
    cpucache = ''
    modelstring = ''
    try:
        f = open('/proc/cpuinfo', 'r')
    except Exception, e:
        tolog("Could not open /proc/cpuinfo: %s" % e)
    else:
        re_model = re.compile('^model name\s+:\s+(\w.+)')
        re_cache = re.compile('^cache size\s+:\s+(\d+ KB)')

        # loop over all lines in cpuinfo
        for line in f.readlines():
            # try to grab cpumodel from current line
            model = re_model.search(line)
            if model:
                # found cpu model
                cpumodel = model.group(1)

            # try to grab cache size from current line
            cache = re_cache.search(line)
            if cache:
                # found cache size
                cpucache = cache.group(1)

            # stop after 1st pair found - can be multiple cpus
            if cpumodel and cpucache:
                # create return string
                modelstring = cpumodel + " " + cpucache
                break

        f.close()

    # default return string if no info was found
    if not modelstring:
        modelstring = "UNKNOWN"

    return modelstring


def touch(fname, times=None):
    """
    Polyfill for touch(1).

    :param fname: file to touch
    :param times: time to set
    :return:
    """
    with open(fname, 'a'):
        os.utime(fname, times)

# construct file path
def construct_file_path(base_path, scope, lfn):
    hash = hashlib.md5()
    hash.update('%s:%s' % (scope, lfn))
    hash_hex = hash.hexdigest()
    correctedscope = "/".join(scope.split('.'))
    dstURL = "{basePath}/{scope}/{hash1}/{hash2}/{lfn}".format(basePath=base_path,
                                                               scope=correctedscope,
                                                               hash1=hash_hex[0:2],
                                                               hash2=hash_hex[2:4],
                                                               lfn=lfn)
    return dstURL

# calculate adler32
def calc_adler32(file_name):
    val = 1
    blockSize = 32 * 1024 * 1024
    with open(file_name) as fp:
        while True:
            data = fp.read(blockSize)
            if not data:
                break
            val = zlib.adler32(data, val)
    if val < 0:
        val += 2 ** 32
    return hex(val)[2:10].zfill(8).lower()


        

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

    def call(self, arguments, timeout=None, terminate_timeout=5,stdout_file=None,stderr_file=None,shell=False):
        if not shell:
            log.info("calling " + " ".join(pipes.quote(x) for x in arguments))
        else:
            log.info('calling ' + arguments)
        if 'ATHENA_PROC_NUMBER' in os.environ:
            log.info('ATHENA_PROC_NUMBER='+os.environ['ATHENA_PROC_NUMBER'])
        
        
        if stdout_file is not None and stderr_file is not None:
            stdout = open(stdout_file,'w')
            stderr = open(stderr_file,'w')
        
            child = subprocess.Popen(arguments, stdout=stdout, stderr=stderr,env=os.environ,shell=shell)
            child.wait()

            stdout = 'stdout saved to ' + stdout_file
            stderr = 'stderr saved to ' + stderr_file
        else:
            stdout = subprocess.PIPE
            stderr = subprocess.PIPE
            child = subprocess.Popen(arguments, stdout=stdout, stderr=stderr,env=os.environ,shell=shell)
            stdout,stderr = child.communicate()
        

        

        log.info('subprocess exited with return code: %s'%str(child.returncode))


        #if timeout:
        #    end = time.time() + timeout
        #while child.pool() is None:
        #    if timeout and end < time.time():
        #        log.info("child timed out, terminating")
        #        self.terminate_child(child)
        #        end = time.time() + terminate_timeout
        #        break

        #while child.is_running():
        #    if terminate_timeout and end < time.time():
        #        log.info("child termination timed out, killing")
        #        self.kill_child(child)
        #        break

        #rc = child.wait()

        return child.returncode,stdout,stderr



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
