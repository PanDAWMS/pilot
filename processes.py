import commands
import os
import signal
import time
import re
import pUtil
from subprocess import Popen, PIPE


def findProcessesInGroup(cpids, pid):
    """ recursively search for the children processes belonging to pid and return their pids
    here pid is the parent pid for all the children to be found
    cpids is a list that has to be initialized before calling this function and it contains
    the pids of the children AND the parent as well """

    cpids.append(pid)
    psout = commands.getoutput("ps -eo pid,ppid -m | grep %d" % pid)
    lines = psout.split("\n")
    if lines != ['']:
        for i in range(0, len(lines)):
            thispid = int(lines[i].split()[0])
            thisppid = int(lines[i].split()[1])
            if thisppid == pid:
                findProcessesInGroup(cpids, thispid)

def isZombie(pid):
    """ Return True if pid is a zombie process """

    zombie = False

    out = commands.getoutput("ps aux | grep %d" % (pid))
    if "<defunct>" in out:
        zombie = True

    return zombie

def getProcessCommands(euid, pids):
    """ return a list of process commands corresponding to a pid list for user euid """

    _cmd = 'ps u -u %d' % (euid)
    processCommands = []
    ec, rs = commands.getstatusoutput(_cmd)
    if ec != 0:
        pUtil.tolog("Command failed: %s" % (rs))
    else:
        # extract the relevant processes
        pCommands = rs.split('\n') 
        first = True
        for pCmd in pCommands:
            if first:
                # get the header info line
                processCommands.append(pCmd)
                first = False
            else:
                # remove extra spaces
                _pCmd = pCmd
                while "  " in _pCmd:
                    _pCmd = _pCmd.replace("  ", " ")
                items = _pCmd.split(" ")
                for pid in pids:
                    # items = username pid ...
                    if items[1] == str(pid):
                        processCommands.append(pCmd)
                        break

    return processCommands


def printProcessTree():
    import subprocess
    pl = subprocess.Popen(['ps', '--forest', '-ef'], stdout=subprocess.PIPE).communicate()[0]
    pUtil.tolog(pl)

def dumpStackTrace(pid):
    """ run the stack trace command """

    # make sure that the process is not in a zombie state
    if not isZombie(pid):
        pUtil.tolog("Running stack trace command on pid=%d:" % (pid))
        cmd = "pstack %d" % (pid)
        out = commands.getoutput(cmd)
        if out == "":
            pUtil.tolog("(pstack returned empty string)")
        else:
            pUtil.tolog(out)
    else:
        pUtil.tolog("Skipping pstack dump for zombie process")

def killProcesses(pid, pgrp):
    """ kill a job upon request """

    pUtil.tolog("killProcesses() called")

    # if there is a known subprocess pgrp, then it should be enough to kill the group in one go
    status = False

    _sleep = True
    if pgrp != 0:
        # kill the process gracefully
        pUtil.tolog("Killing group process %d" % (pgrp))
        try:
            os.killpg(-pgrp, signal.SIGTERM)
        except Exception,e:
            pUtil.tolog("WARNING: Exception thrown when killing the child group process under SIGTERM, wait for kill -9 later: %s" % (e))
            _sleep = False
        else:
            pUtil.tolog("(SIGTERM sent)")

        if _sleep:
            _t = 30
            pUtil.tolog("Sleeping %d s to allow processes to exit" % (_t))
            time.sleep(_t)

        try:
            os.killpg(-pgrp, signal.SIGKILL)
        except Exception,e:
            pUtil.tolog("WARNING: Exception thrown when killing the child group process under SIGTERM, wait for kill -9 later: %s" % (e))
        else:
            pUtil.tolog("(SIGKILL sent)")
            status = True

    if not status:
        # firstly find all the children process IDs to be killed
        children = []
        findProcessesInGroup(children, pid)

        # reverse the process order so that the athena process is killed first (otherwise the stdout will be truncated)
        children.reverse()
        pUtil.tolog("Process IDs to be killed: %s (in reverse order)" % str(children))

        # find which commands are still running
        try:
            cmds = getProcessCommands(os.geteuid(), children)
        except Exception, e:
            pUtil.tolog("getProcessCommands() threw an exception: %s" % str(e))
        else:
            if len(cmds) <= 1:
                pUtil.tolog("Found no corresponding commands to process id(s)")
            else:
                pUtil.tolog("Found commands still running:")
                for cmd in cmds:
                    pUtil.tolog(cmd)

                # loop over all child processes
                for i in children:
                    # dump the stack trace before killing it
                    dumpStackTrace(i)

                    # kill the process gracefully
                    try:
                        os.kill(i, signal.SIGTERM)
                    except Exception,e:
                        pUtil.tolog("WARNING: Exception thrown when killing the child process %d under SIGTERM, wait for kill -9 later: %s" % (i, str(e)))
                        pass
                    else:
                        pUtil.tolog("Killed pid: %d (SIGTERM)" % (i))

                    _t = 10
                    pUtil.tolog("Sleeping %d s to allow process to exit" % (_t))
                    time.sleep(_t)
    
                    # now do a hardkill just in case some processes haven't gone away
                    try:
                        os.kill(i, signal.SIGKILL)
                    except Exception,e:
                        pUtil.tolog("WARNING: Exception thrown when killing the child process %d under SIGKILL, ignore this if it is already killed by previous SIGTERM: %s" % (i, str(e)))
                        pass
                    else:
                        pUtil.tolog("Killed pid: %d (SIGKILL)" % (i))

    pUtil.tolog("Killing any remaining orphan processes")
    killOrphans()

def checkProcesses(pid):
    """ Check the number of running processes """

    children = []
    n = 0
    try:
        findProcessesInGroup(children, pid)
    except Exception, e:
        pUtil.tolog("!!WARNING!!2888!! Caught exception in findProcessesInGroup: %s" % (e))
    else:
        n = len(children)
        pUtil.tolog("Number of running processes: %d" % (n))
    return n

def killOrphans():
    """ Find and kill all orphan processes belonging to current pilot user """
    
    pUtil.tolog("Searching for orphan processes")
    cmd = "ps -o pid,ppid,args -u %s" % (commands.getoutput("whoami"))

    processes = commands.getoutput(cmd)
    pattern = re.compile('(\d+)\s+(\d+)\s+(\S+)')

    count = 0
    for line in processes.split('\n'):
        ids = pattern.search(line)
        if ids:
            pid = ids.group(1)
            ppid = ids.group(2)
            args = ids.group(3)
            if 'cvmfs2' in args:
                pUtil.tolog("Ignoring possible orphan process running cvmfs2: pid=%s, ppid=%s, args='%s'" % (pid, ppid, args))
            elif 'pilots_starter.py' in args:
                pUtil.tolog("Ignoring Pilot Launcher: pid=%s, ppid=%s, args='%s'" % (pid, ppid, args))
            elif ppid == '1':
                count += 1
                pUtil.tolog("Found orphan process: pid=%s, ppid=%s, args='%s'" % (pid, ppid, args))
                if args.endswith('bash'):
                    pUtil.tolog("Will not kill bash process")
                else:
                    cmd = 'kill -9 %s' % (pid)
                    ec, rs = commands.getstatusoutput(cmd)
                    if ec != 0:
                        pUtil.tolog("!!WARNING!!2999!! %s" % (rs))
                    else:
                        pUtil.tolog("Killed orphaned process %s (%s)" % (pid, args))

    if count == 0:
        pUtil.tolog("Did not find any orphan processes")
    else:
        pUtil.tolog("Found %d orphan process(es)" % (count))

def getMaxMemoryUsageFromCGroups():
    """ Read the max_memory from CGROUPS file memory.max_usage_in_bytes"""

    max_memory = None

    # Get the CGroups max memory using the pilot pid
    pid = os.getpid()
    path = "/proc/%d/cgroup" % (pid)
    if os.path.exists(path):
        cmd = "grep memory %s" % (path)
        pUtil.tolog("Executing command: %s" % (cmd))
        out = commands.getoutput(cmd)
        if out == "":
            pUtil.tolog("(Command did not return anything)")
        else:
            pUtil.tolog(out)
            if ":memory:" in out:
                pos = out.find('/')
                path = out[pos:]
                pUtil.tolog("Extracted path = %s" % (path))

                pre = getCGROUPSBasePath()
                if pre != "":
                    path = pre + os.path.join(path, "memory.max_usage_in_bytes")
                    pUtil.tolog("Path to CGROUPS memory info: %s" % (path))

                    try:
                        f = open(path, 'r')
                    except IOError, e:
                        pUtil.tolog("!!WARNING!!2212!! Could not open file %s: %s" % (path, e))
                    else:
                        max_memory = f.read()
                        f.close()

                else:
                    pUtil.tolog("CGROUPS base path could not be extracted - not a CGROUPS site")
            else:
                pUtil.tolog("!!WARNING!!2211!! Invalid format: %s (expected ..:memory:[path])" % (out))
    else:
        pUtil.tolog("Path %s does not exist (not a CGROUPS site)")

    return max_memory

def getCGROUPSBasePath():
    """ Return the base path for CGROUPS """

    return commands.getoutput("grep \'^cgroup\' /proc/mounts|grep memory| awk \'{print $2}\'")

def isCGROUPSSite():
    """ Return True if site is a CGROUPS site """

    status = False

    if getCGROUPSBasePath() != "":
        status = True

    # Make experiment specific?
#    if os.environ.has_key('ATLAS_CGROUPS_BASE'):
#        cgroups = os.environ['ATLAS_CGROUPS_BASE']
#        if cgroups != "":
#            pUtil.tolog("ATLAS_CGROUPS_BASE = %s" % (cgroups))
#            # if cgroups.lower() == "true":
#            status = True

    return status
