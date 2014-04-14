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

## @brief List all processes and parents and form a dictionary where the
#  parent key lists all child PIDs
#  @parameter listMyOrphans If this is @c True, then processes which share the same
#  @c pgid as this process and have parent PID=1 (i.e., init) get added to this process's children,
#  which allows these orphans to be added to the kill list. N.B. this means
#  that orphans have two entries - as child of init and a child of this
#  process
def getAncestry(listMyOrphans = False):
    psCmd = ['ps', 'ax', '-o', 'pid,ppid,pgid,args', '-m']

    try:
        pUtil.tolog('Executing %s' % psCmd)
        p = Popen(psCmd, stdout=PIPE, stderr=PIPE)
        stdout = p.communicate()[0]
        psPID = p.pid
    except OSError, e:
        pUtil.tolog('!!WARNING!!2222!! Failed to execute "ps" to get process ancestry: %s' % repr(e))
        raise
   
    childDict = {}
    myPgid = os.getpgrp()
    myPid = os.getpid()
    for line in stdout.split('\n'):
        try:
            (pid, ppid, pgid, cmd) = line.split(None, 3)
            pid = int(pid)
            ppid = int(ppid)
            pgid = int(pgid)
            # Ignore the ps process
            if pid == psPID:
                continue
            if ppid in childDict:
                childDict[ppid].append(pid)
            else:
                childDict[ppid] = [pid]
            if listMyOrphans and ppid == 1 and pgid == myPgid:
                pUtil.tolog("Adding PID %d to list of my children as it seems to be orphaned: %s" % (pid, cmd))
                if myPid in childDict:
                    childDict[myPid].append(pid)
                else:
                    childDict[myPid] = [pid]
               
        except ValueError:
            # Not a nice line
            pass
    return childDict

## @brief Find all the children of a particular PID (calls itself recursively to descend into each leaf)
#  @note  The list of child PIDs is reversed, so the grandchildren are listed before the children, etc.
#  so signaling left to right is correct
#  @param psTree The process tree returned by @c trfUtils.listChildren(); if None then @c trfUtils.listChildren() is called internally.
#  @param parent The parent process for which to return all the child PIDs
#  @param listOrphans Parameter value to pass to getAncestry() if necessary
#  @return @c children List of child PIDs
def listChildren(psTree = None, parent = os.getpid(), listOrphans = False):
    """ Take a psTree dictionary and list all children """
    if psTree == None:
        psTree = getAncestry(listMyOrphans = listOrphans)
   
    pUtil.tolog("List children of %d (%s)" % (parent, psTree.get(parent, [])))
    children = []
    if parent in psTree:
        children.extend(psTree[parent])
        for child in psTree[parent]:
            children.extend(listChildren(psTree, child))
    children.reverse()
    return children

## @brief Kill all PIDs
#  @note Even if this function is used, subprocess objects need to join() with the
#  child to prevent it becoming a zombie
#  @param childPIDs Explicit list of PIDs to kill; if absent then listChildren() is called
#  @param sleepTime Time between SIGTERM and SIGKILL
#  @param message Boolean if messages should be printed
#  @param listOrphans Parameter value to pass to getAncestry(), if necessary (beware, killing
#  orphans is dangerous, you may kill "upstream" processes; Caveat Emptor)
def infanticide(childPIDs = None, sleepTime = 3, message = True, listOrphans = False):
    if childPIDs is None:
        childPIDs = listChildren(listOrphans = listOrphans)
       
    if len(childPIDs) > 0 and message:
        pUtil.tolog('Killing these child processes: %s...' % str(childPIDs))
       
    for pid in childPIDs:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
       
    time.sleep(sleepTime)
       
    for pid in childPIDs:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            # OSError happens when the process no longer exists - harmless
            pass

def killProcesses(pid):
    """ Kill all subprocesses """
    # Code is based on Graeme Stewarts TRF function with the same name

    # ignore the pid - it will be found by the infanticide() function
    pUtil.tolog("Using infanticide() to kill all subprocesses")
    try:
        infanticide()
    except Exception, e:
        pUtil.tolog("Caught exception in infanticide(): %s" % (e))

def killProcessesOld(pid):
    """ kill a job upon request """

    #printProcessTree()
    # firstly find all the children process IDs to be killed
    children = []
    findProcessesInGroup(children, pid)
    # reverse the process order so that the athena process is killed first 
    #(otherwise the stdout will be truncated)
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
            first = True
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

                if first:
                    _t = 60
                    first = False
                else:
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
    cmd = "ps -o pid,ppid,comm -u %s" % (commands.getoutput("whoami"))
    processes = commands.getoutput(cmd)
    pattern = re.compile('(\d+)\s+(\d+)\s+(\S+)')

    count = 0
    for line in processes.split('\n'):
        ids = pattern.search(line)
        if ids:
            pid = ids.group(1)
            ppid = ids.group(2)
            comm = ids.group(3)
            if ppid == '1':
                count += 1
                pUtil.tolog("Found orphan process: pid=%s, ppid=%s" % (pid, ppid))
                cmd = 'kill -9 %s' % (pid)
                ec, rs = commands.getstatusoutput(cmd)
                if ec != 0:
                    pUtil.tolog("!!WARNING!!2999!! %s" % (rs))
                else:
                    pUtil.tolog("Killed orphaned process %s (%s)" % (pid, comm))

    if count == 0:
        pUtil.tolog("Did not find any orphan processes")
    else:
        pUtil.tolog("Found %d orphan process(es)" % (count))

