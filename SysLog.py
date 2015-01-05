import syslog
from commands import getoutput

from pUtil import tolog

def sysLog(message):
    """ Write message to syslog """

    status = False
    try:
        if message != "":
            syslog.syslog(syslog.LOG_ERR, message)
        else:
            tolog("!!WARNING!!4444!! Will not write empty message to syslog")
    except Exception, e:
        tolog("!!WARNING!!4444!! Failed to write to syslog: %s" % (e))
    else:
        status = True

    return status

def getSysLogTail():
    """ Return the tail of the syslog """

    out = ""
    path = "/var/log/messages"
    cmd = "tail %s" % (path)
    tolog("Executing command: %s" % (cmd))
    try:
        out = getoutput(cmd)
    except Exception, e:
        tolog("!!WARNING!!4444!! Could not read path %s, %s" % (path, e))

    return out

def dumpSysLogTail():
    """ Dump the syslog tail to the pilot log """

    tolog("syslog tail:\n%s" % (getSysLogTail()))
    
if __name__ == "__main__":

    message = "hi"
    if sysLog(message):
        tolog("ok")
        out = getSysLogTail()
        if out != "":
            print out
        else:
            print "NOT ok"
    else:
        tolog("not ok")
    dumpSysLogTail()
