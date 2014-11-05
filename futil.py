import os
import sys
import traceback
import errno
import string
from pUtil import tolog

class LoggingLevel:
    ALL = 0
    DEBUG = 10
    INFO  = 20
    WARNING = 40
    ERROR   = 50

loglevel= LoggingLevel()
del LoggingLevel

__logging_level = loglevel.ALL

def log_setlevel(level):
    global __logging_level
    __logging_level = level    

def log(*arg):
    """Log function"""
    argstr = ' '.join(map(str, arg))
    print argstr
    
def log_err(*arg):
    """Log function for errors"""
    # if __logging_level<=loglevel.ERROR: - errors always printed
    log('E:', *arg)

def log_warn(*arg):
    """Log function for warnings"""
    if __logging_level<=loglevel.WARNING:
        log('W:', *arg)

def log_deb(*arg):
    """Log function for warnings"""
    if __logging_level<=loglevel.DEBUG:
        log('D:', *arg)

def log_info(*arg):
    """Log function for information"""
    if __logging_level<=loglevel.INFO:
        log('I:', *arg)

def check_syserr(ecode_par, etext):
    """Check errors due to code execution (Unix only)

    ecode -- return exit code to analyze
    etext -- error message to display
    
    The return exit code from code execution in Unix is a composed information:
    0xYYZZ where YY is the exit code (if exit(2) was called), ZZ is the signal (if called)

    Return False if ecode==0 (the profram exited successfully)
    Return (exit_code, exit_signal) after printing erorr messages if the program
     exited abnormally (exit code != 0 or due to a interrupt signal)

    check_syserr can be used in 3 different ways:
    if (check_syserr(s, o)):    # as condition (shortcut)
    check_syserr(s, o)          # to print error messages clarifying the error
    retval = check_syserr(s, o) # to take specific actions depending on the output
    if retval:          # remember to check (if no error return False, retval is not a tuple)
        if retval(0):
            ...         # use the error code
        if retval(1):
            ...         # use the signal code
            
    """    
    try:
        ecode = int(ecode_par)
    except ValueError:
        tolog('!!WARNING!!5000!! Exit code is not an integer: %s' % str(ecode_par))
        ecode = 1    
    if ecode == 0:
        return False
    _est = '-'
    _esig = '-'
    _num_est = 0
    _num_esig = 0
    if os.WIFEXITED(ecode):
        _est = os.WEXITSTATUS(ecode)
        _num_est = os.WEXITSTATUS(ecode)
    if os.WIFSIGNALED(ecode):
        _esig = os.WTERMSIG(ecode)
        _num_esig = os.WTERMSIG(ecode)
    if _num_est == errno.ETIMEDOUT:
        # timed_command timeout possible
        etext = "Possible incomplete execution due to time-out"
    if not etext:
        etext = "(etext not set)"
    tolog('WARNING: Abnormal termination: ecode=%s, ec=%s, sig=%s, len(etext)=%d' % (ecode, _est, _esig, len(etext)))
    if len(etext) > 0:
        # filter etext from new lines to prevent concaternated log extract warning/failed messages
        etext = etext.replace('\n', ' ')
        tolog('WARNING: Error message: %s' % (etext))
    else:
        tolog('!!WARNING!!5000!! Function did not return an error message')
    return (_num_est, _num_esig)    # always evaluating True

def is_timeout(ecode):
    """
    timed_command returns errno.ETIMEDOUT whenever the command times out
    Double check behavior with signals
    """
    _est = None
#    if os.WIFEXITED(ecode):
    if os.WIFSIGNALED(ecode):
        _est = os.WEXITSTATUS(ecode)
    if _est == errno.ETIMEDOUT:
        return True
    return False
                            
def get_exc_short():
    """Print only error type and error value.
    """
    exType, exValue, exTb = sys.exc_info()
    resL1 = traceback.format_exception_only(exType, exValue)
    return string.join(resL1, "")

def get_exc():
    """
    Print error type, error value and traceback.
    """
    exType, exValue, exTb = sys.exc_info()
    resL1 = traceback.format_exception(exType, exValue, exTb, None)
    return string.join(resL1, "")

def get_exc_plus():
    """Print the usual traceback information, followed by a listing of all the
    local variables in each frame.
    """
    exType, exValue, exTb = sys.exc_info()
    tb = exTb
    #tb = copy.copy(exTb)      # make a shallow copy of y
    #tb = copy.deepcopy(exTb)  # make a deep copy of y
    #tb = sys.exc_info()[2]
    while 1:
        if not tb.tb_next:
            break
        tb = tb.tb_next
    stack = []
    f = tb.tb_frame
    while f:
        stack.append(f)
        f = f.f_back
    stack.reverse()
    #Formatting the information
    resL1 = traceback.format_exception(exType, exValue, exTb, None)
    resL2 = []
    resL2.append("\nLocals by frame, innermost last\n")
    for frame in stack:
        resL2.append("\nFrame %s in %s at line %s\n" % (frame.f_code.co_name,
                                             frame.f_code.co_filename,
                                             frame.f_lineno)
                     )
        for key, value in frame.f_locals.items():
            #We have to be careful not to cause a new error in our error
            #printer! Calling str() on an unknown object could cause an
            #error we don't want.
            try:                   
                resT = "\t%20s = %s\n" % (key, value)
            except:
                 resT = "\t%20s = <ERROR WHILE PRINTING VALUE>\n" % key
            resL2.append(resT)
    return string.join(resL1+resL2, "")
        
