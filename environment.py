from Configuration import Configuration
# import pUtil
import sys
import os

def set_environment():
    
    env = Configuration()
    
    # env['version'] = pUtil.getPilotVersion()
    env['pilot_version_tag'] = 'PR' # 'RC'
    env['force_devpilot'] = False
    
    # get the python executable path, to be used by child job
    env['pyexe'] = sys.executable
    
    # default job dispatcher web server
    env['pshttpurl'] = "pandaserver.cern.ch"   # Should be set with pilot option -w <url>

    # cache URL used by some experiment classes
    env['cache'] = ""

    # URL for the schedconfig
    env['schedconfigURL'] = "http://" + env['pshttpurl'] # Could be set with pilot option -I <url>

    # global variables with some default (test) values
    env['psport'] = 25443                      # PanDA server listening port
    env['uflag'] = None                        # Flag if this pilot is to only get a user analysis job or ordinary job
    env['abortDQSpace'] = 10                   # Unit is in GB
    env['warnDQSpace'] = 100                   # Unit is in GB
    env['localsizelimit_stdout'] = 2*1024**2   # Size limit of payload stdout size during running. unit is in kB
    env['localspacelimit'] = 2*1024**2         # Space limit of remaining local disk size during running. unit is in kB
    env['localspacelimit_user'] = 7*1024**2    # Maximum size of user work area. unit is in kB
    env['localspacelimit0'] = 5*1024**2        # Initial space limit before asking for a job. unit is in kB
    env['outputlimit'] = 500*1024**3           # Maximum allowed size of an output file. unit is B
    env['pilotId'] = 'xtestP001'               # Pilot id
    env['jobSchedulerId'] = 'xtestJS001'       # Scheduler id
    env['heartbeatPeriod'] = 30*60             # 30 minutes limit for checking job state file
    env['maxjobrecDefault'] = 20               # Default maximum number of job recoveries
    env['maxjobrec'] = env['maxjobrecDefault'] # Maximum number of job recoveries
    env['zombieJobList'] = []                  # Zombie list
    env['prodJobDone'] = False                 # Will be set to true when production job has finished
    env['rmwkdir'] = None                      # If set to true, workdir will always be deleted
    env['jobrec'] = True                       # Perform job recovery per default
    env['jobRequestFlag'] = True               # Ask server for initial job (read from file otherwise)
    env['debugLevel'] = 0                      # 0: debug info off, 1: display function name when called, 2: full debug info
    env['maxNumberOfRecoveryAttempts'] = 15    # As attempted by the job recovery
    env['stagein'] = False                     # Set to True during stagein phase
    env['stageout'] = False                    # Set to True during stageout phase
    env['queuename'] = ""                      # Name of queue used to download config info
    env['loopingLimitDefaultProd'] = 12*3600   # If job does not write anything in N hours, it is considered a looping job (production jobs)
    env['loopingLimitDefaultUser'] = 3*3600    # If job does not write anything in N hours, it is considered a looping job (user analysis jobs)
    env['loopingLimitMinDefault'] = 2*3600     # Minimum allow looping limit
    env['stageinretry'] = 2                    # Number of stage-in tries
    env['stageoutretry'] = 2                   # Number of stage-out tries
    env['logFileDir'] = ""                     # Log file directory
    env['testLevel'] = "0"                     # Test suite control variable (0: no test, 1: put error, 2: ...)
    env['loggingMode'] = None                  # True puts the pilot in logging mode (will not run a job), False is a normal pilot. None means run as before
    env['updateServerFlag'] = True             # Switch on/off Panda server updates (for testing only), default is True
    env['globalSite'] = None                   # Site object
    env['globalWorkNode'] = None               # Work node object
    env['globalJob'] = None                    # Job object
    env['memory'] = None                       # For memory restrictions when asking the dispatcher for a new job (MB)
    env['proxycheckFlag'] = True               # True (default): perform proxy validity checks, False: no check
    env['wrapperFlag'] = False                 # True for wrappers that expect an exit code via return, False (default) when exit() can be used
    env['cleanupLimit'] = 2                    # Cleanup time limit in hours, see Cleaner.py
    env['logTransferred'] = False              # Boolean to keep track of whether the log has been transferred or not
    env['errorLabel'] = "WARNING"              # Set to FAILED when job recovery is not used
    env['nSent'] = 0                           # Throttle variable
    env['proxyguard'] = None                   # Global proxyguard object needed for the signal handler to be able to restore the proxy if necessary
    env['jobRecoveryMode'] = None              # When true, the pilot only runs job recovery, no payload download
    env['pilotToken'] = None                   # Pilot authentication token
    env['countryGroup'] = ""                   # Country group selector for getJob request
    env['workingGroup'] = ""                   # Working group selector for getJob request
    env['allowOtherCountry'] = False           #
    env['inputDir'] = ""                       # Location of input files (source for mv site mover)
    env['outputDir'] = ""                      # Location of output files (destination for mv site mover)
    env['jobIds'] = []                         # Global job id list
    env['stageoutStartTime'] = None            # Set when pilot receives "stageout" state message, used by looping job killer
    env['timefloor_default'] = None            # Time limit for multi-jobs in minutes (turned off by default)
    env['useCoPilot'] = False                  # CERNVM Co-Pilot framework (on: let Co-Pilot finish job, off: let pilot finish job (default))
    env['update_freq_server'] = 30*60          # Server update frequency, 30 minutes
    env['experiment'] = "ATLAS"                # Current experiment (can be set with pilot option -F <experiment>)
    env['getjobmaxtime'] = 3*60                # Maximum time the pilot will attempt to download a single job (seconds)
    env['pandaJobDataFileName'] = "pandaJobData.out" # Job definition file name    
    env['verifySoftwareDirectory'] = True      # Normally the softwre directory should be verified, but potentially not on an HPC system
    env['workerNode'] = None
    env['hasQueueData'] = None
    env['stdout_tail'] = ""
    env['stdout_path'] = ""

    # to test site mover
    env['copytool'] = 'gfal-copy'
    env['copytoolin'] = 'gfal-copy'

    # In case the PILOT_INITDIR file has not been created yet, which means that this module is being
    # used by pilot.py, it will be created here using the current directory as init dir
    path = os.path.join(os.getcwd(), "PILOT_INITDIR")
    if not os.path.exists(path):
        writeToFile(path, os.getcwd())
    env['pilot_initdir'] = readStringFromFile("PILOT_INITDIR")

    # data structure for job information in main loop
    # jobDic = {'prod/analy':[pid,job,os.getpgrp]}
    # (analysis label currently not used)
    env['jobDic'] = {}
        
    # some default values
    env['sitename'] = "testsite"
    env['workdir'] = "/tmp"
    env['appdir'] = ""
    env['dq2url'] = ""
    env['return_code'] = None
    env['return'] = 0

    return env

def readStringFromFile(filename):
    """ read exit code from file <workdir>/EXITCODE """

    s = ""
    if os.path.exists(filename):
        try:
            f = open(filename, "r")
        except Exception, e:
            print "Failed to open %s: %s" % (filename, str(e))
        else:
            s = f.read()
            print "Found string %s in file %s" % (s, filename)
            f.close()
    else:
        print "No string to report (file %s does not exist)" % (filename)

    return s

def storePilotInitdir(targetdir, pilot_initdir):
    """ Store the pilot launch directory in a file used by environment.py """

    # This function is used to store the location of the init directory in the init directory itself as well as in the
    # site work directory. The location file is used by environment.py to set the global env['pilot_initdir'] used
    # by the pilot and the Monitor

    # This function must be called before the global env variable is instantiated in the pilot

    path = os.path.join(targetdir, "PILOT_INITDIR")
    writeToFile(path, pilot_initdir)

def writeToFile(filename, s):
    """ write string s to file """

    try:
        f = open(filename, "w")
    except Exception, e:
        print "!!WARNING!!2990!! Could not open: %s, %s" % (filename, str(e))
    else:
        f.write("%s" % (s))
        f.close()
        print 'Wrote string "%s" to file: %s' % (s, filename)
