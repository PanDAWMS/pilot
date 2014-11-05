#!/bin/env python

################################################################################
# Trivial version of SimplePilot to be used with pilotWrapper
#
# The purpose of this version is to have a wrapper/pilot set 
# as less verbose as possible, just for fast tests.
# 
################################################################################

import sys, os, time, urllib2, cgi, commands, re, getopt
import pUtil as utils
import pickle

import tempfile
import stat
import shutil

import myproxyUtils

import subprocess

#baseURLSSL = 'https://voatlas57.cern.ch:25443/server/panda'
baseURLSSL = 'https://pandaserver.cern.ch:25443/server/panda'

SC_TimeOut   = 10

################################################################################
#               A u x i l i a r y     c l a s s e s 
################################################################################

import threading 

class ThreadHandler(threading.Thread):
       def __init__(self, period, function, *args): 
                threading.Thread.__init__(self)       # seting the engine to start the thread
                self.__stopevent = threading.Event()  # to stop the thread when needed
                self.__function = function            # function to be performed within the thread
                self.__period = period                # time between two calls to function
                self.__args = args
        
       def run(self):
                """executes actions periodically
                """

                n = 0
                while True:
                        time.sleep(1)
                        n += 1
                        if self.__stopevent.isSet():
                                # kills the thread
                                break
                        if n == self.__period:
                                self.__function(*self.__args)
                                n = 0
         

       def stop(self):
                """kills the thread
                """
                self.__stopevent.set() 

class StageOptions(object):
        # class to handle stage-in and stage-out options
        def __init__(self):
                self.pilotStageInSource = None
                self.pilotStageInDestination = None
                self.pilotStageInProtocol = None
                self.pilotStageOutSource = None
                self.pilotStageOutDestination = None
                self.pilotStageOutProtocol = None
                self.destinationURL = None

                #New Variables for Globus Online functions  
                #Stagein and Stageout are used to transfer standard files previous and after job
                #execusion. 
                #OutputGO, WorkingendpointGO and Data are used to transfer the result data files
                #to an final gridFTP Server.
                #The GO stands for Globus Online
                self.StageinGOsrc = None
                self.StageinGOdest = None
                self.StageoutGOsrc = None
                self.StageoutGOdest = None
                self.OutputGO = None
                self.WorkingendpointGO = None
                self.Data = None


################################################################################
#               A u x i l i a r      f u n c t i o n s                      
################################################################################

#def search_destinationURL(opts):
#    """inspects the string opts (jobParameters)
#    searching for the string like
#       --destinationURL=gsiftp://host:port/filename
#    If that string exists, it is extracted from opts
#    and the value is returned
#    """

#    destinationURL = None

#    index = opts.find('--destinationURL')
#    if index > -1:
#            whitesp_index = opts.find(' ',index)
#            if whitesp_index > -1:
#                    # the string is in the middle of opts
#                    destinationURL = opts[index : whitesp_index]
#            else:
#                    # the string is at the end of opts
#                    destinationURL = opts[index : ]

#            opts = opts.replace(destinationURL, '')
#            destinationURL = destinationURL.split('=')[1]

#    print '======== jobParameters after parsing for destinationURL'
#    print 'opts = %s' %opts
#    print 'destinationURL = %s' %destinationURL
#    print

#    return opts, destinationURL

def search_stage_opts(opts):
    """inspects the string opts (jobParameters)
    searching for any stage in/out option
    Strings are like: 
        --pilotStageInSource=<blah>
    """   

    stageopts = StageOptions()

    list_opts = opts.split()
    rest_opts = ''

    for opt in list_opts:
        if opt.startswith('--pilotStageInProtocol'):
                stageopts.pilotStageInProtocol = opt.split('=')[1]
        elif opt.startswith('--pilotStageInSource'):
                stageopts.pilotStageInSource = opt.split('=')[1]
        elif opt.startswith('--pilotStageInDestination'):
                stageopts.pilotStageInDestination = opt.split('=')[1]
        elif opt.startswith('--pilotStageOutProtocol'):
                stageopts.pilotStageOutProtocol = opt.split('=')[1]
        elif opt.startswith('--pilotStageOutSource'):
                stageopts.pilotStageOutSource = opt.split('=')[1]
        elif opt.startswith('--pilotStageOutDestination'):
                stageopts.pilotStageOutDestination = opt.split('=')[1]
        elif opt.startswith('--destinationURL'):
                stageopts.destinationURL = opt.split('=')[1]
        elif opt.startswith('--StageinGOsrc'):
                stageopts.StageinGOsrc = opt.split('=')[1]
        elif opt.startswith('--StageinGOdest'):
                stageopts.StageinGOdest = opt.split('=')[1]
        elif opt.startswith('--StageoutGOsrc'):  
                stageopts.StageoutGOsrc = opt.split('=')[1]
        elif opt.startswith('--StageoutGOdest'):
                stageopts.StageoutGOdest = opt.split('=')[1]
        elif opt.startswith('--OutputGO'):
                stageopts.OutputGO = opt.split('=')[1]
        elif opt.startswith('--WorkingendpointGO'):
                stageopts.WorkingendpointGO = opt.split('=')[1]
        elif opt.startswith('--Data'):   
                stageopts.Data = opt.split('=')[1]

        else:
                rest_opts += ' ' + opt

    return rest_opts, stageopts


def list_stagein_cmds(stageopts):

    list_sources = []
    list_destinations = []
    list_commands = []

    if not stageopts.pilotStageInSource:
        # no stage-in requested
        return []

    list_sources = stageopts.pilotStageInSource.split(',')     

    if stageopts.pilotStageInDestination:
        list_destinations = stageopts.pilotStageInDestination.split(',')     
    else:
        list_destinations = None

    if stageopts.pilotStageIntProtocol:
        list_protocols = stageopts.pilotStageInProtocol.split(',')
        if len(list_protocols) != len(list_sources):  # there was only one protocol
                list_protocols = list_protocols*len(list_destinations)
    else:
        list_protocols = None

    list_commands = map(stageincmd, list_sources, list_destinations, list_protocols)   
    return list_commands


def list_stageout_cmds(stageopts):

    list_sources = []
    list_destinations = []
    list_commands = []

    if not stageopts.pilotStageOutDestination:
        # no stage-out requested
        return []

    list_destinations = stageopts.pilotStageOutDestination.split(',')     

    # VERY IMPORTANT NOTE: dest is first argument and src is the second one !!!
    if stageopts.pilotStageOutSource:
        list_sources = stageopts.pilotStageOutSource.split(',')     
    else:
        list_sources = None

    if stageopts.pilotStageOutProtocol:
        list_protocols = stageopts.pilotStageOutProtocol.split(',')
        if len(list_protocols) != len(list_sources):  # there was only one protocol
                list_protocols = list_protocols*len(list_sources)
    else:
        list_protocols = None

    list_commands = map(stageoutcmd, list_destinations, list_sources, list_protocols)   
    return list_commands

def list_stagein_transfers(stageopts):
    """Creates list of transfer task during StageIn.
    """
        
    list_sources = []
    list_destinations = []
    list_transfers = [] 
    list_endpoint = []
    
    if not stageopts.StageinGOsrc:
        # no stage-in requested 
        return []
        
    list_sources = stageopts.StageinGOsrc.split(',')
        
    #list_destinations = stageopts.StageinGOdest.split(',')

    list_destinations = stageopts.WorkingendpointGO.split(',')

    #when more than 1 file as source, must repeate destination list to have right pairs.
    while len(list_sources) != len(list_destinations):
             list_destinations.append(list_destinations[0])


    list_transfers = map(stageincmdGO, list_sources, list_destinations)
    return list_transfers

def list_stageout_transfers(stageopts):
    """Creates list of transfer task during StageOut.
    """

    list_sources = []
    list_destinations = []
    list_transfers = []
    list_endpoint = []

    if not stageopts.StageoutGOdest:
        # no stage-out requested
        return []

    list_destinations = stageopts.StageoutGOdest.split(',')
    list_sources = stageopts.StageoutGOsrc.split(',')
    list_endpoint = stageopts.WorkingendpointGO.split(',')

    while len(list_sources) != len(list_destinations):
       list_destinations.append(list_destinations[0])
       list_endpoint.append(list_endpoint[0])

    list_transfers = map(stageoutcmdGO, list_destinations, list_sources, list_endpoint)
    return list_transfers

def list_output_transfers(stageopts):
    """Creates list of transfer task during StageIne.
    """

    list_data = []
    list_destinations = []
    list_transfers = []
    list_endpoint = []

    if not stageopts.Data:
        # no stage-out requested
        return []

    list_endpoint = stageopts.WorkingendpointGO.split(',')

    list_destinations = stageopts.OutputGO.split(',')

    list_data = stageopts.Data.split(',')

    while len(list_data) != len(list_destinations):
       list_destinations.append(list_destinations[0])
       list_endpoint.append(list_endpoint[0])

    list_transfers = map(outcmdGO, list_destinations, list_data, list_endpoint)
    print list_transfers
    return list_transfers


def getproxy(userID):
    """retrieves user proxy credential from Cerns myproxy server
    Uses the info of the submiter from panda before.
    Code partial extracted from MyProxyUtil
    """
    dn = userID
    if dn.count('/CN=') > 1:
       first_index = dn.find('/CN=')
       second_index = dn.find('/CN=', first_index+1)
       dn = dn[0:second_index]
    arg = ['myproxy-logon','-s','myproxy.cern.ch','--no_passphrase','-l',dn]
    print arg
    subprocess.call(arg)


def outcmdGO(dest, src, endp):
    """create command for output file using Globus Online. Uses job directory information
    to create output file path.
    """

    #Activate the destination endpoint, a gridFTP server
    destination = dest.split(':')[0]
    arg = ['gsissh', 'cli.globusonline.org', 'endpoint-activate','-g', destination]
    subprocess.call(arg)

    #Create transfer task 
    cmd = 'gsissh cli.globusonline.org scp -v %s:%s/%s %s'%(endp,jobdir, src, dest)
    
    return cmd

def stageincmdGO(src, dest):
    """create command for stage-in using Globus Online
    """
    
    #Activate the endpoint at source
    source = src.split(':')[0]
    arg = ['gsissh', 'cli.globusonline.org', 'endpoint-activate','-g', source]
    subprocess.call(arg)

    #Create transfer task
    cmd = 'gsissh cli.globusonline.org scp -v %s %s'%(src, dest)
    
    return cmd

def stageoutcmdGO(dest, src, endp):
    """create command for stage-out using Globus Online
    """

    #Activate the endpoint at destination
    destination = dest.split(':')[0]
    arg = ['gsissh', 'cli.globusonline.org', 'endpoint-activate','-g', destination]
    subprocess.call(arg)

    #Create transfer task
    cmd = 'gsissh cli.globusonline.org scp -v %s %s'%(src, dest)

    return cmd

def stageincmd(src, dest=None, protocol=None):
    """create command for stage-in
    """
    # FIXME: check the format of src and dest
    if protocol:
        cmd = protocol
    else:
        # default value
        cmd = 'globus-url-copy'

    if dest:
        cmd = cmd + ' %s %s' %(src, dest)
    else:
        filename = src.split('/')[-1]
        cmd = cmd + ' %s file://${PWD}/%s' %(src, filename) 
    return cmd

def stageoutcmd(dest, src=None, protocol=None):
    """create command for stage-in
    VERY IMPORTANT NOTE: dest is first argument and src is the second one !!!
    """
    # FIXME: check the format of src and dest
    if protocol:
        cmd = protocol
    else: 
        # default value
        cmd = 'globus-url-copy'

    if src:
        cmd = cmd + ' %s %s' %(src, dest)
    else:
        filename = dest.split('/')[-1]
        cmd = cmd + ' file://${PWD}/%s %s' %(filename, dest) 
    return cmd

def stageinwrapperGO(stageopts):

    list_transfers = list_stagein_transfers(stageopts)
    stageinfile = open('stagein.sh','w')
    print >> stageinfile, '#!/bin/bash '
    print >> stageinfile, ''
    for cmd in list_transfers:
        print >> stageinfile, cmd
    stageinfile.close()
    commands.getoutput('chmod +x stagein.sh')

def stageoutwrapperGO(stageopts):

    list_transfers = list_stageout_transfers(stageopts)
    stageoutfile = open('stageout.sh','w')
    print >> stageoutfile, '#!/bin/bash '
    print >> stageoutfile, ''
    for cmd in list_transfers:
        print >> stageoutfile, cmd

    # preventing stageout.sh from being executed twice
    # in case it is called from the transformation script
    # by adding a lock file
    print >> stageoutfile, 'touch ./stageoutlock'

    stageoutfile.close()
    commands.getoutput('chmod +x stageout.sh')

def outputwrapperGO(stageopts):

    list_transfers = list_output_transfers(stageopts)
    outputfile = open('output.sh','w')
    print >> outputfile, '#!/bin/bash '
    print >> outputfile, ''
    for cmd in list_transfers:
        print >> outputfile, cmd

    outputfile.close()
    commands.getoutput('chmod +x output.sh')


def stageinwrapper(stageopts):

    list_cmds = list_stagein_cmds(stageopts)

    stageinfile = open('stagein.sh','w') 
    print >> stageinfile, '#!/bin/bash '
    print >> stageinfile, ''
    for cmd in list_cmds:
        print >> stageinfile, cmd
    stageinfile.close()
    commands.getoutput('chmod +x stagein.sh')

def stageoutwrapper(stageopts):

    list_cmds = list_stageout_cmds(stageopts)

    stageoutfile = open('stageout.sh','w') 
    print >> stageoutfile, '#!/bin/bash '
    print >> stageoutfile, ''
    for cmd in list_cmds:
        print >> stageoutfile, cmd

    # preventing stageout.sh from being executed twice
    # in case it is called from the transformation script
    # by adding a lock file
    print >> stageoutfile, 'touch ./stageoutlock'

    stageoutfile.close()
    commands.getoutput('chmod +x stageout.sh')

 
#def gridFTP(fout, destinationURL):
#    """if destinationURL is specified (is not None or NULL)
#    the output file (fout) is staged-out with gridFTP
#    """
   
#    if destinationURL:
#        cmd = 'globus-url-copy file://%s %s' %(fout, destinationURL)
#        print
#        print 'output file staged-out with gridFTP'
#        print cmd  
#        print 
#        commands.getoutput(cmd)

def downloadGC():
    """Download Globus Connect file from Globus Online Server
    """
    print "Downloading Globus Connect to local Filesystem:"
    arg = ['curl','--connect-timeout','20','--max-time','120','-s','-S','http://confluence.globus.org/download/attachments/14516429/globusconnect','-o','globusconnect']
    print arg
    subprocess.call(arg)

def createEndpoint(endpoint):
    """Create the endpoint and returns the code for setup
    """
    print "Inside createEndpoint"
    print "Endpoint:"
    print endpoint
    arg = ['gsissh', 'cli.globusonline.org', 'endpoint-add','--gc', endpoint]
    print arg
    proc = subprocess.Popen(arg, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    return_code = proc.wait()
    i = 0
    for line in proc.stdout:
        print line.rstrip()
        i += 1
        if i == 3:
           pin = line

    return pin

def setupGC(endpoint):
    """Installing Globus Connect on working node. Creates endpoint, get the setup code and uses it on setup mode
    """
    print "inside setupGC"
    pin = createEndpoint(endpoint)
    pin = pin.strip()
    arg = ['sh', 'globusconnect', '-setup', pin]
    print arg
    subprocess.call(arg)

def removeEndpoint(endpoint):
    """Remove Endpoint used by Globus Connect
    """
    print "------Removing Endpoint--------"
    arg = ['gsissh', 'cli.globusonline.org', 'endpoint-remove', endpoint]
    print arg
    subprocess.call(arg)

def removeGC():
    """Removes  Globus Connect and configuration files on working node
    """
    print "-----Removing Globus Connect Files-----"
    arg = ['rm','-rf','~/.globusonline/','globusconnect']
    print arg
    subprocess.call(arg)

def startGC():
    """Start Globus Connect on working node
    """
    print "-----Running Globus Connect------"
    arg = ['sh', 'globusconnect', '-start']
    print arg
    subprocess.Popen(arg)

def stopGC():
    """Stop Globus Connect on working node
    """
    print "----Stopping Globus Connect-----"
    arg = ['sh', 'globusconnect', '-stop']
    print arg
    subprocess.call(arg)

def getQueueData():
    """get the info about the panda queue
    """

    cmd = "curl --connect-timeout 20 --max-time 120 -sS 'http://panda.cern.ch:25880/server/pandamon/query?tpmes=pilotpars&queue=%s&siteid=%s'" %(qname, site)
    # a list of queue specifications is retrieved from the panda monitor
    # Not the entire schedconf is retrieved, only what is setup in 
    # /monitor/Controller.py (line 81)
    queuedata = commands.getoutput(cmd)
    return queuedata

def getqueueparameter(par):
    """ extract par from s """

    queuedata = getQueueData()

    matches = re.findall("(^|\|)([^\|=]+)=",queuedata)
    for tmp,tmpPar in matches:
        if tmpPar == par:
            patt = "%s=(.*)" % par
            idx  = matches.index((tmp,tmpPar))
            if idx+1 == len(matches):
                patt += '$'
            else:
                patt += "\|%s=" % matches[idx+1][1]
            mat = re.search(patt,queuedata)
            if mat:
                return mat.group(1)
            else:
                return ''
    return ''

################################################################################

try:
    site = os.environ['PANDA_SITE']
except:
    site = ''

print "dataPilot.py: Running python from",sys.prefix,", platform", sys.platform, ", version", sys.version
try:
    print "Python C API version", sys.api_version
except:
    pass

jobdir = ''
grid = ''
batch = ''
logtgz = ''
sleeptime = 20
tpsvn = 'http://www.usatlas.bnl.gov/svn/panda/autopilot/trunk'

### command-line parameters
# First pull out pilotpars. Because getopt.getopt does not handle quoted params properly.
pat = re.compile('.*--pilotpars=(".*[^\\\]{1}").*')
optstr = ''
for a in sys.argv[1:]:
    optstr += " %s" % a
mat = pat.match(optstr)
pilotpars = ''
if mat:
    pilotpars = mat.group(1)
    print "pilotpars: -->%s<--" % pilotpars
    optstr = optstr.replace('--pilotpars=%s'%pilotpars,'')
    optlist = optstr.split()
    print "Options after removing pilotpars:",optlist
else:
    optlist = sys.argv[1:]


# protect the option list from extraneous arguments slapped on by batch system,
# in particular when doing MPI submission

# reserve the lists, as we'll need those out of the loop scope
opts=[]
args=[]

valid = ("site=","tpid=","schedid=","pilotpars=","transurl=","queue=")

for opt in optlist:
    try:
       opt,arg = getopt.getopt( [opt], "", valid)
       opts += opt
       args += arg
    except:
       print "Warning: possible extraneous option skipped at index ", optlist.index(opt)

site = ''
tpid = '?'
schedid = '?'
transurl = ''
qname = ''

for o, a in opts:
    if o == "--site":
        site = a
    if o == "--tpid":
        tpid = a
    if o == "--schedid":
        schedid = a
    if o == "--queue":
        qname = a
    if o == "--pilotpars":
        pilotpars = a
    if o == "--transurl":
        transurl = a

if site != '':
    print "Pilot will run with site identity", site
else:
    site = 'TWTEST'
    print "Pilot will run with default site identity", site

print "Running on queue", qname
## Check that curl is present
cmd = 'curl -V'
out = commands.getoutput(cmd)
if out.find('https') < 0:
    print "dataPilot: ERROR: curl not found or not https-capable. Aborting."
    print "!!FAILED!!2999!!trivialPilot: curl not found or not https-capable"
    sys.exit(1)

host = commands.getoutput('hostname -f')
user = commands.getoutput('whoami')
workdir = ''


def getJob():
    """
    Get a job from the Panda dispatcher
    """

    global param
    global glexec_flag
    ## Request job from dispatcher
    print "==== Request Panda job from dispatcher"

    # When glexec is True, then it is needed to read 
    # the value of 'credname' and 'myproxy' from the panda server
    # This info is delivered only if getProxyKey is "TRUE"
    # If glexec is needed or not in this site is known
    # thru parameter 'glexec' included in queuedata.txt
    glexec_flag = getqueueparameter('glexec').capitalize()

    data = {
        'siteName':site,
        'node':host,
        'prodSourceLabel':'user', #'test' 
        'computingElement':qname,
        'getProxyKey':glexec_flag,
        }
    
    maxtries = 3
    ntries = 0
    status = 99
    PandaID = 0
    while status != 0:
        ntries += 1
        if ntries > maxtries: 
            break

        # performs a loop while dispatcher has no job (status==20)
        # or until the maxtime (10 min) passes
        totaltime = 0
        while totaltime < 10*60:
            print 'trial after %s seconds' %totaltime
            status, param, response = utils.toServer(baseURLSSL,'getJob',data,os.getcwd())
            if status != 20:
                 break
            time.sleep(60)
            totaltime += 60
        print 'results from getJob: '
        print '   status = %s' %status
        print '   param = %s' %param
        print '   response = %s' %response
        if status == 0:
            print "==== Job retrieved:"
            ## Add VO as a param. Needs to go into schema. $$$
            param['vo'] = 'OSG'
            PandaID = param['PandaID']
            for p in param:
                print "   %s=%s" % ( p, param[p] )
        elif status == SC_TimeOut:
            # allow more tries
            print "Sleep for %s" % sleeptime
            time.sleep(sleeptime)
        else:
            break

    return status, PandaID

def runJob():
    """
    Run the assigned job
    """
    print "==== Running PandaID", param['PandaID']
    if transurl != '':
        # Take transformation URL from the pilot wrapper parameters
        trf = transurl
    else:
        trf = param['transformation']
        #if not trf.startswith('http'):
        #    print "Bad transformation field '%s': should be url." % trf
        #    print "!!FAILED!!2999!!Bad transformation field, should be url"
        #    sys.exit(1)

    pid = param['PandaID']
    # set up workdir
    global cleanup
    global workdir
    global jobdir
    global logtgz
    if workdir != '':
        jobdir = workdir+'/panda-'+pid
        out = commands.getoutput('mkdir %s'%jobdir)
        cleanup = True
    else:
        workdir = './'
        jobdir = os.getcwd()
        cleanup = False
    print "Job directory is", jobdir

    opts = param['jobPars']
    #opts, destinationURL = search_destinationURL(opts)
    opts, stageopts = search_stage_opts(opts) 
    #destinationURL = stageopts.destinationURL
    #stageinwrapper(stageopts)
    #stageoutwrapper(stageopts)
    

    #Retrieve user proxy credential
    userDN = param['prodUserID']
    getproxy(userDN)
   
    endpoint = stageopts.WorkingendpointGO
        
    print "Launching donwloadGC function"
    downloadGC()
    time.sleep(10)
    print "lauching setupGC function"
    setupGC(endpoint)

    print "Launching Globus Connect with -Start"
    startGC()
    time.sleep(30)

    stageinwrapperGO(stageopts)
    stageoutwrapperGO(stageopts)
    outputwrapperGO(stageopts)

    ## Set environment variables for job use
    env = "export PandaID=%s;" % pid
    env += "export PandaSite=%s;" % site
    env += "export QueueName=%s;" % qname
    env += "export PandaWorkdir=%s;" % workdir
    env += "export dispatchBlock=%s;" % param['dispatchDblock']
    env += "export destinationBlock=%s;" % param['destinationDblock']
    if param.has_key('swRelease'):
        env += "export swRelease=%s;" % param['swRelease']
    if param.has_key('homepackage'):
        env += "export homepackage=%s;" % param['homepackage']
    if param.has_key('logFile'):
        env += "export logFile=%s;" % param['logFile']
        logtgz = param['logFile']
    if not os.environ.has_key("APP"):
        if os.environ.has_key("OSG_APP"): env += "export APP=%s;" % os.environ["OSG_APP"]
        elif os.environ.has_key("VO_ATLAS_SW_DIR"): env += "export APP=%s;" % os.environ["VO_ATLAS_SW_DIR"]
    if trf.startswith('http'):
        cmd = '%s cd %s; curl --insecure -s -S -o run.sh %s; chmod +x run.sh ' % ( env, jobdir, trf )
    else:
        # if the transformation is not an URL then it is the path to the executable
        # in this case the run.sh script is created by hand just pointing to this path
        cmd = '%s cd %s; echo "#!/bin/bash" > run.sh; echo "%s \$@" >> run.sh; chmod +x run.sh' %(env, jobdir, trf)
    st, out = commands.getstatusoutput(cmd)

    if st != 0:
        print "!!FAILED!!2999!!Error in user job script setup"
        print "ERROR in script setup"
        print "Command:", cmd
        print "Output:", out
        return st


    # Adding pilotpars, when it is not empty, to the options list 
    if pilotpars != '':
        opts += ' --pilotpars=%s' %pilotpars

    ## -------------------------------------------------
    ##         Run the job
    ## -------------------------------------------------

    #### BEGIN TEST ####
    #cmd = 'cd %s; %s ./run.sh %s' % ( jobdir, env, opts )
    #cmd = 'cd %s; %s ./stagein.sh; ./run.sh %s; ./stageout.sh' % ( jobdir, env, opts )
    
    # The script stageout.sh is executed only if there is no lock file.
    # This lock file can be created by the script itself if it has been invoked previously 
    # from the transformation script

    cmd = 'cd %s; %s ./stagein.sh; ./run.sh %s; [ ! -f ./stageoutlock ] && ./stageout.sh; ./output.sh' % ( jobdir, env, opts )
    # FIXME
    # in the future, instead of a monster cmd string, 
    # it should be in a wrapper script or any other solution
    
    #### END TEST ####
    # executing the job
    print 'command to be executed is = %s' %cmd

    global glexec_flag
    out=''
    if glexec_flag:
        print "executing payload under glexec"

        myproxy_server = param['myproxy']
        MyPI = myproxyUtils.MyProxyInterface(myproxy_server)
        MyPI.userDN = param['prodUserID']
        MyPI.credname = param['credname'] 
        
        glexec = myproxyUtils.executeGlexec(MyPI)
        glexec.payload = cmd 
        glexec.opts = opts
        glexec.execute()   
        st = glexec.status
        out = glexec.output
    else:
        ######## BEGIN TEST #######
        #st, out = commands.getstatusoutput(cmd)
        if 'LBNE_DAYA' in site:   #FIXME: this is just a temporary solution !!
                import subprocess
                popen = subprocess.Popen(cmd, 
                                         shell=True, 
                                         stdout=subprocess.PIPE, 
                                         stderr=subprocess.STDOUT)
                out = ''
                for line in iter(popen.stdout.readline, ""):
                        #print line         # this is for the stdout
                        print line[:-1]     # this is for the stdout, and removing the final \n
                        out += line + '\n'  # this is to record it in a file
                        ###out += line        # this is to record it in a file
                st = popen.wait()

        else:
                st, out = commands.getstatusoutput(cmd)
        ######## END TEST #######

    # writing the output in job.out
    fout = '%s/job.out'%jobdir
    fh = open(fout,'w')
    fh.write(out+'\n')
    fh.close()

    print "\n==== Job script output written to", fout

    # If destinationURL was specified, 
    # stage-out the output file with gridFTP
    #gridFTP(fout, destinationURL)

    # ------------------------------------------------------
    # analyzing the output searching for error messages
    ############  BEGIN TEST  ##########
    if 'LBNE_DAYA' in site:   #FIXME: this is just a temporary solution !!
        jobstat = analyzeoutput_dayabay(jobdir)
    else:
        jobstat = analyzeoutput_default(st, jobdir)
    ############  END TEST  ##########

    if jobstat == 0:
        print "Final job status: success"
    else:
        print "Final job status: error code", jobstat
    # ------------------------------------------------------

    #print '\n======== Job script output:\n',out,'\n======== End of job script output\n'

    stopGC()
    time.sleep(20)
    removeEndpoint(endpoint)
    time.sleep(10)
    removeGC()

    return jobstat

def analyzeoutput_default(st, jobdir):

    jobstat = 0
    if st != 0:
        print "ERROR: trivialPilot: Job script failed. Status=%s" % st
        print "======== Job script run.sh content:"
        print commands.getoutput('cat run.sh')
    else:
        print "!!INFO!!0!!Job script completed OK. Status=0"
    errcodes = commands.getoutput("grep '!!FAILED!!' %s/job.out"%jobdir)
    if len(errcodes) > 0:
        print "---- Synposis of FAILED messages in job:"
        print errcodes
    warncodes = commands.getoutput("grep '!!WARNING!!' %s/job.out"%jobdir)
    if len(warncodes) > 0:
        print "---- Synposis of WARNING messages in job:"
        print warncodes
    infocodes = commands.getoutput("grep '!!INFO!!' %s/job.out"%jobdir)
    if len(infocodes) > 0:
        print "---- Synposis of INFO messages in job:"
        print infocodes
    pat = re.compile('.*!!FAILED!!([0-9]+)!!(.*)$')
    mat = pat.search(errcodes)
    if mat:
        jobstat = 1
    return jobstat

def analyzeoutput_dayabay(jobdir):

    # possible error messages in the output of a dayabay job
    # "ERROR":1000001 -- removed on David Jaffe's request on 20110513
    errors_msgs = {"FATAL":			1000002,
                   "segmentation violation":	1000003,
                   "IOError":			1000004,
                   "ValueError":		1000005}
    
    # command to search for any of those error messages in the output
    cmd = 'egrep "%s" %s/job.out' %('|'.join(errors_msgs.keys()),jobdir)
    errline = commands.getoutput(cmd)
    if errline:
        # errline is not empty => some of the error messages was detected
        print 'errline: ', errline
        for err, code in errors_msgs.iteritems():
            if err in errline:
                # jobstat is the value corresponding with that key in the dictionary                    
                # the key is the first of the error messages found 
                print 'err and code: ', err, code
                return code
    # if everything was fine...
    return 0


def shutdown(jobstat):
    """
    Report to dispatcher, clean up and shut down the pilot
    """
    global param
    pid = param['PandaID']
    print "==== Cleanup for PandaID", pid
    if cleanup:
        cmd = 'rm -rf %s'%jobdir
        print cmd
        out = commands.getoutput(cmd)
    ## Report completion to dispatcher
    if jobstat == 0:
        state = 'finished'
    else:
        state = 'failed'
    endJob(pid, state, jobstat)

def endJob(pid, state, jobstat):
    data = {}
    data['node'] = host
    data['siteName'] = site
    data['jobId'] = pid
    data['schedulerID'] = schedid
    data['pilotID'] = os.environ.get('GTAG', tpid)
    data['state'] = state
    data['timestamp'] = utils.timeStamp()
    data['transExitCode'] = jobstat
    data['computingElement'] = qname

    print "== Updating Panda with completion info"
    status, pars, response = utils.toServer(baseURLSSL,'updateJob',data,os.getcwd())
    if status != 0:
        print "Error contacting dispatcher to update job status: return value=%s" % status
    else:
        if jobstat == 0:
            print "==== PandaID %s successful completion reported to dispatcher" % pid
            print "!!FINISHED!!0!!PandaID %s done" % pid
        else:
            print "==== PandaID %s failed completion reported to dispatcher" % pid
            print "!!FAILED!!2999!!PandaID %s done" % pid
    print "==== Directory at job completion:"
    print commands.getoutput('pwd; ls -al')

def updateJob(pid):
    data = {}
    data['node'] = host
    data['siteName'] = site
    data['jobId'] = pid
    data['schedulerID'] = schedid
    data['pilotID'] = os.environ.get('GTAG', tpid)
    data['state'] = 'running'
    data['timestamp'] = utils.timeStamp()

    # update server
    print "== Updating Panda with running state"
    status, pars, response = utils.toServer(baseURLSSL,'updateJob',data,os.getcwd())
    if status != 0:
        print "Error contacting dispatcher to update job status: status=%s" % status
    else:
        print "==== PandaID %s running status reported to dispatcher" % pid

# ====================================================================== 

if __name__ == "__main__":
    status, PandaID = getJob()
    if status == 0:
        status = updateJob(PandaID)
        heartbeat = ThreadHandler(30*60, updateJob, PandaID) 
        heartbeat.start()
        status = runJob()
        heartbeat.stop()
        shutdown(status)
