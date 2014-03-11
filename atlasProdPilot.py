#!/bin/env python

import sys, os, time, urllib2, cgi, commands, re, getopt, traceback
import random
#import PilotUtils as utils
import pUtil as utils
from SiteInformation import SiteInformation

baseURLSSL = ''

try:
    site = os.environ['PANDA_SITE']
except:
    site = ''

print "atlasProdPilot.py: Running python from",sys.prefix,", platform", sys.platform, ", version", sys.version
try:
    print "Python C API version", sys.api_version
except:
    pass

os.environ['PilotHomeDir'] = commands.getoutput('pwd')
tpsvn = 'http://www.usatlas.bnl.gov/svn/panda/autopilot/trunk'

### command-line parameters
# First pull out pilotpars. Because getopt.getopt does not handle quoted params properly.
pat = re.compile('.*--pilotpars="(.*[^\\\]{1})".*')
optstr = ''
for a in sys.argv[1:]:
    optstr += " %s" % a
mat = pat.match(optstr)
pilotPars = ''
if mat:
    pilotPars = mat.group(1)
    print "optstr: -->%s<--" % optstr
    print "pilotPars: -->%s<--" % pilotPars
    optstr = optstr.replace('--pilotpars=\"%s\"'%pilotPars,'')
    optlist = optstr.split()
    print "Options after removing pilotPars:",optlist
else:
    optlist = sys.argv[1:]
opts, args = getopt.getopt(optlist, "",("site=","tpid=","schedid=","pilotpars=","queue=","user=","countrygroup=","allowothercountry="))
site = ''
tpid = '?'
schedid = '?'
qname = ''
_user = ''
_countrygroup = ''
_allowothercountry = ''

for o, a in opts:
    if o == "--site":
        site = a
    if o == "--tpid":
        tpid = a
    if o == "--schedid":
        schedid = a
    if o == "--pilotpars":
        pilotpars = a # not used!
    if o == "--queue":
        qname = a
    if o == "--user":
        _user = a
    if o == "--countrygroup":
        _countrygroup = a
    if o == "--allowothercountry":
        _allowothercountry = a

if _user != "" and not "--user" in pilotPars:
    pilotPars += " --user=%s" % _user
if _countrygroup != "" and not "--countrygroup" in pilotPars:
    pilotPars += " --countrygroup=%s" % _countrygroup
if _allowothercountry != "" and not "--alowothercountry" in pilotPars:
    pilotPars += " --allowothercountry=%s" % _allowothercountry
print "Final pilotPars:", pilotPars

# FIXME : check these variables are really passed as input
#         and abort and/or raise exception otherwise

if schedid != '?':
    os.environ['PANDA_JSID'] = schedid
if tpid != '?':
    os.environ['GTAG'] = tpid
if site != '':
    print "Pilot will run with site identity", site
else:
    site = 'TWTEST'
    print "Pilot will run with default site identity", site

if qname != '':
    print "Queue specified by command option as",qname
else:
    print "!!WARNING!!2999!!Queue not specified by command line option"

## Check that curl is present
cmd = 'curl -V'
out = commands.getoutput(cmd)
if out.find('https') < 0:
    print "atlasProdPilot: ERROR: curl not found or not https-capable. Aborting."
    print "!!FAILED!!2999!!atlasProdPilot: curl not found or not https-capable"
    sys.exit(1)

host = commands.getoutput('hostname -f')
user = commands.getoutput('whoami')

# --------------------------------------------------# 
#        R U N   T H E   J O B                      #
# --------------------------------------------------# 

def runJob():
    """
    Run the assigned job
    """

    global cleanup
    global jobdir

    print "==== Running pilot"

    os.environ['pilotPars']   = pilotPars
    os.environ['PandaSite']   = site
    os.environ['QueueName']   = qname
    os.environ['PilotID']     = tpid
    os.environ['SchedulerID'] = schedid

    ## Report to the monitor that this pilot ID is live
    ReportToMonitor(tpid, host)

    ## Read the queue params, and return the queuedata
    queuedata = ReadQueueParams()

    # set up the working directory
    jobdir, workdir, cleanup = Workdir()

    # download the transformation script and run it
    # return the jobstat value
    return DownloadAndExecute(queuedata, workdir)

def ReportToMonitor(tpid, host):
    """Report to the monitor that this pilot ID is live
    """

    if tpid != "?":
        print "Report pilot ID %s is live" % tpid
        si = SiteInformation()
        sslCertificate = si.getSSLCertificate()
        curl_url = 'http://panda.cern.ch:25980/server/pandamon/query?tpmes=setpilotlive&tpid=%s&host=%s' % (tpid,host)
        curl_cmd = 'curl --connect-timeout 20' \
                   + ' --max-time 120' \
                   + ' --cacert %s' % (sslCertificate) \
                   + ' -sS "%s"' % curl_url
        print curl_cmd
        curl_output = commands.getoutput(curl_cmd)
        print curl_output
    else:
        print "pilot ID is unknown"

def ReadQueueParams():

    queuedata = ""
    verified = False

    si = SiteInformation()
    qdfname = si.getQueuedataFileName(check=False)
#    qdfname = "%s/queuedata.dat" % (os.environ['PilotHomeDir'])
#    qdfname = "%s/queuedata.json" % (os.environ['PilotHomeDir'])
    if os.path.exists(qdfname):
        print "queuedata already downloaded"
        verified, queuedata = readqueuedata(qdfname, first_trial=True)
    else:
        print "Downloading queuedata in atlasProdPilot"

        # download queuedata and verify it
        extension = utils.getExtension(alternative='dat')
        if extension == "json":
            _ext = extension
        else:
            _ext = "pilot"
        curl_url = 'http://pandaserver.cern.ch:25085/cache/schedconfig/%s.pilot.%s' % (qname, _ext)
        si = SiteInformation()
        sslCertificate = si.getSSLCertificate()
        cmd = 'curl --connect-timeout 20 --max-time 120 --cacert %s -sS "%s" > %s' % (sslCertificate, curl_url, qdfname)
        print "Executing command: %s" % (cmd)
        try:
            ret, rs = commands.getstatusoutput(cmd)
        except Exception, e:
            print "!!WARNING!!1999!! Failed with curl command: %s" % str(e)
        else:
            if ret != 0:
                print "!!WARNING!!1999!! Failed with curl command: %d" % (ret)
            else:
                verified, queuedata = readqueuedata(qdfname, False, 1, 1)
                if verified:
                    print "Queuedata verified by wrapper"
                else:
                    print "Queuedata not verified (pilot will try again later)"
    VerifyQueuedata(verified)
    return queuedata

def readqueuedata(qdfname, first_trial, i=None, N=None):
    """read back the queuedata to verify its validity

    if this is the first trial, i and N are not definied
    """

    verified = False
    try:
        f = open(qdfname, "r")
    except Exception, e:
        print "!!WARNING!!1999!! Open failed with %s" % str(e)
    else:
        queuedata = f.read()
        if type(queuedata) == str:
            print "Queuedata is of type str"
        elif type(queuedata) == unicode:
            print "Queuedata is of type unicode (will convert to str)"
            try:
                queuedata = queuedata.encode('ascii')
            except Exception, e:
                print "Failed to convert to ascii: %s" % (e)
            else:
                print "queuedata converted to ascii str"
        else:
            print "Unknown queuedata type: %s" % str(type(queuedata))

        try:
            q_split = queuedata.split('=')[0]
        except:
            print "split failed, bad queuedata"
        else:
            if q_split != 'appdir':
                if first_trial:
                    print '!!WARNING!!1999!! Bad queuedata'
                else:
                    print 'curl command returned bad queuedata (attempt %d/%d)' % (i, N)
            else:
                print "Queue specific parameters:", queuedata
                verified = True
        f.close()

    return verified, queuedata

def VerifyQueuedata(verified):
    """verify the information received from the server throught the queuedata info
    """

    if not verified:
        print "Did not download queuedata from server"
    else:
        # is the site online?
        si = SiteInformation()
        status = si.readpar('status')
        if status.upper() == "OFFLINE":
            print "Site is %s, ignore" % (status.upper())
            # sys.exit(2)
        else:
            print "Site is", status

def Workdir():
   
    getworkdir = getWorkdir()
    workdir = ''

    if getworkdir != '':
        print "Setting workdir to %s from queue DB wntmpdir setting" % getworkdir
        workdir = getworkdir
    if workdir != '':
        jobdir = workdir+'/panda-'+commands.getoutput('uuidgen')
        os.system('mkdir %s'%jobdir)
        cleanup = True
    else:
        if os.environ.has_key('EDG_WL_SCRATCH'):
            jobdir = os.environ['EDG_WL_SCRATCH']
        elif os.environ.has_key('OSG_WN_TMP'):
            jobdir = os.environ['OSG_WN_TMP']
        else:
            jobdir = os.getcwd()
        workdir = jobdir
        cleanup = False
    print "Job directory is", jobdir
    print "Work directory is", workdir
    os.environ['PandaWorkdir'] = workdir
    if not os.environ.has_key("APP"):
        if os.environ.has_key("OSG_APP"): 
            os.environ['APP'] = os.environ["OSG_APP"]
        elif os.environ.has_key("VO_ATLAS_SW_DIR"): 
            os.environ['APP'] = os.environ["VO_ATLAS_SW_DIR"]

    return jobdir, workdir, cleanup

def getWorkdir():
    """obtaining the working directory
    """

    si = SiteInformation()
    getworkdir = si.readpar('wntmpdir')

    ## If workdir contains env var, translate it
    pat = re.compile('(.*)(\$[^\/]+)(.*)')
    mat = pat.match(getworkdir)
    if mat:
        envvar = mat.group(2)
        envvar = envvar.replace('$','')
        if os.environ.has_key(envvar):
            envval = os.environ[envvar]
            getworkdir = "%s%s%s" % ( mat.group(1), envval, mat.group(3) )
            print "Translated wntmpdir with env var to", getworkdir
        else:
            print "WARNING: wntmpdir contains env var %s that is undefined" % envvar
 
    return getworkdir

def DownloadAndExecute(queuedata, workdir):
    """downloads and executes the transformation script

    queuedata has to be known since it is used by the script that is executed in this function
    """

    jobstat = 0
    try:
        jobstat = trans_atlasprod_thin(queuedata, workdir)

        print "Final job status from transformation wrapper:", jobstat
        if jobstat != 0:
            if os.environ.has_key('PandaID'):
                print "Sending Panda status=failed"
                pid = os.environ['PandaID']
                endJob(pid, 'failed', jobstat)
            else:
                print "==== Pilot failed to complete"
                time.sleep(5)
    except Exception, e:
        type, value, tback = sys.exc_info()
        if type:
            print "       %s: %s" % ( type, value )
            tblines = traceback.extract_tb(tback)
            for l in tblines:
                fname = l[0][l[0].rfind('/')+1:]
                print "       %s: %s: %s: %s" % ( fname, l[1], l[2], l[3] )
        if jobstat == 0:
            jobstat = 2999
        if str(e) == "1111":
            print "atlasProdPilot: Either the pilot could not pick up a job or it could not create the job workdir"
        else:
            print "!!FAILED!!%s!! atlasProdPilot: Job script error: %s" % (jobstat, e)
        print "atlasProdPilot is waiting 20s for pilot to finish"
        time.sleep(20)
        if os.environ.has_key("PandaID"):
            print "Sending Panda status=failed"
            pid = os.environ['PandaID']
            endJob(pid, 'failed', jobstat)
        else:
            print "==== Pilot failed to complete"

    return jobstat

#--------------------------------------------------# 
#  A N C I L L A R Y     F U N C T I O N S         #
#--------------------------------------------------# 

def getScript(libcode): 
    """get a script to be run from the libcode input option.
    If libcode has only one item, that is picked up.
    If libcode has two items, the first one is selected 
    with a probability of 99%, and the second one 1% of times. 
    """

    scripts = libcode.split(',')
    if len(scripts) == 1:
        script_index = 0
    elif len(scripts) == 2:
        # select randomly one of the two scripts
        # with a probability of 99% to pick up the first one
        weights = [
            (1, 0.01),
            (0, 0.99)
        ]
        script_index = getScriptIndex(weights)
    else:
        print "Current weight function only handles two libcode scripts"
        script_index = 0

    print 'Got script index:', script_index
    s = scripts[script_index]
    print 'Downloading script:', s
    return s

def getScriptIndex(weights):
    """function returning a random number following a
    provided distribution of weights
    weights is a list a tuples (value, weight)

    Due the nature of the algorithm implemented
    the weights have to be sorted from the lower one to the higher one
    """
 
    n = random.uniform(0, 1)
    for (value, weight) in weights:
            if n < weight:
                    break
            n = n - weight
    return value

#--------------------------------------------------# 
# O L D    T R A N S F O R M A T I O N   C O D E   #
#--------------------------------------------------# 

def trans_atlasprod_thin(queuedata, workdir):
       
    jobstat = 0

    puser = ""

    print "--- start ---"

    if os.environ.has_key('HOSTNAME'):
        print "%s at %s " % ( time.strftime("%a %b %d %H:%M:%S %Y", time.gmtime(time.time())), os.environ['HOSTNAME'] )
    else:
        print commands.getoutput("echo `date` at `hostname`")

    try:
        site = os.environ['PandaSite']
    except:
        print "!!FAILED!!6999!!PandaSite env var must be supplied to trans script"
        jobstat = 1
    
    if not jobstat:
        jobstat, libcode, pilotsrcurl, puser, countrygroup, allowothercountry = process_pilotpars()

    # Pull down the production pilot scripts
    if not jobstat:
        if pilotsrcurl == "":
            print "!!FAILED!!6999!!No URL for production pilot scripts specified"
            jobstat = 1
        if libcode == "":
            print "!!WARNING!!6999!!Auxiliary lib code not specified on --libcode option"
   
        ## BEGIN TEST ###### 
        ## is pilotsrcurl a list?
        #pilotsrcurl_list = pilotsrcurl.split(',')
        #print 'pilotsrcurl list:', pilotsrcurl_list
    
        #s = getScript(libcode)
    
        #for pilotsrcurl_i in pilotsrcurl_list:
        #    print 'Trying src url:', pilotsrcurl_i
        #    sfile = os.path.basename(s)
        #    scripturl = "%s/%s" % ( pilotsrcurl_i, s )
        #    verified = downloadscript(scripturl, sfile) 
    
        #    if verified:
        #        print "Script verified:", s
        #        break
        #    else:
        #        print "Script could not be verified:", s

        #if not verified:
        #    print "!!FAILED!!6999!! Failed to download/unpack %s" % (sfile)
        #    jobstat = 3001
        ## END TEST ###### 
    
        print "Listing after code retrieval:"
        cmd = '/bin/ls -al'
        out = commands.getoutput(cmd)
        print out
   
    # read the params from the queuedata 
    # if everything is OK reading the params,
    # then run pilot.runMain
    if not jobstat:
        runpars, jobstat = getrunpars(queuedata, workdir, puser, countrygroup, allowothercountry, libcode)
    if not jobstat:
        jobstat = runMain(runpars)
    
    print "--- Clean up", time.strftime("%a %b %d %H:%M:%S %Y", time.gmtime(time.time()))
    print "--- Finished", time.strftime("%a %b %d %H:%M:%S %Y", time.gmtime(time.time()))
 
    return jobstat

def process_pilotpars():

    jobstat = 0
    libcode = ""
    pilotsrcurl = ""
    puser = ""
    countrygroup = ""
    allowothercountry = ""
    pilotPars = os.environ['pilotPars']
    
    if pilotPars == "":
        print "!!FAILED!!6999!!No pilot wrapper parameters delivered with --pilotpars option"
        jobstat = 1
    else: 
        # Process the parameters
        print "pilotPars:", pilotPars
        optlist = pilotPars.split()
        print "optlist:", optlist
        optstr = "a:l:q:u:m:g:r:j:f:"
        opts, args = getopt.getopt(optlist, optstr,("script=","libcode=","pilotsrcurl=","user=","countrygroup=","allowothercountry="))
        print "opts:", opts
        for o, a in opts:
            if o == "--libcode":
                libcode = a
            if o == "--pilotsrcurl":
                pilotsrcurl = a
            if o == "--user":
                puser = a
            if o == "--countrygroup":
                countrygroup = a
            if o == "--allowothercountry":
                allowothercountry = a
    return jobstat, libcode, pilotsrcurl, puser, countrygroup, allowothercountry

def downloadscript(scripturl, sfile):
    """download the script with curl
    """

    verified = False

    si = SiteInformation()
    sslCertificate = si.getSSLCertificate()
    cmd = "curl  --connect-timeout 20 --max-time 120 --cacert %s %s -s -S -o %s" % (sslCertificate, scripturl, sfile)
    print cmd

    max_trials = 2
    for trial in range(1, max_trials+1):
        st, out = commands.getstatusoutput(cmd)
        print "%s: %s" % (st, out)
        if st != 0:
            print "Error retrieving script with curl (attempt %d/%d)" % (trial, max_trials)
        else:
            cmd = "chmod +x %s; /bin/ls -al %s" % ( sfile, sfile )
            print cmd
            stt, out = commands.getstatusoutput(cmd)
            print "%s: %s" % (stt, out)
            if stt == 0:
                if sfile.find('.tar.gz') > 0:
                    print "Untarring", sfile
                    cmd = "tar xzf %s" % sfile
                    print cmd
                    ec, out = commands.getstatusoutput(cmd)
                    print out
                    if ec != 0:
                        print "tar failed, curl did not return valid archive (attempt %d/%d)" % (trial, max_trials)
                    else:
                        cmd = "chmod +x *.py *.sh"
                        print cmd
                        ec, out = commands.getstatusoutput(cmd)
                        print ec, out
                        verified = True
                        break
                else:
                    verified = True
                    break
            else:
                print "chmod failed (attempt %d/%d)" % (trial, max_trials)
        if verified:
            break

    return verified

def getrunpars(queuedata, workdir, puser, countrygroup, allowothercountry, libcode):

    jobstat = 0

    appdir = ""
    reldir = ""
    sitesetup = ""
    releases = ""
    datadir = ""
    dq2url = ""
    
    queue = os.environ['QueueName']
    print "atlasProdPilot running on queue", queue

    if os.environ.has_key('PANDA_URL_SSL'):
        global baseURLSSL
        baseURLSSL = os.environ['PANDA_URL_SSL']
        pat = re.compile('^(.*):([0-9]+)/.*$')
        mat = pat.match(baseURLSSL)
        if mat:
            pandaURL = mat.group(1)
            pandaPort = mat.group(2)
        else:
            print "!!FAILED!!6999!!Bad Panda URL %s" % baseURLSSL
            jobstat = 1
    else:
        print "!!FAILED!!6999!!PANDA_URL_SSL undefined"
        jobstat = 1

    # reading setup from queuedata
    try:
        # checking if queuedata is not None
        queuedata
    except:
        queuedata = ""
    else:
        si = SiteInformation()
        appdir  = si.getpar('appdir',queuedata)
        datadir = si.getpar('datadir',queuedata)
        dq2url  = si.getpar('dq2url',queuedata)

        par = si.getpar('gatekeeper',queuedata)
        if par != "":
            print "Setting ATLAS_CONDDB to", par
            os.environ["ATLAS_CONDDB"] = par

    print "Setting RUCIO_ACCOUNT to pilot"
    os.environ["RUCIO_ACCOUNT"] = 'pilot'

    if datadir == "":
        if os.environ.has_key("SCRATCH_DIRECTORY"):
            datadir = os.environ["SCRATCH_DIRECTORY"]
        elif os.environ.has_key("OSG_WN_TMP"):
            datadir = os.environ["OSG_WN_TMP"]
        else:
            print "!!WARNING!!2500!!Cannot locate scratch area"
    
    if datadir == "":
        print "datadir not defined"
    else:
        print "datadir:", datadir
    
    if dq2url == "":
        print "dq2url not defined"
    else:
        print "dq2url:", dq2url

    try:
        release = os.environ["swRelease"]
        release = release.replace('Atlas-','')
        print "Release:", release
    except:
        print "swRelease is not defined"
        release = ""

    runpars = [ '-s', site, 
                '-h', queue, 
                '-d', workdir, 
                '-q', dq2url, 
                '-f', 'true', 
                '-w', pandaURL, 
                '-p', pandaPort, 
                '-l', 'true'
              ]

    print "Using source: %s" % (libcode)
    if "pilotcode-dev.tar.gz" in libcode:
        runpars.append('-C')
        runpars.append('0')
        print "Will turn off multi-jobs for dev pilot"

    if appdir != "":
        # Set environment variables the pilot wants
        os.environ["SITEROOT"]="%s/%s" % ( appdir, release )
        runpars.append('-a')
        runpars.append(appdir)
        print "appdir:", appdir
    else:
        print "appdir not defined"

    if puser != "":
        runpars.append('-u')
        runpars.append(puser)

    if countrygroup != "":
        runpars.append('-o')
        runpars.append(countrygroup)

    if allowothercountry != "":
        runpars.append('-A')
        runpars.append(allowothercountry)

    return runpars, jobstat

def runMain(runpars):
        # If things still OK, run job
        print "--- Run job script", time.strftime("%a %b %d %H:%M:%S %Y", time.gmtime(time.time()))
        sys.path.append('.')
        import pilot

        jobstat = pilot.runMain(runpars)
        return jobstat

# --------------------------------------------------# 
#  S H U T D O W N   A N D   E N D   J O B          #
# --------------------------------------------------# 

def shutdown():
    """
    Report to dispatcher, clean up and shut down the pilot
    """
    print "==== Cleanup for pilot wrapper"
    if cleanup:
        cmd = 'rm -rf %s'%jobdir
        print cmd
        os.system(cmd)

def endJob(pid, state, jobstat):
    """this function is invoked when state is failed 
    """

    data = {}
    data['node'] = host
    data['siteName'] = site
    data['jobId'] = pid
    data['schedulerID'] = schedid
    data['pilotID'] = tpid
    data['state'] = state
    data['timestamp'] = utils.timeStamp()
    data['transExitCode'] = jobstat
    ### Prepare the xml catalog file
    data['xml'] = '' #xmltxt
    print "== Updating Panda with completion info"
    status, pars, response = utils.toServer(baseURLSSL,'updateJob',data,os.getcwd())
    if status != 0:
        print "Error contacting dispatcher to update job status: status=%s" % status
    else:
        if jobstat == 0:
            print "==== PandaID %s successful completion reported to dispatcher" % pid
            print "!!FINISHED!!0!!PandaID %s done" % pid
        else:
            print "==== PandaID %s failed completion reported to dispatcher" % pid
            print "!!FAILED!!2999!!PandaID %s done" % pid
    print "==== Directory at job completion:"
    print commands.getoutput('pwd; ls -al')

# --------------------------------------------------# 
#  C H E C K I N G   T H E   E N V I R O N M E N T  #
# --------------------------------------------------# 

def checkEnvironment():

    localenvs = checkGrid()
    batchenvs = checkBatch()

    createdirs()

    envs = getEnvs(localenvs, batchenvs)

    LibraryCheck()
    showEnvs(envs)
    checkDirs()   
    testBasicCommands()

def checkGrid():

    grid = ''
    localenvs = []
    info = {}


    # Report where we are
    if os.environ.has_key('OSG_GRID'):
        grid = 'OSG'
        localenvs = [
            'CERT_SCRIPTS_HOME', 
            'GRID3_APP_DIR',     
            'GRID3_DATA_DIR', 
            'GRID3_GRIDFTP_LOG', 
            'GRID3_SITE_INFO', 
            'GRID3_SITE_NAME', 
            'OSG_APP',         
            'OSG_DATA', 
            'OSG_DEFAULT_SE',  
            'OSG_GRID', 
            'OSG_GRIDFTP_LOG', 
            'OSG_JOB_CONTACT', 
            'OSG_LOCATION',    
            'OSG_SITE_NAME', 
            'OSG_SITE_READ',   
            'OSG_SITE_WRITE', 
            'OSG_USER_VO_MAP', 
            'OSG_UTIL_CONTACT', 
            'OSG_WN_TMP', 
        ]
        info['OSG_SITE_NAME']       = 'Site name'
        info['OSG_SITE_CITY']       = 'Facility location'
        info['OSG_SITE_INFO']       = 'Site information'
        info['OSG_CONTACT_NAME']    = 'Contact name'
        info['OSG_CONTACT_EMAIL']   = 'Contact email'
        info['OSG_SPONSOR']         = 'OSG sponsor'
        info['OSG_STORAGE_ELEMENT'] = 'Storage element available?'
        info['OSG_DEFAULT_SE']      = 'Default storage element'

    if os.environ.has_key('VO_ATLAS_SW_DIR'):
        grid = 'LCG'
        localenvs = [
            'APEL_HOME',
            'EDG_LOCATION',     
            'EDG_LOCATION', 
            'EDG_LOCATION_VAR', 
            'EDG_TMP', 
            'EDG_WL_LOCATION',  
            'EDG_WL_LOCATION_VAR', 
            'EDG_WL_SCRATCH',   
            'EDG_WL_TMP', 
            'EDG_WL_USER', 
            'GLITE_LOCATION',     
            'GLITE_LOCATION_LOG', 
            'GLITE_LOCATION_TMP', 
            'GLITE_LOCATION_VAR', 
            'GPT_LOCATION', 
            'INSTALL_ROOT', 
            'LCG_GFAL_INFOSYS', 
            'LCG_JAVA_HOME', 
            'LCG_LOCATION',     
            'LCG_LOCATION_VAR', 
            'LCG_TMP',
            'LFC_HOST', 
            'RGMA_HOME',
            'SASL_PATH',
            'SITE_NAME',
            'UI_LOC', 
            'VO_ATLAS_DEFAULT_SE', 
            'VO_ATLAS_SW_DIR', 
            'VO_ESR_DEFAULT_SE',   
            'VO_OPS_DEFAULT_SE', 
            'VO_SC3_ATLAS_DEFAULT_SE', 
        ]
        info['SITE_NAME']           = 'Site name'
        info['VO_ATLAS_DEFAULT_SE'] = 'ATLAS default SE'

    if grid != '':
        print '----- Grid: %s' % grid
        if len(info) > 0:
            for env, desc in info.iteritems():
                if os.environ.has_key(env): 
                    print "%s (%s): %s" % ( desc, env, os.environ[env] )
    else:
        print 'Grid environment not identified'

    return localenvs

def checkBatch():

    batch = ''
    batchenvs = []

    if os.environ.has_key('PBS_ENVIRONMENT'):
        batch = 'PBS'
        batchenvs = [
            'PBS_ENVIRONMENT', 
            'PBS_JOBCOOKIE', 
            'PBS_JOBID',
            'PBS_JOBNAME', 
            'PBS_MOMPORT',
            'PBS_NODEFILE',
            'PBS_NODENUM',
            'PBS_O_HOME',
            'PBS_O_HOST',
            'PBS_O_LANG',
            'PBS_O_LOGNAME', 
            'PBS_O_PATH'
            'PBS_O_QUEUE',
            'PBS_O_SHELL',
            'PBS_O_WORKDIR', 
            'PBS_QUEUE', 
            'PBS_TASKNUM', 
            'PBS_VNODENUM',
        ]

    if os.environ.has_key('LSF_VERSION'):
        batch = 'LSF'
        batchenvs = [
            'LSB_JOB_EXECUSER', 
            'LSB_JOBID', 
            'LSB_JOBRES_PID', 
            'LSF_INVOKE_CMD', 
            'LSF_LIBDIR', 
            'LSF_SCRATCH', 
            'LSF_SERVERDIR', 
            'LSF_VERSION', 
        ]

    if os.environ.has_key('_CONDOR_SCRATCH_DIR'):
        batch = 'Condor'
        batchenvs = [
            '_CONDOR_SCRATCH_DIR',
        ]

    if batch != '':
        print '----- Batch system: %s' % batch
    else:
        print 'Batch system not identified'

    return batchenvs

def createdirs():
    # Pee on a tree: we were here
    for d in 'OSG_APP OSG_DATA APP DATA'.split():
        if os.environ.has_key(d):
            commands.getoutput('if ! test -d $%s/panda; then mkdir $%s/panda; fi'%(d,d));

def getEnvs(localenvs, batchenvs):
    # env vars to look at:
    envs = [
        'APP', 
        'CLASSPATH',
        'DATA', 
        'ENVIRONMENT',
        'GRID', 
        'HOSTTYPE', 
        'INIT_VERSION', 
        'JAVA_HOME',
        'JAVA_INSTALL_PATH',
        'LD_LIBRARY_PATH', 
        'LIBPATH', 
        'LOGNAME'
        'LS_SUBCWD', 
        'MAIL',
        'PATH', 
        'PWD', 
        'PYTHONPATH',
        'REMOTE_HOST', 
        'SCRATCH_DIRECTORY',
        'SHLIB_PATH',
        'SITEROOT', 
        'T_DISTREL', 
        'TMP', 
        'USER', 
        'WN_TMP', 
    ]
    condorenvs = [
        '_CONDOR_SCRATCH_DIR',
        '_CONDOR_ANCESTOR_21130',
    ]
    gridenvs = [
        'CLASSADJ_INSTALL_PATH',
        'DPNS_HOST'
        'GATEKEEPER_JM_ID',
        'GATEKEEPER_PEER', 
        'GLOBUS_CONFIG',
        'GLOBUS_ERROR_VERBOSE', 
        'GLOBUS_GASS_CACHE_DEFAULT', 
        'GLOBUS_GATEKEEPER_CONTACT_STRING', 
        'GLOBUS_GRAM_JOB_CONTACT', 
        'GLOBUS_GRAM_MYJOB_CONTACT',
        'GLOBUS_ID', 
        'GLOBUS_LOCATION', 
        'GLOBUS_OPTIONS', 
        'GLOBUS_PATH', 
        'GLOBUS_REMOTE_IO_URL',
        'GLOBUS_SPOOL_DIR', 
        'GRID_ID',
        'GRID_SECURITY_DIR', 
        'MonaLisa_HOME',
        'MYPROXY_SERVER', 
        'MYPROXY_TCP_PORT_RANGE',
        'SITE_GIIS_URL',
        'SRM_CONFIG', 
        'SRM_PATH', 
        'VDS_HOME', 
        'VDT_LOCATION', 
        'VDT_POSTINSTALL_README',
        'VOMS_LOCATION',
        'X509_CERT_DIR', 
        'X509_USER_PROXY', 
        'X509_VOMS_DIR', 
    ]
    envs = envs + condorenvs + gridenvs + localenvs + batchenvs
    return envs

def LibraryCheck():
    print "==== Environment variable check:"
    if os.environ.has_key('LD_LIBRARY_PATH') and len(os.environ['LD_LIBRARY_PATH']) == 0 and os.environ.has_key('LIBPATH'):
        os.environ['LD_LIBRARY_PATH'] = os.environ['LIBPATH']

def showEnvs(envs):

    envs.sort()
    for e in envs:
        if os.environ.has_key(e):
            val = os.environ[e]
            print "%s = %s" % ( e, val )

def checkDirs():
    # Directories to look at, if existing:
    print "==== Directory check:"
    dirs = ( 
        '/scratch', 
        '$GLOBUS_LOCATION', 
        '$OSG_APP', 
        '$OSG_DATA', 
        '$OSG_GRID', 
        '$OSG_WN_TMP', 
        '$SITEROOT', 
        '$TMPBATCH', 
        '$VO_ATLAS_SW_DIR', 
        '$VO_ATLAS_SW_DIR/software', 
    )
    for d in dirs:
        out = commands.getoutput('echo \'%s\'="%s"' % (d, d) )
        pat = re.compile('.*=(.*)$')
        mat = pat.match(out)
        if mat:
            if len(mat.group(1)) > 0:
                print "---- %s=%s" % ( d, mat.group(1) )
                print commands.getoutput('ls -al %s | wc -l' % d)
            else:
                print "---- %s undefined" % d
        else:
            print "---- %s not known"

def testBasicCommands():
    # Commands to test:
    print "==== Command check:"
    cmds = (
        'curl -V',
        'pwd',
        'ls -al',
        )
    for c in cmds:
        print "---- %s" % c
        print commands.getoutput(c)

# --------------------------------------------------# 
#               M A I N                             #
# --------------------------------------------------# 

if __name__ == "__main__":
    checkEnvironment()
    status = runJob()
    shutdown()
