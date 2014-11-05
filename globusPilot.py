#!/direct/usatlas+u/mxp/python2.6.7/bin/python2.6
# /bin/env python

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

import subprocess

import tempfile
import stat
import shutil
import commands

# and now enter Globus

from globusonline.transfer import api_client
from globusonline.transfer.api_client import Transfer


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

################################################################################

def globus_opts(opts):
    list_opts = opts.split()

    g_opts = {}
    other_opts = ''

    recognized_globus_opts = ['globus-user',
                              'globus-endpoint-in',
                              'globus-endpoint-out',
                              'globus-endpoint-local',
                              'dir-in',
                              'dir-out',
                              'files-in',
                              'files-out',
                              'in-mode',
                              'out-mode'
                              ]

    # modes: local:gc:server.


    
    for opt in list_opts:
        # print 'opt', opt, ('=' in opt)
        if '=' in opt:
               opts = opt.split('=')
               o = opts[0]
               v = opts[1]
               if o in recognized_globus_opts:
                      g_opts[o] = v
               else:
                      other_opts+=opt+' '
        else:
               other_opts+=opt+' '

    return g_opts, other_opts


def getproxy(userID):
       # This is just for reference
       # userDN = param['prodUserID']  # for Maxim, this would be "/DC=org/DC=doegrids/OU=People/CN=Maxim Potekhin 597945/CN=506264002"
       
       filename = 'globus_client_'+str(os.getpid())+'.x509'
       st, out = commands.getstatusoutput('myproxy-logon -s myproxy.cern.ch -l "'+userID+'"  -n -o '+filename)
       print 'st', st
       print 'out', out

       return filename

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

print "globusPilot.py: Running python from",sys.prefix,", platform", sys.platform, ", version", sys.version
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

print 'OPTLIST:', optlist
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

print 'OPTS:', opts

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

print 'SITE:', site

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
    print "!!FAILED!!2999!!dataPilot: curl not found or not https-capable"
    sys.exit(1)

host = commands.getoutput('hostname -f')
user = commands.getoutput('whoami')
workdir = ''


def getJob():
    """
    Get a job from the Panda dispatcher
    """

    global param
    ## Request job from dispatcher
    print "==== Request Panda job from dispatcher"

    # --mxp-- deprecated
    #    glexec_flag = getqueueparameter('glexec').capitalize()

    data = {
        'siteName':site,
        'node':host,
        'prodSourceLabel':'user', #'test' 
        'computingElement':qname,
        'getProxyKey':0
        }
    
    print data
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
        #        while totaltime < 10*60:
        while totaltime < 30:
            print 'trial after %s seconds' %totaltime
            status, param, response = utils.toServer(baseURLSSL,'getJob',data,os.getcwd())
            if status != 20:
                 break
            time.sleep(5)
            totaltime += 5
#        print 'results from getJob: '
#        print '   status = %s' %status
#        print '   param = %s' %param
#        print '   response = %s' %response
        if status == 0:
            print "==== Job retrieved:"
            ## Add VO as a param. Needs to go into schema. $$$
            param['vo'] = 'OSG'
            PandaID = param['PandaID']
#            for p in param:
#                print "   %s=%s" % ( p, param[p] )
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


    cmd = 'cd %s; ./run.sh %s ' % ( jobdir, opts )
    # FIXME
    # in the future, instead of a monster cmd string, 
    # it should be in a wrapper script or any other solution
    
    #### END TEST ####
    # executing the job
    print 'command to be executed is = %s' %cmd

    out=''
    st, out = commands.getstatusoutput(cmd)


    # writing the output in job.out
    fout = '%s/job.out'%jobdir
    fh = open(fout,'w')
    fh.write(out+'\n')
    fh.close()

    print "\n==== Job script output written to", fout

    # If destinationURL was specified, 
    # stage-out the output file with gridFTP
    #gridFTP(fout, destinationURL)

    jobstat = analyzeoutput_default(st, jobdir)


    if jobstat == 0:
        print "Final job status: success"
    else:
        print "Final job status: error code", jobstat

    return jobstat

def analyzeoutput_default(st, jobdir):

    jobstat = 0
    if st != 0:
        print "ERROR: dataPilot: Job script failed. Status=%s" % st
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


def shutdown(jobstat):
    """
    Report to dispatcher, clean up and shut down the pilot
    """
    global param
    pid = param['PandaID']
    print "==== Cleanup for PandaID", pid
#    if cleanup:
#        cmd = 'rm -rf %s'%jobdir
#        print cmd
#        out = commands.getoutput(cmd)
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
#    print "==== Directory at job completion:"
#    print commands.getoutput('pwd; ls -al')

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
def GO_create_api(globus_user, proxy, host_cert):
    try:
           return api_client.TransferAPIClient(username=globus_user,
                                              server_ca_file=host_cert,
                                              cert_file=proxy, key_file=proxy)
    except:
           return None
# ====================================================================== 
def GO_create_gc_endpoint(ep, globus_user, proxy, host_cert):

    api = None
    try:
           api = api_client.TransferAPIClient(username=globus_user,
                                              server_ca_file=host_cert,
                                              cert_file=proxy, key_file=proxy)
    except:
           return 1, None
    
    data = {}
    try:
           status_code, status_message, data = api.endpoint_create(ep, is_globus_connect=True)
           print status_code, status_message, data
    except:
           return 2, None

    st, out = commands.getstatusoutput('sh globusconnect -stop')
    print 'Result of preemptive gc stop', out

    setup_key = data['globus_connect_setup_key']
    print 'Activating', setup_key
    st, out = commands.getstatusoutput('sh globusconnect -setup '+setup_key)
    print 'Activation result', out
    arg = ['sh', 'globusconnect', '-start']
    subprocess.Popen(arg)
    print 'Started'

    time.sleep(1)

    try:
           status_code, status_message, data = api.endpoint_autoactivate(ep)
           time.sleep(1)
           status_code, status_message, data = api.endpoint(ep) #  print data
    except:
           return 3, None

    return 0, api

def GO_transfer(ep1, dir1, ep2, dir2, files, api):

    print ep1, dir1, ep2, dir2, files

    status_code, status_message, data = api.submission_id()
    sid =  data['value']
    print 'SID', sid

    t = Transfer(sid, ep1, ep2)
    for f in files:
        path1 = dir1+f
        path2 = dir2+f
        print path1, path2
        t.add_item(path1, path2)

    status_code, status_message, data = api.transfer(t)

    tid = data['task_id']#    print 'task No.', tid
    task_status='ACTIVE'
    while task_status=='ACTIVE':
        status_code, status_message, data = api.task(tid)
        task_status=data['status']
        #        print 'data:', data
        print 'task:', task_status

        if task_status!='ACTIVE':break
        time.sleep(5)

#        if 'Error' in data['nice_status_details']:
#               print 'ERROR'
#               quitPilot(heartbeat)

def quitPilot(heartbeat):
       st, out = commands.getstatusoutput('sh globusconnect -stop')
       print 'Result of gc stop', out
       
       heartbeat.stop()
       shutdown(0)
       sys.exit(0)
       
# ====================================================================== 
def GO_delete_gc_endpoint(ep, api):
    arg = ['sh', 'globusconnect', '-stop']
    subprocess.Popen(arg)
    print 'Stopped'
    status_code, status_message, data = api.endpoint_delete(ep)
    print 'Deleted'
       
# ====================================================================== 
def GO_gc_endpoint_name(qname):
       st, hostname = commands.getstatusoutput("hostname -s | sed -e's/\./_/g' | tr a-z A-Z")
       
       return qname+'_'+hostname+'_'+str(os.getpid())
# ====================================================================== 

if __name__ == "__main__":

    print '******************* mxp globusPilot******************'

    status, PandaID = getJob()
    print 'Panda ID', PandaID
    
    if status == 0:
        opts = param['jobPars'] #  print 'OPTIONS:', opts
        g_opts, opts = globus_opts(opts) 
        param['jobPars'] = opts

        print 'globus-related options:'
        print '------------------------------------------------------------'
        for k in g_opts.keys():
               print k,':\t', g_opts[k]
        print '------------------------------------------------------------'
        print 'o:', opts

        # This is part is just parsing out individual globus-related parameters already obtained from the options
        
        try:
               globus_user = g_opts['globus-user']
               if globus_user == None or globus_user=='': shutdown(1000000)
        except:
               shutdown(1000000)

        globusAPI = None
        serverAPI = None

        ep = GO_gc_endpoint_name(qname)
        print 'ep', ep
        # by default, if not specified, assume local node as the endpoint

        ep_in    = ep
        ep_out   = ep
        ep_local = None

        # and here override with parameters if any
        try:
               ep_in = g_opts['globus-endpoint-in']
        except:
               pass
        try:
               ep_out = g_opts['globus-endpoint-out']
        except:
               pass
        try:
               ep_local = g_opts['globus-endpoint-local']
        except:
               pass

        fli = []
        try:
               fli = g_opts['files-in'].split(',')
        except:
               pass

        flo = []
        try:
               flo = g_opts['files-out'].split(',')
        except:
               pass


        di = '/'
        do = '/'

        try:
               di = g_opts['dir-in']
        except:
               pass
        try:
               do = g_opts['dir-out']
        except:
               pass


        im = g_opts['in-mode']
        om = g_opts['out-mode']
        

        # ...and so it begins:
        status = updateJob(PandaID)
        heartbeat = ThreadHandler(30*60, updateJob, PandaID) 
        heartbeat.start()

        globusJobDir = os.getcwd()+'/'
        print 'CWD:',globusJobDir
        os.chmod(globusJobDir,  0777) #  st, out = commands.getstatusoutput('ls -ld '+globusJobDir)  print st, out

        proxy = '' # may or may not fill in the actual file name

        if(im == 'local'):  # local stage-in
               for f in fli:
                      cmd = 'cp '+di+'/'+f+' .'
                      print '*** LOCAL IN-MODE cmd', cmd
                      st, out = commands.getstatusoutput(cmd)
                      print st, out

        if(im == 'gc'): # globus-connect stage-in
               proxy = getproxy(globus_user) # print 'proxy is', proxy
               host_cert = "gd-bundle_ca.cert"

               st, globusAPI = GO_create_gc_endpoint(ep, globus_user, proxy, host_cert)
               if st!=0:
                      print 'problem creating a Globus Connect endpoint'
                      status = updateJob(PandaID)
                      shutdown(status)

               time.sleep(2)
               # stage-in
               if len(fli)>0:
                      if not ep_in:
                             print 'endpoint-in undefined, exiting'
                             quitPilot(heartbeat)
                      print '*** GC IN-MODE parameters:', ep_in, di, ep, globusJobDir, fli
                      GO_transfer(ep_in, di, ep, globusJobDir, fli, globusAPI)

        if(im == 'server'): # server to server stage-in
               print 'server mode'
               proxy = getproxy(globus_user) #
#               proxy = "/direct/usatlas+u/sm/ap/froo"
               print 'proxy is', proxy
               host_cert = "gd-bundle_ca.cert"
               
               serverAPI = GO_create_api(globus_user, proxy, host_cert)
               if len(fli)>0:
                      if not ep_in or not ep_local:
                             print 'endpoint-in or endpoint-local are undefined, exiting'
                             quitPilot(heartbeat)

                      print ep_in
                      status_code, status_message, data = serverAPI.endpoint(ep_in)
                      #                      print 'ep_in status', data
                      status_code, status_message, data = serverAPI.endpoint(ep_local)
                      #                      print 'ep_local status', data
                      print '*** SERVER IN-MODE parameters:', ep_in, di, ep_local, globusJobDir, fli
                      GO_transfer(ep_in, di, ep_local, globusJobDir, fli, serverAPI)


        status = runJob()

        if(om == 'local'): # local stage-out
               for f in flo:
                      cmd = 'cp '+f+' '+do
                      print 'cmd', cmd
                      st, out = commands.getstatusoutput(cmd)
                      print st, out

        if(om == 'gc'): # Globus stage-out
               if not globusAPI:
                      proxy = getproxy(globus_user) # print 'proxy is', proxy
                      host_cert = "gd-bundle_ca.cert"

                      try:
                             st, globusAPI = GO_create_gc_endpoint(ep, globus_user, proxy, host_cert)
                             time.sleep(2)
                             # stage-out
                             if len(flo)>0: GO_transfer(ge, globusJobDir, ep_out, do, flo, globusAPI)
                      except:
                             pass
               else:
                      print 'have globusAPI',globusJobDir



               if(do[len(do)-1]!='/'): do+='/'
               if len(flo)>0:
                      if not ep_out:
                             print 'endpoint-out undefined, exiting'
                             quitPilot(heartbeat)

                      status_code, status_message, data = globusAPI.endpoint(ep_out)
                      print 'ep_out status', data
                      print '*** GC OUT-MODE parameters:', ep, globusJobDir, ep_out, do, flo
                      GO_transfer(ep, globusJobDir, ep_out, do, flo, globusAPI)
               GO_delete_gc_endpoint(ep, globusAPI)


        if(om == 'server'): # server to server stage-out
               print 'server mode'
               proxy = getproxy(globus_user)
               print 'proxy is', proxy
               host_cert = "gd-bundle_ca.cert"

               if(do[len(do)-1]!='/'): do+='/'
               if not serverAPI: serverAPI = GO_create_api(globus_user, proxy, host_cert)
               if len(fli)>0:
                      if not ep_out or not ep_local:
                             print 'endpoint-out or endpoint-local are undefined, exiting'
                             quitPilot(heartbeat)

                      status_code, status_message, data = serverAPI.endpoint(ep_out)
                      #                      print 'ep_out status', data
                      status_code, status_message, data = serverAPI.endpoint(ep_local)
                      #                      print 'ep_local status', data
                      print '*** SERVER OUT-MODE parameters:', ep_local, globusJobDir, ep_out, do, flo
                      GO_transfer(ep_local, globusJobDir, ep_out, do, flo, serverAPI)


        # cleanup:
        if proxy!='': os.unlink(proxy)
        for f2remove in fli:
               os.unlink(globusJobDir+f2remove)
        
        quitPilot(heartbeat)

#                             ge = g_opts['globus-endpoint']

#                      if st!=0:
#                             print 'problem creating a Globus Connect endpoint'
#                             status = updateJob(PandaID)
#                             shutdown(status)


