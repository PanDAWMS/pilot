import os, re, sys, urllib, commands, time, cgi, string
try:
    import datetime
except:
    print "Cannot import datetime. Old python. I'll live with it."
import cPickle as pickle
try:
    from xml.dom import minidom
    from xml.dom.minidom import Document
    from xml.dom.minidom import parse, parseString
except:
    print "!!WARNING!!2999!!PilotUtils: Cannot import xml.dom"

########### dispatcher status codes
SC_Success   =  0
SC_TimeOut   = 10
SC_NoJobs    = 20
SC_Failed    = 30
SC_NonSecure = 40

try:
    baseURL = os.environ['PANDA_URL']
except:
    #baseURL = 'http://voatlas19.cern.ch:25080/server/panda'
    baseURL = 'http://voatlas57.cern.ch:25080/server/panda'
    #baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except:
    #baseURLSSL = 'https://voatlas19.cern.ch:25443/server/panda'
    baseURLSSL = 'https://voatlas57.cern.ch:25443/server/panda'
    #baseURLSSL = 'https://pandaserver.cern.ch:25443/server/panda'
try:
    baseURLDQ2 = os.environ['PANDA_URL_DQ2']
except:
    baseURLDQ2 = 'http://dms02.usatlas.bnl.gov:80/dq2'
try:
    baseURLDQ2LRC = os.environ['PANDA_URL_DQ2LRC']
except:
    baseURLDQ2LRC = 'http://dms02.usatlas.bnl.gov:8000/dq2/lrc'
try:
    baseURLSUB = os.environ['PANDA_URL_SUB']
except:
    baseURLSUB = 'https://gridui01.usatlas.bnl.gov:24443/dav/test'

# exit code
EC_Failed = 255


# look for a grid proxy certificate
def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ['X509_USER_PROXY']
    except:
        pass
    # see the default place
    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    # FIXME
    print "SimplePilot: No valid grid proxy certificate found"
    return ''


# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl'
        # verification of the host certificate
        self.verifyHost = False
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = os.environ['X509_USER_PROXY']
        self.sslKey  = os.environ['X509_USER_PROXY']


    # GET method
    def get(self,url,data):
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        #com += ' --verbose'
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        # tmpName = commands.getoutput('uuidgen')
        tmpName = 'curl.config'
        tmpFile = open(tmpName,'w')
        tmpFile.write(strData)
        tmpFile.close()
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        ret = commands.getstatusoutput(com)
        # remove temporary file
        #os.remove(tmpName)
        return ret


    # POST method
    def post(self,url,data):
        # make command
        com = '%s --silent' % self.path
        #if not self.verifyHost:
        #    com += ' --insecure'

        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        #com += ' --verbose'
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        #tmpName = commands.getoutput('uuidgen')
        tmpName = 'curl.config'
        tmpFile = open(tmpName,'w')
        tmpFile.write(strData)
        tmpFile.close()

        com += " --show-error "
        com += " --capath %s" %os.environ['X509_CADIR']
        com += ' --config %s' % tmpName
        com += ' %s' % url

        # execute
        #print com
        ret = commands.getstatusoutput(com)
        # remove temporary file
        #os.remove(tmpName)        
        return ret


    # PUT method
    def put(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        #com += ' --verbose'
        # emulate PUT 
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        # execute
        return commands.getstatusoutput(com)


# send message to dispatcher
def toDispatcher(cmd, data):
    try:
        tpre = datetime.datetime.utcnow()
    except:
        pass
    print "toDispatcher:", cmd, " Data:"
    print data
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURLSSL + '/' + cmd
    #print "Posting %s" % url
    curlstat,output = curl.post(url,data)
    #print "Posted"
    try:
        tpost = datetime.datetime.utcnow()
        print "Elapsed seconds:" (tpost-tpre).seconds
    except:
        pass
    print "toDispatcher:", cmd, " Data:"
    try:
        if curlstat == 0:
            # parse response message
            outtxt = output.lower()
            if outtxt.find('<html>') > 0:
                if outtxt.find('read timeout') > 0:                   
                    print "!!FAILED!!2999!!Timeout on dispatcher exchange"
                else:
                    print "!!FAILED!!2999!!HTTP error on dispatcher exchange"
                print "HTTP output: ",output
                return EC_Failed,None,None
            parlist = cgi.parse_qsl(output,keep_blank_values=True)
            print "Panda response:"
            print parlist
            param = {}
            for p in parlist:
                param[p[0]] = p[1]
            status = int(param['StatusCode'])
            if status == SC_Success:
                print "Successful dispatcher exchange"
            elif status == SC_NoJobs:
                print "!!FINISHED!!0!!Dispatcher has no jobs"
            elif status == SC_TimeOut:
                print "!!FAILED!!2999!!Dispatcher reports timeout"
            elif status == SC_Failed:
                print "!!FAILED!!2999!!Dispatcher reports failure processing message"
                print parlist
            elif status == SC_NonSecure:
                print "!!FAILED!!2999!!Attempt to retrieve job with non-secure connection disallowed"
            else:
                print "!!FAILED!!2999!!Unknown dispatcher status code:", status
                print "curl.config file:"
                print commands.getoutput('cat curl.config')
        else:
            print "!!FAILED!!2999!!Dispatcher message curl error ", curlstat
            print "Output = ", output
            print "curl.config file:"
            print commands.getoutput('cat curl.config')
            return curlstat, None,None
        if status == SC_Success:
            return status, param, output
        else:
            return status, None, None
    except:
        _type, value, traceBack = sys.exc_info()
        print "ERROR %s : %s %s" % (cmd, _type,value)
        return EC_Failed,None, None

# get job
def getJob(data):
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURLSSL + '/getJob'
    status,output = curl.post(url,data)
    try:
        #return status,pickle.loads(output)
        return status,output
    except:
        _type, value, traceBack = sys.exc_info()
        print "ERROR getJobStatus : %s %s" % (_type,value)
        return EC_Failed,None

# update job
def updateJob(data):
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURLSSL + '/updateJob'
    status,output = curl.post(url,data)
    try:
        #return status,pickle.loads(output)
        return status,output
    except:
        _type, value, traceBack = sys.exc_info()
        print "ERROR getJobStatus : %s %s" % (_type,value)
        return EC_Failed,None

def timestamp(): 
    ''' return ISO-8601 compliant date/time format '''
    tmptz = time.timezone
    if tmptz>0:
        signstr='-'
    else:
        signstr='+'
    tmptz_hours = int(tmptz/3600)
    return str("%s%s%02d%02d"%(time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()), signstr, tmptz_hours, int(tmptz/60-tmptz_hours*60)))

def PFCxml(fname,lfns=[],fguids=[],pfns=[],fntag='lfn',alog=None,alogguid=None):
    """ Adapted from panda/pilot2/pUtil.py
    fguids list will be changed in the caller as well, since list is mutable object
    fguids and lfns are for output files, alog and alogguid are for workdir tarball log
    files, which are not mutable so don't expect the value changed inside this function
    will change in the caller as well !!! """

    # firstly make sure every file has a guid

    flist=[]
    plist=[]
    glist=[]
    
    if alog:
        flist.append(alog)
        plist.append('')
        if not alogguid:
            alogguid=commands.getoutput('uuidgen')
        glist.append(alogguid)

    if lfns:
        flist=flist+lfns
        plist=plist+pfns
        for i in range(0,len(lfns)):
            try:
                fguids[i]
            except IndexError, e:
                #print "This item doesn't exist"
                fguids.insert(i,commands.getoutput('uuidgen'))
            else:
                if not fguids[i]: # this guid doesn't exist
                    fguids[i]=commands.getoutput('uuidgen')
        glist=glist+fguids

    if fntag == "pfn":
        #create the PoolFileCatalog.xml-like file in the workdir
        fd=open(fname,"w")
        fd.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        fd.write("<!-- Edited By POOL -->\n")
        fd.write('<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n')
        fd.write("<POOLFILECATALOG>\n")
        for i in range(0,len(flist)):
            fd.write('  <File ID="%s">\n'%(glist[i]))
            fd.write("    <physical>\n")
            fd.write('      <pfn filetype="" name="%s"/>\n'%(flist[i]))
            fd.write("    </physical>\n")
            fd.write("  </File>\n")
        fd.write("</POOLFILECATALOG>\n")
        fd.close()
    elif fntag=="lfn":
        # create the metadata.xml-like file that's needed by dispatcher jobs
        fd=open(fname,"w")
        fd.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        fd.write("<!-- ATLAS file meta-data catalog -->\n")
        fd.write('<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n')
        fd.write("<POOLFILECATALOG>\n")
        for i in range(0,len(flist)):
            fd.write('  <File ID="%s">\n'%(glist[i]))
            fd.write("    <logical>\n")
            fd.write('      <lfn name="%s"/>\n'%(flist[i]))
            fd.write("    </logical>\n")
            if plist[i] != '':
                fd.write("    <physical>\n")
                fd.write('      <pfn filetype="" name="%s"/>\n'%(plist[i]))
                fd.write("    </physical>\n")
            fd.write("  </File>\n")
        fd.write("</POOLFILECATALOG>\n")
        fd.close()
    else:
        print "fntag is neither lfn nor pfn, didn't create the XML file for output files"

def getCatalogFiles(fname):
    """Return the files in an XML file catalog"""
    print "Catalog content:"
    print commands.getoutput("cat %s"%fname)
    flist = []
    xmldoc = minidom.parse(fname)
    fileList = xmldoc.getElementsByTagName("File")
    for thisfile in fileList:
        fdict = {}
        fdict['lfn'] = str(thisfile.getElementsByTagName("lfn")[0].getAttribute("name"))
        fdict['guid'] = str(thisfile.getAttribute("ID"))
        flist.append(fdict)
    return flist

def tolog(str):
    commands.getoutput('echo "`date`: %s" >> jobstatus.dat'%str)

def writelog(str):
    commands.getoutput('echo "`date`: ======== Bulk status write" >> jobstatus.dat')
    fh = open("jobstatus.dat","a")
    fh.write("===== Bulk status write\n")
    fh.write(str)
    fh.write("================================= End bulk status write\n")
    fh.close()

def getpar(par,str):
    pars = str.split('|')
    for p in pars:
        pat = re.compile(".*%s=(.*)"%par)
        mat = pat.match(p)
        if mat:
            return mat.group(1)
    return ''

def readpar(par):
    fh = open('%s/queuedata.dat'%os.environ['PilotHomeDir'])
    queuedata = fh.read()
    fh.close()
    return getpar(par,queuedata)

def envSetup():
    envsetup = readpar('envsetup')
    if envsetup == '':
        if os.environ.has_key('UI_LOC'):
            setupfile = '%s/etc/profile.d/grid_env.sh' % os.environ['UI_LOC']
            if os.path.exists():
                envsetup = "source %s;" % setupfile
    return envsetup

def collectWNInfo():
    '''collect node information, like cpu, memory and disk space'''
    fd = open("/proc/meminfo", "r")
    mems=fd.readline()
    while mems:
        if mems.upper().find("MEMTOTAL") != -1:
            mem=float(mems.split()[1])/1024
            break
        mems=fd.readline()
    fd.close()

    fd=open("/proc/cpuinfo","r")
    lines=fd.readlines()
    fd.close()
    for line in lines:
        if not string.find(line,"cpu MHz"):
            cpu=float(line.split(":")[1])
            break
    return mem,cpu
