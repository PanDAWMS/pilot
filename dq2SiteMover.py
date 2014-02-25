import os, re, sys
import commands

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar

class dq2SiteMover(SiteMover.SiteMover):
    """ SiteMover for dq2 """
    
    copyCommand = "dq2"
    checksum_command = "adler32"
    __errorStr = '!!WARNING!!2995!! %s'
    __dq2get = '%s dq2-get --client-id=pilot -d -a -f %s -H %s -t %s %s' # envsetup, lfn, target directory, timeout, dsn
#    __dq2put = '%s dq2-get --client-id=pilot -d -a -f %s -H %s -t %s %s' # envsetup, filename, destination, timeout, dsn
    __dq2put = '%s which dq2-put; dq2-put --client-id=pilot -d -a -C -s %s -f %s %s' # envsetup, source directory, filename, dsn
    __timeout = 1800 # seconds
    __error = PilotErrors()
    __pilotErrorDiag = ''

    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path

    def get_timeout(self):
        return self.__timeout

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        if not os.environ.has_key('DQ2_LOCAL_SITE_ID'):
#            os.environ['DQ2_LOCAL_SITE_ID'] = "HEPHY-UIBK_SCRATCHDISK" #"CERN-PROD_MCDISK" #"BNL-OSG2_USERDISK" #"ROAMING"
            os.environ['DQ2_LOCAL_SITE_ID'] = "BNL-OSG2_USERDISK" #"ROAMING"
            tolog("!!!WARNING!!2999!! DQ2_LOCAL_SITE_ID was not set. Now set to %s" % (os.environ['DQ2_LOCAL_SITE_ID']))

        # get the special command setup
        #try:
        #    spsetup = str(pdict['spsetup'])
        #except:
        #    self.__pilotErrorDiag = 'No spsetup: %s' % str(sys.exc_info())
        #    tolog(self.__errorStr % self.__pilotErrorDiag)            
        #    return self.__error.ERR_STAGEINFAILED, self.__pilotErrorDiag

        # get a proper envsetup
        envsetup = self.getEnvsetup(get=True)

        # prepend the special command setup to the envsetup
        envsetup += 'export PYTHONPATH=`echo $PYTHONPATH | sed -e "s%$SITEROOT/sw/lcg/external/Python[^:]*:%%g"`;export PATH=/usr/bin:$PATH;'
        tolog("get_data() will use envsetup: %s" % (envsetup))

        # is there a DSN in the pdict?
        try:
            # test jobs: dsn = "ddo.000001.Atlas.Ideal.DBRelease.v050101"
            dsn = str(pdict['dsname'])
        except:
            self.__pilotErrorDiag = 'DSN parsing error: %s' % str(sys.exc_info())
            tolog(self.__errorStr % self.__pilotErrorDiag)            
            return self.__error.ERR_STAGEINFAILED, self.__pilotErrorDiag

        # everything here, check for validaty
        if not dsn:
            self.__pilotErrorDiag = 'empty DSN!'
            tolog(self.__errorStr % self.__pilotErrorDiag)            
            return self.__error.ERR_STAGEINFAILED, self.__pilotErrorDiag
            
        if not lfn:
            self.__pilotErrorDiag = 'empty LFN!'
            tolog(self.__errorStr % self.__pilotErrorDiag)            
            return self.__error.ERR_STAGEINFAILED, self.__pilotErrorDiag

        if not path:
            tolog('path is empty, using current directory')            
            path = os.getcwd()

        # everything there, build the command
        execStr = self.__dq2get % (envsetup, lfn, path, self.__timeout, dsn)

        tolog('pdict: %s' % str(pdict))
        try:
            tolog("DQ2_LOCAL_SITE_ID: %s" % (os.environ['DQ2_LOCAL_SITE_ID']))
        except Exception, e:
            tolog("!!WARNING!!2999!! %s" % str(e))
        tolog('executing command: %s' % (execStr))

        # execute
        status, output = commands.getstatusoutput(execStr)

        # validate
        if status:
            self.__pilotErrorDiag = 'dq2-get failed (%s): %s' % (status, output)
            tolog(self.__errorStr % self.__pilotErrorDiag)            
            return self.__error.ERR_STAGEINFAILED, self.__pilotErrorDiag

        # a successful transfer must end with "Finished"
        if output[-len('Finished'):] == 'Finished':
            tolog('Command output: %s' % (output))
            tolog("Transfer finished")
        else:
            self.__pilotErrorDiag = 'dq2-get failed (%s): %s' % (status, output)
            tolog(self.__errorStr % self.__pilotErrorDiag)            
            return self.__error.ERR_STAGEINFAILED, self.__pilotErrorDiag

        return 0, self.__pilotErrorDiag

    def put_data(self, source, ddm_storage, fsize=0, fchecksum=0, dsname='', **pdict):
        """ copy output file from local dir to SE and register into dataset and catalogues """

        if not os.environ.has_key('DQ2_LOCAL_SITE_ID'):
#            os.environ['DQ2_LOCAL_SITE_ID'] = "HEPHY-UIBK_SCRATCHDISK" #"OU" #"CERN-PROD_MCDISK" #"BNL-OSG2_USERDISK"
            os.environ['DQ2_LOCAL_SITE_ID'] = "BNL-OSG2_USERDISK"
            tolog("!!!WARNING!!2999!! DQ2_LOCAL_SITE_ID was not set. Now set to %s" % (os.environ['DQ2_LOCAL_SITE_ID']))

        # get the special command setup
        token = pdict.get('token', '')
        spsetup = pdict.get('spsetup', '')
        analJob = pdict.get('analJob', False)
        experiment = pdict.get('experiment', '')
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        # get the site information object
        si = getSiteInformation(experiment)

        # preparing variables
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype="adler32")
            if ec != 0:
                return self.put_data_retfail(ec, pilotErrorDiag)

        # get a proper envsetup
        envsetup = self.getEnvsetup()

        tolog("envsetup=%s" % str(envsetup))
        tolog("spsetup=%s" % str(spsetup))
        if spsetup == "None" or spsetup == None:
            spsetup = ""
        # prepend the special command setup to the envsetup
        envsetup = spsetup + envsetup + 'export PYTHONPATH=`echo $PYTHONPATH | sed -e "s%$SITEROOT/sw/lcg/external/Python[^:]*:%%g"`;export PATH=/usr/bin:$PATH;'
        tolog("put_data() will use envsetup: %s" % (envsetup))

        # check for validity
        if not source:
            self.__pilotErrorDiag = 'empty source!'
            tolog(self.__errorStr % self.__pilotErrorDiag)            
            return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)

        if not dsname:
            self.__pilotErrorDiag = 'empty DSN!'
            tolog(self.__errorStr % self.__pilotErrorDiag)            
            return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)

        # for production jobs, the SE path is stored in seprodpath
        # for analysis jobs, the SE path is stored in sepath
        destination = si.getPreDestination(analJob, token, prodSourceLabel)
        if destination == '':
            if ddm_storage != "":
                destination = ddm_storage
            if destination == '':
                pilotErrorDiag = "put_data destination path in SE not defined"
                tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
                return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        tolog("Going to store job output at: %s" % (destination))

        # get the LFC path
        lfcpath, pilotErrorDiag = self.getLFCPath(analJob)
        if lfcpath == "":
            return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, pilotErrorDiag)
        tolog("LFC path: %s" % (lfcpath))

        # extract the filename from the source
        filename = os.path.basename(source)
        ec, pilotErrorDiag, dst_gpfn, lfcdir = self.getFinalLCGPaths(analJob, destination, dsname, filename, lfcpath)
        if ec != 0:
            return self.put_data_retfail(ec, pilotErrorDiag)

        # get the proper endpoint
        se = self.getProperSE(token)
        putfile = dst_gpfn #"%s%s" % (se, dst_gpfn)

        # everything there, build the command
#        execStr = self.__dq2put % (envsetup, filename, putfile, self.__timeout, dsname)
#        execStr = self.__dq2put % (envsetup, sourcedir, putfile, dsname)
        envsetup += "voms-proxy-init -noregen -voms atlas;"
        execStr = self.__dq2put % (envsetup, os.path.dirname(source), filename, dsname)
        tolog('executing command: %s' % (execStr))

        # execute
        status, output = commands.getstatusoutput(execStr)
        
        # validate
        if status:
            self.__pilotErrorDiag = 'dq2-put failed (%s): %s' % (status, output)
            tolog(self.__errorStr % self.__pilotErrorDiag)            
            return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)

        # a successful transfer must end with "Finished"
        if output[-len('Finished'):] == 'Finished':
            tolog('Command output: %s' % (output))
            tolog("Transfer finished")
        else:
            self.__pilotErrorDiag = 'dq2-put failed (%s): %s' % (status, output)
            tolog(self.__errorStr % self.__pilotErrorDiag)            
            return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)

        return 0, self.__pilotErrorDiag, filename, fsize, fchecksum, self.arch_type
