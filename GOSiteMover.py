import os, re, sys
import commands

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar
from timed_command import timed_command
from time import time
from FileStateClient import updateFileState

try:
    import subprocess
except Exception, e:
    print e
    
class GOSiteMover(SiteMover.SiteMover):
    """ SiteMover for local copy commands etc """

    copyCommand = "globusonline"
    checksum_command = "adler32"
    __warningStr = '!!WARNING!!2995!! %s'
    __spacetoken = '-t %s' # space token descriptor
    __localget = '%s lsm-get %s %s %s' # environment, options, lfn, target directory
    __localput = '%s lsm-put %s %s %s' # environment, space token (optional), source directory, destination
    __localspace = '%s lsm-df %s %s' # environment, space token (optional), storage end-point
    __localerror = 'lsm-error %d' # error code
    __par_filesize = ' --size %s' # filesize in bytes
    __par_checksum = ' --checksum %s' # checksum string: "adler32:NNN", "md5:NNN", default is assumed MD5
    __timeout = 5400 # seconds
    __error = PilotErrors()
    __pilotErrorDiag = ''

    def get_timeout(self):
        return self.__timeout

    def check_space(self, storage_endpoint):
        """ available spave in the SE """

        # build setup string
        envsetup = self.getEnvsetup()

        # build the df command
        execStr = self.__localspace % (envsetup, '', storage_endpoint)
        tolog("Executing command: %s" % (execStr))

        # execute
        try:
            status, telapsed, cout, cerr = timed_command(execStr, self.__timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog(self.__warningStr % self.__pilotErrorDiag)            
            status = 1
            output = str(e)
            telapsed = self.__timeout
        else:
            # improve output parsing, keep stderr and stdout separate
            output = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))

        # validate
        if status:
            status = os.WEXITSTATUS(status)
            self.__pilotErrorDiag = 'lsm-df failed (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)            
            return 999999
        try:
            return int(output.strip())
        except:
            self.__pilotErrorDiag = 'lsm-df wrong format (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)            

        return 999999

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        # Get input parameters from pdict
        useCT = pdict.get('usect', True)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'local', lfn, guid)

        if not path:
            tolog('path is empty, using current directory')            
            path = os.getcwd()

        # build setup string
        envsetup = self.getEnvsetup(get=True)

        # should the root file be copied or read directly by athena?
        directIn, useFileStager = self.getTransferModes()
        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
                updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
            else:
                # determine if the file is a root file according to its name
                rootFile = self.isRootFileName(lfn)

                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                    updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
                elif rootFile:
                    tolog("Found root file according to file name: %s (will not be transferred in direct reading mode)" % (lfn))
                    report['relativeStart'] = None
                    report['transferStart'] = None
                    self.__sendReport('FOUND_ROOT', report) 
                    if useFileStager:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="file_stager", type="input")
                    else:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="remote_io", type="input")
                    return 0, self.__pilotErrorDiag
                else:
                    tolog("Normal file transfer")
        else:
            tolog("not directIn")



        # THE FOLLOWING CODE UNTIL THE END OF THIS FUNCTION SHOULD BE REPLACED WITH NEW CODE FOR GLOBUS-ONLINE


	def downloadGC():
            """Download Globus Connect file from Globus Server
            """
            arg = ['curl','--connect-timeout','20','--max-time','120','-s','-S','http://confluence.globus.org/download/attachments/14516429/globusconnect','-o','globusconnect']
            tolog("Download Arguments: %s" % (arg))
            subprocess.call(arg)
        
        #Function to retrieve users proxy from Cern myproxy server.
        #The credential should be deposite to the server by the user under the Pilots DN username
        #and authoried to retrieve it by the Pilot without passphrase,
        #to run Globus Online.
        #Code partially extracted from MyproxyUtils.py
        #Uses as parameter userID, which is obtained from job info from Panda.
        
	def getproxy(userID):
            """Gets users myproxy from cern server
            """
            dn = userID
            if dn.count('/CN=') > 1:
               first_index = dn.find('/CN=')
               second_index = dn.find('/CN=', first_index+1)
               dn = dn[0:second_index]
            
            arg = ['myproxy-logon','-s','myproxy.cern.ch','--no_passphrase','-l',dn]
            subprocess.call(arg)
        
        #Fuction uses endpoint as parameter, that the user specifies at job submission.
        #Corresponds to the name of the endpoint for the working node using Globus-
        #Connect. Returns the code for the setup
        
        def createEndpoint(endpoint):
            """Create the endpoint the user specifies and returns the code for setup
            """
            arg = ['gsissh', 'cli.globusonline.org', 'endpoint-add','--gc', endpoint]
            proc = subprocess.Popen(arg, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            return_code = proc.wait()
            i = 0
            for line in proc.stdout:
                tolog(" %s " % (line.rstrip()))
                i += 1
                if i == 3:
                   code = line
        
            return code

        #Function uses endpoint as parameter to get the code and run the
        #setup in Globus Connect. This connects the endpoint on the working node with
        #Globus Online
        
        def setupGC(endpoint):
            """Installing Globus Connect on working node. Creates endpoint, get the setup code and uses it on setup mode
            """
            code = createEndpoint(endpoint)
            code = code.strip()
            arg = ['sh', 'globusconnect', '-setup', code]
            tolog("Arguments:  %s:" % (arg))
            subprocess.call(arg)

        #Function that runns Globus Connect on the background of the working node
        #to make it a Globus Online Endpoint
        
	def startGC():
            """Start Globus Connect process on working node, on the background
            """
            tolog("-----Running Globus Connect------")
            arg = ['sh', 'globusconnect', '-start']
            subprocess.Popen(arg)

        
        #Function uses source and destinations as parameters.Source is a
        #Globus Online Endpoint and path to file, like this:
        #SITE1:/home/osg_data/panda/0064.file
        #Destination is the Endpoint on the working node ,using Globus Connect,
        #like this:
        #OSGDEV:/direct/usatlas+u/ccontrer/pilot/autopilot/trunk/
        #User defines source and destination at job submission.
        #This function activates the source endpoint, under the consideration
        #it is a gridFTP server. Destination is already activated since it is a
        #Globus Connect endpoint and need no manual activation
        #This function creates one transfer task, so it should be executed
        #for as many source files as the user specifies
        
        
        #For a better dinamic, destination path can/should be obtained from job information (?)
        #and the endpoint is specified by user, as above.

	dest = endpoint + ':' + jobdir

	def stageincmdGO(src, dest):
            """create command for stage-in using Globus Online
            """
            #Activate the source endpoint
            source = src.split(':')[0]
            arg = ['gsissh', 'cli.globusonline.org', 'endpoint-activate','-g', source]
            subprocess.call(arg)
            
            #Create transfer task
            cmd = 'gsissh cli.globusonline.org scp -v %s %s'%(src, dest)
            tolog("Transfer task created: %s" % (cmd))
            return cmd

        #END OF GLOBUS ONLINE FUNCTIONS

    
        # build the get command
        _params = ""
        if fsize != 0 and fsize != "0":
            _params += self.__par_filesize % (fsize,)
        if fchecksum and fchecksum != 'None' and fchecksum != 0 and fchecksum != "0" and not self.isDummyChecksum(fchecksum):
            csumtype = self.getChecksumType(fchecksum)
            # special case for md5sum (command only understands 'md5' and 'adler32', and not 'ad' and 'md5sum')
            if csumtype == 'md5sum':
                csumtype = 'md5'
            _params += self.__par_checksum % ("%s:%s" % (csumtype, fchecksum),)

        # add the guid option
        _params += " --guid %s" % (guid)

        execStr = self.__localget % (envsetup, _params, gpfn, os.path.join(path, lfn))
        tolog("Executing command: %s" % (execStr))
        
        report['transferStart'] = time()
        try:
            status, telapsed, cout, cerr = timed_command(execStr, self.__timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog(self.__warningStr % self.__pilotErrorDiag)
            status = 1
            output = str(e)
            telapsed = self.__timeout
        else:
            # improve output parsing, keep stderr and stdout separate
            output = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))
        tolog("Command output:\n%s" % (output))
        report['validateStart'] = time()

        if status:
            # did the copy command time out?
            if is_timeout(status):
                self.__pilotErrorDiag = "lsm-get failed: time out after %d seconds" % (telapsed)
                tolog(self.__warningStr % self.__pilotErrorDiag)            
                self.__sendReport('GET_TIMEOUT', report)
                return self.__error.ERR_GETTIMEOUT, self.__pilotErrorDiag

            status = os.WEXITSTATUS(status)
            self.__pilotErrorDiag = 'lsm-get failed (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)            
            self.__sendReport('COPY_FAIL', report)
            return self.__error.ERR_STAGEINFAILED, self.__pilotErrorDiag

        # the lsm-get command will compare the file size and checksum with the catalog values

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")
        self.__sendReport('DONE', report)
        return 0, self.__pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from local dir to SE and register into dataset and catalogues """

        # Get input parameters from pdict
        token = pdict.get('token', '')
        analJob = pdict.get('analJob', False)
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        dsname = pdict.get('dsname', '')
        extradirs = pdict.get('extradirs', '')
        prodSourceLabel = pdict.get('prodSourceLabel', '')
        workDir = pdict.get('workDir', '')

        if prodSourceLabel == 'ddm' and analJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analJob = False

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'local'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            report['guid'] = guid.replace('-','')
#            report['dataset'] = dsname

        filename = os.path.basename(source)

        # get the local file size and checksum
        csumtype = self.checksum_command
        if fsize == 0 or fchecksum == 0:
            ec, self.__pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype=csumtype)
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, self.__pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize




        # THE FOLLOWING CODE UNTIL THE END OF THIS FUNCTION SHOULD BE REPLACED WITH NEW CODE FOR GLOBUS-ONLINE

        #Function uses Destination, source and endpoint as parameters.
        #Destination is the where the output Data will be transfered, like this:
        #SITE1:/home/osg_data/panda/
        #endp is the Endpoint corresponding to the working node
        #jobdir is the directory of the job (where the data is)
        #src is the data file that shall be transferes.

	#endp is the endpoint created user specified for working node.
	#jobdir is he directory where job is running and data is located
	#data is the data file to be transferred
	#dest is the destination


        def outcmdGO(dest, data, endp):
            """create command for output file using Globus Online. Uses job directory information
            to create output file path.
            """
            #Activate the destination endpoint, a gridFTP server
            destination = dest.split(':')[0]
            arg = ['gsissh', 'cli.globusonline.org', 'endpoint-activate','-g', destination]
            subprocess.call(arg)
               
            #Create transfer task
            cmd = 'gsissh cli.globusonline.org scp -v %s:%s/%s %s'%(endp, jobdir, data, dest)
            tolog("Transfer task created: %s" % (cmd))       
            return cmd



        #Function uses destination and source as parameters.
        #User specifies both of them at job submission, like this:
        #Source: OSGDEV:/direct/usatlas+u/ccontrer/pilot/autopilot/trunk/0064.file
        #Destination: SITE1:/home/osg_data/panda/
        #This function activates the destination endpoint, under the consideration
        #it is a gridFTP server. Source endpoint is already activated since it is a
        #Globus Connect endpoint and need no manual activation.
        #This function creates one transfer task, so it should be executed
        #for as many source files as the user specifies
        
        
	#As in Stage-in, source path can/should be generated with Endpoint and path obtaind from
	#user and job information

	src = endpoint + ':' + jobdir

	def stageoutcmdGO(dest, src):
            """create command for stage-out using Globus Online
            """
            #Activate the endpoint at destination
            destination = dest.split(':')[0]
            arg = ['gsissh', 'cli.globusonline.org', 'endpoint-activate','-g', destination]
            subprocess.call(arg)
               
            #Create transfer task
            cmd = 'gsissh cli.globusonline.org scp -v %s %s'%(src, dest)
            tolog("Transfer task created: %s" % (cmd))
            return cmd


        #After all transfers are done, Globus Connect must be stopped,
        #and all files removed, globusconnect application and configuration folder
        #at home directory ./globusonline
        
        def stopGC():
            """Stop Globus Connect on working node
            """
            tolog("----Stopping Globus Connect-----")
            arg = ['sh', 'globusconnect', '-stop']
            subprocess.call(arg)
            
            
        def removeGC():
            """Removes  Globus Connect and configuration files on working node
            """
            tolog("-----Removing Globus Connect Files-----")
            arg = ['rm','-rf','~/.globusonline/','globusconnect']
            subprocess.call(arg)

        #END OF GLOBUS ONLINE FUNCTIONS



        # get a proper envsetup
        envsetup = self.getEnvsetup()

        # Maybe be a comma list but take first always
        # (Remember that se can be a list where the first is used for output but any can be used for input)
        se = readpar('se').split(",")[0]
        _dummytoken, se = self.extractSE(se)
        tolog("Using SE: %s" % (se))
        tolog("Original destination: %s" % (destination))

        lfcpath, self.__pilotErrorDiag = self.getLFCPath(analJob)
        if lfcpath == "":
            self.__sendReport('STAGEOUT_FAIL', report)
            return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)

        # for production jobs, the SE path is stored in seprodpath
        # for analysis jobs, the SE path is stored in sepath
        if not analJob:
            # use seprodpath's if possible
            if readpar('seprodpath') != "":
                # process the destination path with getDirList since it can have a complext structure as well as
                # be a list of destination paths matching a corresponding space token
                destinationList = self.getDirList(readpar('seprodpath'))

                # decide which destination path to use depending on the space token for the current file
                if token:
                    # find the proper path
                    destination = self.getMatchingDestinationPath(token, destinationList)
                else:
                    # space tokens are not used
                    destination = destinationList[0]

                if destination == "":
                    tolog("!!WARNING!!2990!! seprodpath not properly defined, using sepath")
                    destination = readpar('sepath')
                tolog("Going to store production job output")
                # add the SRM path; e.g. "srm://head01.aglt2.org:8443/srm/managerv2?SFN=" + "/pnfs/aglt2.org/atlasmcdisk"
                destination = se + destination
            # add the extra stuff
            #if extradirs:
            #    destination = os.path.join(destination, extradirs)
            #if dsname:
            #    destination = os.path.join(destination, dsname)

            # NOTE: LFC registration is not done here but some of the LFC variables are used to find out
            # the disk path so the code has to be partially repeated here
            dst_loc_sedir, _dummy = self.getLCGPaths(destination, dsname, filename, lfcpath)
        else:
            if readpar('sepath') != "":
                destination = readpar('sepath')
                # add the SRM path; e.g. "srm://head01.aglt2.org:8443/srm/managerv2?SFN=" + "/pnfs/aglt2.org/atlasmcdisk"
                destination = se + destination
            pat = re.compile('([^\.]+\.[^\.]+)\..*')
            mat = pat.match(dsname)
            if mat:
                prefixdir = mat.group(1)
                destination = os.path.join(destination, prefixdir)
            else:
                self.__pilotErrorDiag = "put_data encountered unexpected dataset name format: %s" % (dsname)
                tolog('!!WARNING!!2990!! %s' % (self.__pilotErrorDiag))
                self.__sendReport('DSN_FORMAT_UNDEF', report)
                return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)
            dst_loc_sedir = os.path.join(destination, dsname)

            tolog("Going to store analysis job output")

        if dst_loc_sedir == '':
            self.__pilotErrorDiag = "put_data destination path in SE not defined"
            tolog('!!WARNING!!2990!! %s' % (self.__pilotErrorDiag))
            self.__sendReport('DEST_PATH_UNDEF', report)
            return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)

        tolog("Final destination: %s" % (destination))

        # final storage path
        dst_gpfn = os.path.join(dst_loc_sedir, filename)
        tolog("dst_gpfn: %s" % (dst_gpfn))

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=dst_gpfn)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        # build the command
        _params = ""
        if token:
            _params = self.__spacetoken % (token,)
        if fsize != 0:
            _params += self.__par_filesize % (fsize,)
        if fchecksum != 0:
            # special case for md5sum (command only understands 'md5' and 'adler32', and not 'ad' and 'md5sum')
            if csumtype == 'md5sum':
                _csumtype = 'md5'
            else:
                _csumtype = csumtype
            _params += self.__par_checksum % ("%s:%s" % (_csumtype, fchecksum),)

        # add the guid option
        _params += " --guid %s" % (guid)

        execStr = self.__localput % (envsetup, _params, source, dst_gpfn)
        tolog("Executing command: %s" % (execStr))
        report['transferStart'] = time()
        try:
            status, telapsed, cout, cerr = timed_command(execStr, self.__timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog(self.__warningStr % self.__pilotErrorDiag)
            status = 1
            output = str(e)
            telapsed = self.__timeout
        else:
            output = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))
        tolog("Command output:\n%s" % (output))
        report['validateStart'] = time()

        # validate
        if status:
            # did the copy command time out?
            if is_timeout(status):
                self.__pilotErrorDiag = "lsm-put failed: time out after %d seconds" % (telapsed)
                tolog(self.__warningStr % self.__pilotErrorDiag)            
                self.__sendReport('PUT_TIMEOUT', report)
                return self.put_data_retfail(self.__error.ERR_PUTTIMEOUT, self.__pilotErrorDiag)

            status = os.WEXITSTATUS(status)
            self.__pilotErrorDiag = 'lsm-put failed (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)
            self.__sendReport('COPY_FAIL', report)
            if status == 204 or status == 205: # size or checksum failed
                return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag, surl=dst_gpfn)
            else:
                return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)

        self.__sendReport('DONE', report)
        return 0, self.__pilotErrorDiag, dst_gpfn, fsize, fchecksum, self.arch_type

    def __sendReport(self, state, report):
        """
        Send DQ2 tracing report. Set the client exit state and finish
        """
        if report.has_key('timeStart'):
            # finish instrumentation
            report['timeEnd'] = time()
            report['clientState'] = state
            # send report
            tolog("Updated tracing report: %s" % str(report))
            self.sendTrace(report)
