# Base class of site movers
# All site movers inherit from this class

import os
import commands
import re
import time
from urllib import urlopen, urlencode
from urllib2 import Request, urlopen

from futil import *
from pUtil import tolog, readpar, dumpOrderedItems, getDirectAccessDic, getExtension, getSiteInformation
from PilotErrors import PilotErrors
from timed_command import timed_command
from config import config_sm

PERMISSIONS_DIR = config_sm.PERMISSIONS_DIR
PERMISSIONS_FILE = config_sm.PERMISSIONS_FILE
CMD_CHECKSUM = config_sm.COMMAND_MD5
ARCH_DEFAULT = config_sm.ARCH_DEFAULT
LFC_HOME = '/grid/atlas/'

class SiteMover(object):
    """
    File movers move files between a storage element (of different kinds) and a local directory    
    get_data: SE->local
    put_data: local->SE
    check_space: available space in SE
    getMover: static function returning a SiteMover
    
    It furter provides functions useful for child classes (AAASiteMover):
    put_data_retfail -- facilitate return in case of failure
    mkdirWperm -- create recursively dirs setting appropriate permissions
    getLocalFileInfo -- get size and checksum of a local file
    
    This is the Default SiteMover, the SE has to be locally accessible for all the WNs
    and all commands like cp, mkdir, md5checksum have to be available on files in the SE
    E.g. NFS exported file system
    """
    __childDict = {}

    copyCommand = "cp"
    checksum_command = "adler32"
    has_mkdir = True
    has_df = True
    has_getsize = True
    has_md5sum = True
    has_chmod = True
    permissions_DIR = PERMISSIONS_DIR
    permissions_FILE = PERMISSIONS_FILE
    arch_type = ARCH_DEFAULT
    timeout = 5*3600
    useTracingService = True
    filesInDQ2Dataset = {}

    CONDPROJ = ['oflcond', 'comcond', 'cmccond', 'tbcond', 'tbmccond', 'testcond']
    PRODFTYPE = ['AOD', 'CBNT', 'ESD', 'EVNT', 'HIST', 'HITS', 'RDO', 'TAG', 'log', 'NTUP']

    def __init__(self, setup_path='', *args, **kwrds):
        self._setup = setup_path
    
    def get_timeout(self):
        return self.timeout

    def getChecksumCommand(self):
        """ return the checksum command to be used with this site mover """
        return self.checksum_command

    def getID(self):
        """ return the current copy command """
        return self.copyCommand
    
    def getSetup(self):
        """ Return the setup string (pacman setup os setup script) for the copy command used by the mover """
        return self._setup

    def mountNSF4AndGetPFN(self, error, gpfn):
        """ Get and check PNFS mount point, return the pfn """

        ec = 0
        pilotErrorDiag = ""
        src_loc_pfn = ""

        try:
            if 'SFN' in gpfn:
                seName = gpfn.replace("srm://", "").split(':8446/srm/managerv2?SFN=')[0]
                src_loc_pfn = gpfn.split(':8446/srm/managerv2?SFN=')[1]
            else:
                seName = gpfn.replace("srm://", "").split('/')[0]
                src_loc_pfn = gpfn.split('%s' % (seName))[1]
#            seName = gpfn.replace("srm://", "").split(':8446/srm/managerv2?SFN=')[0]
#            src_loc_pfn = gpfn.split(':8446/srm/managerv2?SFN=')[1]
        except Exception, e:
            pilotErrorDiag = "Exception caught: %s" % (e)
            tolog("!!WARNING!!1887!! %s" % (pilotErrorDiag))
            ec = error.ERR_STAGEINFAILED
            return ec, pilotErrorDiag, src_loc_pfn

        _cmd_str = 'mount -l -t nfs4|grep %s' % (seName)
        timeout = 3600
        try:
            s, telapsed, cout, cerr = timed_command(_cmd_str, timeout)
        except Exception, e:
            tolog("!!WARNING!!1887!! timed_command() threw an exception: %s" % str(e))
            s = 1
            o = str(e)
            telapsed = timeout
        else:
            o = cout + cerr
            tolog("Elapsed time: %d" % (telapsed))

        if s == 0:
            try:
                pnfsMountPoint = o.split()[2]
            except Exception, e:
                pilotErrorDiag = "Exception caught: %s" % (e)
                tolog("!!WARNING!!1887!! %s" % (pilotErrorDiag))
                ec = error.ERR_STAGEINFAILED
            else:
                if os.path.ismount("%s" % (pnfsMountPoint)):
                    tolog("PNFS Server: %s, mount point: %s" % (seName, pnfsMountPoint))
                    src_loc_pfn = '%s%s' % (pnfsMountPoint, src_loc_pfn)
                else:
                    tolog("!!WARNING!!1887!! %s is no mount point" % (pnfsMountPoint))
                    pilotErrorDiag = "PNFS system error: %s" % (o)
                    ec = error.ERR_GETFAILEDTOMOUNTNFS4
        else:
            tolog("!!WARNING!!1887!! Command failed: %s" % (_cmd_str))
            if is_timeout(s):
                pilotErrorDiag = "Mount command was timed out after %d seconds" % (telapsed)
                ec = error.ERR_GETTIMEOUT
            else:
                pilotErrorDiag = "PNFS system error: %s" % (o)
                ec = error.ERR_GETPNFSSYSTEMERROR

        return ec, pilotErrorDiag, src_loc_pfn

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """
        Move a file from the local SE (where it was put from DDM) to the working directory.
        gpfn: full source URL (e.g. method://[host[:port]/full-dir-path/filename - a SRM URL is OK) 
        path: destination absolute path (in a local file system). It is assumed to be there. get_data returns an error if the path is missing
        The local file is assumed to have a relative path that is the same of the relative path in the 'gpfn'
        loc_...: variables used to access the file in the locally exported file system
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        timeout = pdict.get('timeout', 5*3600)
        experiment = pdict.get('experiment', "ATLAS")

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'sm', lfn, guid)

        # get the site information object
        si = getSiteInformation(experiment)

        src_loc_pfn = ''
        if si.isTier3():
            src_loc_pfn = gpfn
        else:
            if 'dpm' in gpfn:
                # Get and Check PNFS mount point
                ec, pilotErrorDiag, src_loc_pfn = self.mountNSF4AndGetPFN(error, gpfn)
                if ec != 0:
                    return ec, pilotErrorDiag
            else:
                # remove any host and SFN info from PFN path
                src_loc_pfn = self.extractPathFromPFN(gpfn)

        src_loc_filename = lfn
        # source vars: gpfn, loc_pfn, loc_host, loc_dirname, loc_filename
        # dest vars: path

        if fchecksum != 0 and fchecksum != "":
            csumtype = SiteMover.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = SiteMover.getLocalFileInfo(src_loc_pfn, csumtype=csumtype)
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return ec, pilotErrorDiag
        dest_file = os.path.join(path, src_loc_filename)

        # execute the copy command
        #PN
        _cmd_str = "cp %s %s" % (src_loc_pfn, dest_file)
#        if ".lib." in src_loc_pfn:
#            _cmd_str = _cmd_str.replace('XXX', '')

        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time.time()
        try:
            s, telapsed, cout, cerr = timed_command(_cmd_str, timeout)
        except Exception, e:
            tolog("!!WARNING!!2999!! timed_command() threw an exception: %s" % str(e))
            s = 1
            o = str(e)
            telapsed = timeout
        else:
            o = cout + cerr
            tolog("Elapsed time: %d" % (telapsed))
        report['validateStart'] = time.time()

        # error code handling
        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            check_syserr(s, o)
            if is_timeout(s):
                pilotErrorDiag = "cp get was timed out after %d seconds" % (telapsed)
                ec = error.ERR_GETTIMEOUT
            else:
                o = o.replace('\n', ' ')
                if o.find("No such file or directory") >= 0:
                    if src_loc_pfn.find("DBRelease") >= 0:
                        pilotErrorDiag = "DBRelease file missing: %s" % (src_loc_pfn)
                        ec = error.ERR_MISSDBREL
                    else:
                        pilotErrorDiag = "No such file or directory: %s" % (src_loc_pfn)
                        ec = error.ERR_NOSUCHFILE
                else:
                    pilotErrorDiag = "cp failed with output: ec = %d, output = %s" % (s, o)
                    ec = error.ERR_STAGEINFAILED

            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('COPY_FAIL', report)
            return ec, pilotErrorDiag

        # get remote file size and checksum
        ec, pilotErrorDiag, dstfsize, dstfchecksum = SiteMover.getLocalFileInfo(dest_file, csumtype=csumtype)
        if ec != 0:
            self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
            return ec, pilotErrorDiag

        # compare remote and local file size
        if dstfsize != fsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(gpfn), str(dstfsize), str(fsize))
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('FS_MISMATCH', report)
            return error.ERR_GETWRONGSIZE, pilotErrorDiag

        # compare remote and local file checksum
        if dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            if csumtype == "adler32":
                self.__sendReport('AD_MISMATCH', report)
                return error.ERR_GETADMISMATCH, pilotErrorDiag
            else:
                self.__sendReport('MD5_MISMATCH', report)
                return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag

    def isFileOnTape(surl):
        """ Check if the file is on tape """

        status = False

        # first get the corresponding DQ2 site name
        try:
            sitename = SiteMover.getDQ2SiteName(surl=surl)
        except:
            sitename = None
        if sitename:
            tolog("Attempting to use SiteMover.isTapeSite() for site %s" % (sitename))
            try:
                if SiteMover.isTapeSite(sitename):
                    status = True
            except Exception, e:
                tolog("Failed to execute isTapeSite(): %s (assuming replica is on disk)" % str(e))
                status = None
            if status:
                tolog("Replica is on tape: %s" % (surl))
            else:
                tolog("Replica is on disk: %s" % (surl))
        else:
            tolog("!!WARNING!!2999!! Site problem: Can not determine whether file is on tape since the DQ2 site name is unknown (setup file not sourced)")
            tolog("Replica is assumed to be on disk: %s" % (surl))

        return status
    isFileOnTape = staticmethod(isFileOnTape)

    def isTapeSite(sitename):
        """ Check whether the DQ2 site is a tape site or not """

        status = False
        try:
            from dq2.info import TiersOfATLAS
            if TiersOfATLAS.isTapeSite(sitename):
                status = True
            else:
                status = False
        except:
            tolog("Exception caught (assuming no tape site)")
            status = False
        return status
    isTapeSite = staticmethod(isTapeSite)

    def getDQ2SEType(dq2sitename):
        """ Return the corresponding setype for the site """

        setype = None
        try:
            from dq2.info import TiersOfATLAS
            setype = TiersOfATLAS.getSEType(dq2sitename)
        except:
            tolog("WARNING: getSEType failed")

        return setype
    getDQ2SEType = staticmethod(getDQ2SEType)

    def getDQ2SiteName(surl=None):
        """ Return the DQ2 site name using the SURL """

        sitename = None
        if surl:
            try:
                from dq2.info import TiersOfATLAS
            except:
                # Note: do not print the exception since it sometimes can not be converted to a string (as seen at Taiwan)
                tolog("!!WARNING!!1119!! TiersOfATLAS could not be imported from dq2.info")
            else:
                sites = TiersOfATLAS.getAllDestinationSites()
                for site in sites:
                    if TiersOfATLAS.isSURLFromSiteOrCloud(surl, site):
                        sitename = site
                        break
        return sitename
    getDQ2SiteName = staticmethod(getDQ2SiteName)

    def getDefaultDQ2SiteName(self):
        """ Return the DQ2 site name using the schedconfig.se info """

        # Build a preliminary SURL using minimum information necessary for the getDQ2SiteName() method
        default_token, se = SiteMover.extractSE(readpar('se'))
        tolog("default_token=%s, se=%s" % (default_token, se))

        # Get a preliminary path
        sepath = readpar('seprodpath')
        if sepath == "":
            sepath = readpar('sepath')

        # Note that the sepath might not be simple, but can contain complex structures (brackets and commas)
        # First create a properly formatted selist list and then use the default token to get the corresponding proper sepath
        destinationList = self.getDirList(sepath)
        tolog("destinationList=%s"%str(destinationList))

        # Now find the proper sepath
        destination = self.getMatchingDestinationPath(default_token, destinationList)
        tolog("destination=%s"%destination)

        # Create the SURL
        surl = se + destination
        tolog("surl=%s"%surl)

        # Get the default DQ2 site name
        return SiteMover.getDQ2SiteName(surl=surl)

    def getTiersOfATLASAlternativeName(self, endpoint):
        """ Return the alternativeName from TiersOfATLAS for a given edpoint """

        alternativeName = ""
        try:
            from dq2.info import TiersOfATLAS
        except:
            # Note: do not print the exception since it sometimes can not be converted to a string (as seen at Taiwan)
            tolog("!!WARNING!!1119!! TiersOfATLAS could not be imported from dq2.info")
        else:
            # Now get the alternativeName
            tolog("endpoint=%s"%endpoint)
            alternativeName = TiersOfATLAS.getSiteProperty(endpoint, 'alternateName')[0]

        return alternativeName

    def getTiersOfATLASSE(self, endpoint):
        """ Return the se from TiersOfATLAS """

        se = ""
        try:
            from dq2.info import TiersOfATLAS
        except:
            tolog("!!WARNING!!1119!! TiersOfATLAS could not be imported from dq2.info")
        else:
            # Get the sites list
            sites = TiersOfATLAS.ToACache.sites

            # Get the se info
            try:
                se = sites[endpoint]['srm']
            except Exception, e:
                tolog("!!WARNING!!1120!! No such endpoint in TiersOfATLAS: %s" % (e))
            else:
                tolog("Endpoint %s corresponds to se=%s (TiersOfATLAS)" % (endpoint, se))

        return se

    def getGroupDiskPath(self, endpoint=""):
        """ Get the seprodpath from TiersOfATLAS instead of schedconfig if destination is a groupdisk """
        # We know it's a group disk if 'dst:' is present in the token descriptor (which in this case it the same as the endpoint name)

        sepath = ""

        # Remove the dst: substring from the endpoint string unless the alternativeName is different between the site and the requested endpoint
        if "dst:" in endpoint:
            endpoint = endpoint[len('dst:'):]

            # Get the se from TiersOfATLAS
            se = self.getTiersOfATLASSE(endpoint)
            if se != "":
                # Now extract the seprodpath from the srm info
                sepath = SiteMover.extractSEPath(se)
                
                # Add /rucio to sepath if not there already
                if not sepath.endswith('/rucio'):
                    sepath += '/rucio'
            else:
                tolog("!!WARNING!!3999!! Group disk verification failed, space token will be reset to default value")
        else:
            tolog("!!WARNING!!2233!! Not a groupdisk endpoint: %s" % (endpoint))

        return sepath

    def verifyGroupSpaceToken(self, token):
        """ Make sure that space token is valid in case group disk is requested """
        # In case a groupdisk space token is requested, make sure that the site's alternativeName is the same as the endpoints' alternativeName
        # They will have different alternativeNames if the job originates from a different cloud
        # Note: ATLAS specific
        if not token:
            return None

        if token.startswith("dst:"):
            # Found a groupdisk space token
            _token = token[len('dst:'):]
            tolog("token=%s"%_token)
            tolog("sitename=%s"%self.getDefaultDQ2SiteName())
            # Get the corresponding alternative name and compare it to the alternative name of the site
            alternativeName_token = self.getTiersOfATLASAlternativeName(_token)
            tolog("alternativeName_token = %s" % (alternativeName_token))
            alternativeName_site = self.getTiersOfATLASAlternativeName(self.getDefaultDQ2SiteName())
            tolog("alternativeName_site = %s" % (alternativeName_site))

            # Only proceed ith getting the groupdisk path if the alternativeName's are the same
            if alternativeName_token == alternativeName_site:
                tolog("Verified groupdisk token (same alternativeName for site and endpoint)")
            else:
                tolog("!!WARNING!!3999!! Alternative names are not the same for site and requested endpoint, will reset GROUPDISK")
                default_token, _se = SiteMover.extractSE(readpar('se'))
                tolog("Requested space token %s reset to %s" % (_token, default_token))
                token = default_token

        return token

    def put_data_retfail(fail, errortext, surl=""):
        """
        Provides the return value for put_data when there is a failure.
        Used to enforce the number of parameters returned
        """
        return fail, errortext, surl, 0, 0, ''
    put_data_retfail = staticmethod(put_data_retfail)

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """
        Move the file from the current local directory to a storage element

        Parameters are:
        source: full path of the file in the local directory
        destination: destination SE, method://[hostname[:port]]/full-dir-path/ (NB: no file name)
        fsize: file size of the source file (evaluated if 0)
        fchecksum: checksum of the source file (evaluated if 0)
        pdict: to allow additional parameters that may make sense for specific movers
        
        Assume that the SE is locally mounted and its local path is the same as the remote path
        if both fsize and fchecksum (for the source) are given and !=0 these are assumed without reevaluating them
        returns: exitcode, gpfn, fsize, fchecksum
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        DN = pdict.get('DN', '')
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        scope = pdict.get('scope', '')
        dsname = pdict.get('dsname', '')
        timeout = pdict.get('timeout', 5*3600)
        analyJob = pdict.get('analJob', False)
        testLevel = pdict.get('testLevel', '0')
        extradirs = pdict.get('extradirs', '')
        experiment = pdict.get('experiment', 'ATLAS')
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'sm', lfn, guid)

        # get the checksum type
        if fchecksum != 0 and fchecksum != "":
            csumtype = SiteMover.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = SiteMover.getLocalFileInfo(source, csumtype="adler32")
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return SiteMover.put_data_retfail(ec, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # get the site information object
        si = getSiteInformation(experiment)

        # are we on a tier 3?
        if si.isTier3():
            dst_loc_se = SiteMover.getTier3Path(dsname, DN)
            dst_prefix = ""
            tolog("Writing output on a Tier 3 site to: %s" % (dst_loc_se))
        else:
            dst_se = destination
            if dst_se.find('SFN') != -1:  # srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv1?SFN=/pnfs/usatlas.bnl.gov/
                s = dst_se.split('SFN=')
                dst_loc_se = s[1]
                dst_prefix = s[0] + 'SFN='
            else:
                _sentries = dst_se.split('/', 3)
                try:
                    dst_prefix = _sentries[0] + '//' + _sentries[2] # 'method://host:port' is it always a ftp server? can it be srm? something else?
                    dst_loc_se = '/'+ _sentries[3]
                except Exception, e:
                    pilotErrorDiag = "Could not figure out destination path from dst_se (%s): %s" % (dst_se, str(e))
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.__sendReport('DEST_PATH_UNDEF', report)
                    return SiteMover.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        # VCH added check for Tier3 sites because the ds name is added to the path in SiteMove.getTier3Path()
        if si.isTier3():
            dst_loc_sedir = os.path.join(dst_loc_se, extradirs)
        else:
            dst_loc_sedir = os.path.join(dst_loc_se, os.path.join(extradirs, dsname))

        filename = os.path.basename(source)

        ec, pilotErrorDiag, tracer_error, dst_loc_pfn, lfcdir, surl = si.getProperPaths(error, analyJob, token, prodSourceLabel, dsname, filename, scope=scope)
        if ec != 0:
            self.__sendReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        #dst_loc_pfn = os.path.join(dst_loc_sedir, filename)
        dst_gpfn = dst_prefix + dst_loc_pfn

        try:
            SiteMover.mkdirWperm(os.path.dirname(dst_loc_pfn))
            #SiteMover.mkdirWperm(dst_loc_sedir)
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not create dir: %s, %s" % (dst_loc_sedir, str(e)))

        if testLevel == "1":
           source = "thisisjustatest"

        _cmd_str = "cp %s %s" % (source, dst_loc_pfn)
        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time.time()
        try:
            s, telapsed, cout, cerr = timed_command(_cmd_str, timeout)
        except Exception, e:
            tolog("!!WARNING!!2999!! timed_command() threw an exception: %s" % str(e))
            s = 1
            o = str(e)
            telapsed = timeout
        else:
            o = cout + cerr
            tolog("Elapsed time: %d" % (telapsed))
        report['validateStart'] = time.time()

        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            check_syserr(s, o)
            if is_timeout(s):
                pilotErrorDiag = "cp put was timed out after %d seconds" % (telapsed)
                ec = error.ERR_PUTTIMEOUT
            else:
                o = o.replace('\n', ' ')
                pilotErrorDiag = "cp failed with output: ec = %d, output = %s" % (s, o)
                ec = error.ERR_STAGEOUTFAILED
            self.__sendReport('COPY_FAIL', report)
            return SiteMover.put_data_retfail(ec, pilotErrorDiag)

            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))

        # get remote file size and checksum
        ec, pilotErrorDiag, dstfsize, dstfchecksum = SiteMover.getLocalFileInfo(dst_loc_pfn, csumtype="adler32")
        if ec != 0:
            self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
            return SiteMover.put_data_retfail(ec, pilotErrorDiag)

        # compare remote and local file size
        if dstfsize != fsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(dst_gpfn), str(dstfsize), str(fsize))
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('FS_MISMATCH', report)
            return SiteMover.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag)

        # compare remote and local checksums
        if dstfchecksum != fchecksum:
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(dst_gpfn), dstfchecksum, fchecksum)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            if csumtype == "adler32":
                self.__sendReport('AD_MISMATCH', report)
                return SiteMover.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag)
            else:
                self.__sendReport('MD5_MISMATCH', report)
                return SiteMover.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag)

        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag, str(dst_gpfn), fsize, fchecksum, ARCH_DEFAULT # Eddie added str, unicode protection

    def getLCGPaths(self, destination, dsname, filename, lfcpath):
        """ return the proper paths for lcg-cp/cr file transfer and registration """

        # return full lfc file path (beginning lfcpath might need to be replaced)
        native_lfc_path = self.to_native_lfn(dsname, filename)

        # /grid/atlas/dq2/testpanda/testpanda.destDB.b7cd4b56-1b5e-465a-a5d7-38d5e2609724_sub01000457/
        #58f836d5-ff4b-441a-979b-c37094257b72_0.job.log.tgz
        # tolog("Native_lfc_path: %s" % (native_lfc_path))

        # replace the default path /grid/atlas/dq2 with lfcpath if different
        # (to_native_lfn returns a path begining with /grid/atlas/dq2)
        default_lfcpath = '/grid/atlas/dq2' # to_native_lfn always returns this at the beginning of the string
        if default_lfcpath != lfcpath:
            final_lfc_path = native_lfc_path.replace(default_lfcpath, lfcpath)
        else:
            final_lfc_path = native_lfc_path

        stripped_lfcpath = os.path.dirname(native_lfc_path[len(default_lfcpath):]) # the rest (to be added to the 'destination' variable)
        # /testpanda/testpanda.destDB.b7cd4b56-1b5e-465a-a5d7-38d5e2609724_sub01000457/58f836d5-ff4b-441a-979b-c37094257b72_0.job.log.tgz
        # tolog("stripped_lfcpath: %s" % (stripped_lfcpath))

        # full file path for disk
        if stripped_lfcpath[0] == "/":
            stripped_lfcpath = stripped_lfcpath[1:]
        destination = os.path.join(destination, stripped_lfcpath)
        # /pnfs/tier2.hep.manchester.ac.uk/data/atlas/dq2/testpanda/testpanda.destDB.fcaf8da5-ffb6-4a63-9963-f31e768b82ef_sub01000345
        # tolog("Updated SE destination: %s" % (destination))

        # name of dir to be created in LFC
        lfcdir = os.path.dirname(final_lfc_path)
        # /grid/atlas/dq2/testpanda/testpanda.destDB.dfb45803-1251-43bb-8e7a-6ad2b6f205be_sub01000492
        # tolog("LFC dir: %s" % (lfcdir))

        return destination, lfcdir

    def getPreDestination(self, analyJob, token, prodSourceLabel, alt=False):
        """ get the pre destination """

        destination = ""

        # Special case for GROUPDISK
        # In this case, (e.g.) token = 'dst:AGLT2_PERF-MUONS'
        # Pilot should then consult TiersOfATLAS and get it from the corresponding srm entry 
        if "dst:" in token:
            # if the job comes from a different cloud than the sites' cloud, destination will be set to "" and the
            # default space token will be used instead (the transfer to groupdisk will be handled by DDM not pilot) 
            destination = self.getGroupDiskPath(endpoint=token)

            if destination != "":
                tolog("GROUPDISK token requested (%s), destination=%s" % (token, destination))
                return destination
            else:
                # Reset the space token to the default value
                default_token, _se = SiteMover.extractSE(readpar('se'))
                tolog("Requested space token %s reset to %s" % (token, default_token))
                token = default_token

        if not analyJob:
            # process the destination path with getDirList since it can have a complex structure
            # as well as be a list of destination paths matching a corresponding space token
            if prodSourceLabel == 'ddm' and readpar('seprodpath', alt=alt) == '':
                sepath = readpar('sepath', alt=alt)
            else:
                sepath = readpar('seprodpath', alt=alt)
            destinationList = self.getDirList(sepath)

            # decide which destination path to use depending on the space token for the current file
            if token:
                # find the proper path
                destination = self.getMatchingDestinationPath(token, destinationList, alt=alt)
                if destination == "":
                    tolog("!!WARNING!!2990!! seprodpath not properly defined: seprodpath = %s, destinationList = %s, using sepath instead" %\
                          (sepath, str(destinationList)))
                    sepath = readpar('sepath', alt=alt)
                    destinationList = self.getDirList(sepath)
                    destination = self.getMatchingDestinationPath(token, destinationList, alt=alt)
                    if destination == "":
                        tolog("!!WARNING!!2990!! sepath not properly defined: sepath = %s, destinationList = %s" %\
                              (sepath, str(destinationList)))
            else:
                # space tokens are not used
                destination = destinationList[0]
        else:
            sepath = readpar('sepath', alt=alt)
            destinationList = self.getDirList(sepath)

            # decide which destination path to use depending on the space token for the current file
            if token:
                # find the proper path
                destination = self.getMatchingDestinationPath(token, destinationList, alt=alt)
                if destination == "":
                    tolog("!!WARNING!!2990!! sepath not properly defined: sepath = %s, destinationList = %s" %\
                          (sepath, str(destinationList)))
            else:
                # space tokens are not used
                destination = destinationList[0]

        return destination

    def getUserLFCDir(destination, lfcpath, dsname):
        """ Get the LFC dir path for a user job """

        ec = 0
        pilotErrorDiag = ""
        lfcdir = ""

        # old pat = re.compile('([^\.]+\.[^\.]+)\..*')
        # pat = re.compile('([^\.]+\.[^\.]+\.[^\.]+[^\.]+)\..*')
        pat = re.compile('([^\.]+\.[^\.]+\.[^\.]+)\..*')
        mat = pat.match(dsname)
        if mat:
            # old prefixdir = mat.group(1) # 'user.pnilsson'
            subdirs = mat.group(1).split('.') # 'user.pnilsson.0915151927'
            _user = subdirs[0]     # 'user'
            _username = subdirs[1] # 'pnilsson'
            _field3 = subdirs[2]   # '0915151927'
            prefixdir = os.path.join(_user, _username, _field3)
            destination = os.path.join(destination, prefixdir)
            lfcdir = os.path.join(lfcpath, prefixdir, dsname)
            tolog("SE destination: %s" % (destination))
            tolog("LFC dir: %s" % (lfcdir))
        else:
            error = PilotErrors()
            ec = error.ERR_STAGEOUTFAILED
            pilotErrorDiag = "put_data encountered an unexpected dataset name format: %s" % (dsname)
            tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))

        return 0, pilotErrorDiag, destination, str(lfcdir) # Eddie added str, unicode protection

    getUserLFCDir = staticmethod(getUserLFCDir)

    def getRucioPath(self, file_nr, tokens, scope_dict, lfn, path, analysisJob):
        """ Return a Rucio style path """

        try:
            spacetoken = tokens[file_nr]
        except:
            spacetoken = ""
        try:
            scope = scope_dict[lfn]
        except Exception, e:
            tolog("!!WARNING!!1232!! Failed to extract scope from scope dictionary for file %s: %s" % (lfn, str(scope_dict)))
            tolog("Defaulting to old path style (based on dsname)")
            se_path = os.path.join(path, lfn)
        else:
            se_path = self.getFullPath(scope, spacetoken, lfn, analysisJob, "")

        return se_path

    def getFinalLCGPaths(self, analyJob, destination, dsname, filename, lfcpath, token, prodSourceLabel, scope="", alt=False):
        """
        set up paths differently for analysis and production jobs
        use conventional LFC paths or production jobs
        use special convention for analysis jobs (Aug-Sep 2011)
        """

        dst_gpfn = ""
        lfcdir = ""
        if "/rucio" in destination and scope != "":
            useRucio = True
        else:
            useRucio = False

        if analyJob: # for analysis jobs
            ec, pilotErrorDiag, destination, lfcdir = self.getUserLFCDir(destination, lfcpath, dsname)
            if ec != 0:
                return ec, pilotErrorDiag, dst_gpfn, lfcdir

            dst_gpfn = os.path.join(destination, os.path.join(dsname, filename))

        else:
            # get the proper paths
            destination, lfcdir = self.getLCGPaths(destination, dsname, filename, lfcpath)
            dst_gpfn = os.path.join(destination, filename)
            # /pnfs/tier2.hep.manchester.ac.uk/data/atlas/dq2/testpanda/testpanda.destDB.dfb45803-1251-43bb-8e7a-6ad2b6f205be_sub01000492

        # overwrite the dst_gpfn if path contains /rucio
        if useRucio:
            dst_gpfn = self.getPathFromScope(scope, filename)

            # correct for a potentially missing sepath
            sepath = self.getPreDestination(analyJob, token, prodSourceLabel, alt=alt)
            if not sepath in dst_gpfn:
                dst_gpfn = os.path.join(sepath, dst_gpfn)
                # correct for possible double rucio substring
                if "rucio/rucio" in dst_gpfn:
                    dst_gpfn = dst_gpfn.replace('rucio/rucio', 'rucio')

        return 0, "", dst_gpfn, lfcdir

    def check_space(self, ub):
        """Checking space availability

        This is a wrapper for functions specific to the different movers:
        1. check DQ space URL
        2. invoke _check_space, specific for each SiteMover
         (e.g. SiteMover's _check_space get storage path and check local space availability)
         
        """
        # http://bandicoot.uits.indiana.edu:8000/dq2/space/free
        # http://bandicoot.uits.indiana.edu:8000/dq2/space/total
        # http://bandicoot.uits.indiana.edu:8000/dq2/space/default For when space availability is not verifiable
        tolog("check_space called for: %s" % (ub))
        if ub == "" or ub == "None" or ub == None:
            tolog("Using alternative check space function since URL method can not be applied (URL not set)")
            retn = self._check_space(ub)
        else:
            try:
                tolog("Attempting urlopen with: %s" % (ub + '/space/free'))
                f = urlopen(ub + '/space/free')
                ret = f.read()
                tolog("ret: %s" % str(ret))
                retn = int(ret)
                if retn == 0:
                    retn = 999995
                    tolog(ub + '/space/free returned 0 space available, returning %d' % (retn))
            except:
                tolog("Using alternative check space function since URL method failed")
                retn = self._check_space(ub)
        return retn

    def _check_space(self, ub):
        """ Checking space of a local directory """

        fail = 0
        ret = ''
        if ub == "" or ub == "None" or ub == None:
            dst_loc_se = readpar('sepath')
            if dst_loc_se == "":
                tolog("WARNING: Can not perform alternative space check since sepath is not set")
                return -1
            else:
                tolog("Space check using df is no longer performed - will be replaced with SRM based space check")
                return -1
        else:
            try:
                f = urlopen(ub + '/storages/default')
            except Exception, e:
                tolog('!!WARNING!!2999!! Fetching default storage failed: %s' % str(e))
                return -1
            else:
                ret = f.read()

        if ret.find('//') == -1:
            tolog('!!WARNING!!2999!! Fetching default storage failed!')
            fail = -1
        else:
            dst_se = ret.strip()
            if (dst_se.find('SFN') != -1):  # srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv1?SFN=/pnfs/usatlas.bnl.gov/
                s = dst_se.split('SFN=')
                dst_loc_se = s[1]
            else:
                _sentries = dst_se.split('/', 3)
                dst_loc_se = '/'+ _sentries[3]

            avail = self.check_space_df(dst_loc_se)
            if avail == -1:
                fail = -1

        if fail != 0:
            return fail
        else:
            return avail

    def check_space_df(self, dst_loc_se): 
        """ Run df to check space availability """

        avail = -1
        s, o = commands.getstatusoutput('df %s' % (dst_loc_se))
        if s != 0:
            check_syserr(s, o)
            tolog("WARNING: Error in running df: %s" % str(o))
        else:
            output = o.strip().split('\n')
            for l in output:
                m = re.search('\s\s*([0-9]*)\s\s*([0-9]*)\s\s*([0-9]*)\%\s', l)
                if m != None:
                    avail = int(m.group(2))/1048576
                    break
        return avail

    def getStubTracingReport(self, initial_report, protocol, filename, guid):
        """ Return the first part of the tracing report """

        try:
            report = initial_report
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = protocol
            # mark the catalog (or relative?) start
            report['catStart'] = time.time()
            # the current file
            report['filename'] = filename
            # guid
            report['guid'] = guid.replace('-','')

        return report

    def sendTrace(self, report):
        """ Go straight to the tracing server and post the instrumentation dictionary """

        if not self.useTracingService:
            tolog("Experiment is not using Tracing service. skip sending tracing report")
            return

        url = 'https://rucio-lb-prod.cern.ch/traces/'
        tolog("Tracing server: %s" % (url))
        tolog("Sending tracing report: %s" % str(report))
        try:
            # take care of the encoding
            #data = urlencode({'API':'0_3_0', 'operation':'addReport', 'report':report})
            from json import dumps
            data = dumps(report).replace('"','\\"')

            from SiteInformation import SiteInformation
            si = SiteInformation()
            sslCertificate = si.getSSLCertificate()

            # create the command
            cmd = 'curl --connect-timeout 20 --max-time 120 --cacert %s -v -k -d "%s" %s' % (sslCertificate, data, url)
            tolog("Executing command: %s" % (cmd))
            s,o = commands.getstatusoutput(cmd)
            if s != 0:
                raise Exception(str(o))
        except:
            # if something fails, log it but ignore
            from sys import exc_info
            tolog('!!WARNING!!2999!! tracing failed: %s' % str(exc_info()))
        else:
            tolog("Tracing report sent")

    def __sendReport(self, state, report):
        """
        Send DQ2 tracing report. Set the client exit state and finish
        """
        if report.has_key('timeStart'):
            # finish instrumentation
            report['timeEnd'] = time.time()
            report['clientState'] = state
            # send report
            tolog("Updated tracing report: %s" % str(report))
            self.sendTrace(report)

    def getSURLDictionaryFilename(self, directory, jobId):
        """ return the name of the SURL dictionary file """

        return os.path.join(directory, "surlDictionary-%s.%s" % (jobId, getExtension()))

    def getSURLDictionary(self, directory, jobId):
        """ get the SURL dictionary from file """

        surlDictionary = {}

        # open the dictionary for reading
        filename = self.getSURLDictionaryFilename(directory, jobId)
        if not os.path.exists(filename):
            tolog("SURL dictionary does not exist: %s (will be created)" % (filename))
            return surlDictionary
        try:
            fp = open(filename, "r")
        except OSError, e:
            tolog("!!WARNING!!1800!! Failed to open SURL dictionary for reading: %s" % str(e))
        else:
            # get the dictionary
            importedLoad = False
            if filename.endswith('json'):
                try:
                    from json import load
                except Exception, e:
                    tolog("!!WARNING!!1800!! Could not import load function from json module (too old python version?): %s" % str(e))
                else:
                    importedLoad = True
            else:
                from pickle import load
                importedLoad = True

            if importedLoad:
                # load the dictionary from file
                try:
                    # load the dictionary from file
                    surlDictionary = load(fp)
                except:
                    tolog("!!WARNING!!1800!! JobState could not deserialize file: %s" % (filename))
                else:
                    tolog("Deserialized surl dictionary with %d keys" % len(surlDictionary.keys()))
                    tolog("surlDictionary=%s" % str(surlDictionary))
            fp.close()

        return surlDictionary

    def putSURLDictionary(self, surlDictionary, directory, jobId):
        """ store the updated SURL dictionary """

        status = False

        # open the dictionary for writing
        filename = self.getSURLDictionaryFilename(directory, jobId)
        try:
            fp = open(filename, "w")
        except OSError, e:
            tolog("!!WARNING!!1800!! Could not open SURL dictionary for writing: %s" % str(e))
        else:
            # write the dictionary
            if filename.endswith('json'):
                from json import dump
            else:
                from pickle import dump
            try:
                # write the dictionary to file
                dump(surlDictionary, fp)
            except Exception, e:
                tolog("!!WARNING!!1800!! Could not encode data to SURL dictionary file: %s, %s" % (filename, str(e)))
            else:
                status = True

                fp.close()

        return status

    def updateSURLDictionary(self, guid, surl, directory, jobId):
        """ add the guid and surl to the surl dictionary """

        status = False
        tolog("Adding GUID (%s) and SURL (%s) to dictionary" % (guid, surl))

        # (re-)open dictionary if possible (the dictionary will be empty if the function is called for the first time)
        surlDictionary = self.getSURLDictionary(directory, jobId)

        # add the guid and surl to the dictionary
        surlDictionary[guid] = surl

        # store the updated dictionary
        if self.putSURLDictionary(surlDictionary, directory, jobId):
            tolog("Successfully updated SURL dictionary (which currectly has %d key(s))" % len(surlDictionary.keys()))
            status = True
        else:
            tolog("!!FAILED!!1800!! SURL dictionary could not be updated (later LFC registration will not work)")

        return status

    def getFileInDataset(self, dataset, guid):
        """ Get the file info and if necessary populate the DQ2 dataset dictionary """

        fileInDataset = None

        if dataset == "":
            tolog("!!WARNING!!1110!! Dataset not set")
            return None

        # is the dataset info already available?
        if self.filesInDQ2Dataset.has_key(dataset):
            tolog("(already downloaded DQ2 file info)")
            # get the file info
            try:
                fileInDataset = self.filesInDQ2Dataset[dataset][guid]
            except Exception, e:
                tolog("!!WARNING!!1111!! GUID = %s not found in DQ2 dataset (%s): %s" % (guid, dataset, e))
        else:
            tolog("(will download DQ2 file info)")
            try:
                from dq2.clientapi.DQ2 import DQ2
                dq2 = DQ2()
                dataset_info = dq2.listFilesInDataset(dataset)[0]
            except: # listFilesInDataset is not a subclass of Exception, so only use an except without Exception here
                import sys
                excType, excValue = sys.exc_info()[:2] # skip the traceback info to avoid possible circular reference
                tolog("!!WARNING!!1112!! Failed to get dataset = %s from DQ2" % (dataset))
                tolog("excType=%s" % (excType))
                tolog("excValue=%s" % (excValue))
            else:
                # add the new dataset to the dictionary
                self.filesInDQ2Dataset[dataset] = dataset_info

                # finally get the file info
                try:
                    fileInDataset = self.filesInDQ2Dataset[dataset][guid]
                except Exception, e:
                    tolog("!!WARNING!!1113!! GUID = %s not found in DQ2 dataset (%s): %s" % (guid, dataset, e))

        return fileInDataset

    def getFileInfoFromDQ2(self, dataset, guid):
        """ Get the file size and checksum from DQ2 """

        filesize = ""
        checksum = ""

        # get the file info
        fileInDataset = self.getFileInDataset(dataset, guid)
        if fileInDataset:
            try:
                lfn = fileInDataset["lfn"]
                full_checksum = fileInDataset["checksum"]
                tmp = full_checksum.split(":")
                checksum_type = tmp[0]
                checksum = tmp[1]
                filesize = str(fileInDataset["filesize"])
            except Exception, e:
                tolog("!!WARNING!!1114!! Failed to get file info from DQ2 (using default LFC values for file size and checksum): %s" % (e))
                filesize = ""
                checksum = ""
            else:
                tolog("DQ2 file info for LFN = %s: file size = %s, checksum = %s (type: %s)" % (lfn, filesize, checksum, checksum_type))
        else:
            tolog("!!WARNING!!1115!! Failed to get file info from DQ2 (using default LFC values for file size and checksum)")

        return filesize, checksum

    def verifyPaths(self, paths):
        """ Verify existence of paths """

        status = False
        badPath = ""

        for path in paths:
            if not os.path.exists(path):
                badPath = path

        # if no found bad paths, set return status to True
        if badPath == "":
            status = True

        return status, badPath

    def reportFileCorruption(surl):
        """ Report a corrupted file to the consistency server """

        # clean up the SURL before reporting it
        surl = re.sub(':[0-9]+/', '/', surl)
        surl = re.sub('/srm/v2/server\?SFN=', '', surl)
        surl = re.sub('/srm/managerv1\?SFN=', '', surl)
        surl = re.sub('/srm/managerv2\?SFN=', '', surl)
        tolog("Cleaned up SURL: %s" % (surl))
        try:
            from dq2.clientapi.DQ2 import DQ2
            dq2 = DQ2()
            dq2.declareSuspiciousFiles(surls=[surl], reason='File corrupted', reportedby='p')
        except:
            tolog("!!WARNING!!2111!! Failed to report corrupted file to consistency server")
        else:
            tolog("Reported corrupted file to consistency server: %s" % (surl))
    reportFileCorruption = staticmethod(reportFileCorruption)

    def getMover(cls, *args, **kwrds):
        """
        Creates and provides exactly one instance for each required subclass of SiteMover.
        Implements the Singleton pattern. Method is incomplete.
        """
        cl_name = cls.__name__
        if not issubclass(cls, SiteMover):
            log.error("Wrong Factory invocation, %s is not subclass of SiteMover" % cl_name)
        else:
            return cls(*args, **kwrds)
    getMover = classmethod(getMover)

    # Utility Functions
    def mkdirWperm(newdir):
        """
        - if the dir already exists, silently completes
        - if a regular file is in the way, raise an exception
        - parent directory does not exist, make it as well 
        Permissions are set as they should be.
        PERMISSIONS_DIR is loaded from config.config_sm and it is currently 0775 (group write)
        """
        tolog("Creating dir %s" % newdir)
        
        if os.path.isdir(newdir):
            pass
        elif os.path.isfile(newdir):
            raise OSError("a file with the same name as the desired dir, '%s', already exists." % (newdir))
        else:
            head, tail = os.path.split(newdir)
            if head and not os.path.isdir(head):
                SiteMover.mkdirWperm(head)
            if tail:
                try:
                    os.mkdir(newdir)
                except OSError, e:
                    if not os.path.isdir(newdir):
                        raise e
                # Desired dir permission
                # remember to use octal 0xxx
                # is the desired permission 0775 (group writes) or 0755 (only user writes)?
                os.chmod(newdir, PERMISSIONS_DIR)
    mkdirWperm = staticmethod(mkdirWperm)

    def verifyLocalFile(fsize, fchecksum, fname, extra_path=''):
        """
        Checks if local copy is correct
        returns 0 if OK
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        if fsize == 0 or SiteMover.isDummyChecksum(fchecksum):
            return 0, pilotErrorDiag

        # set checksum type
        if fchecksum != 0 and fchecksum != "":
            csumtype = SiteMover.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # evaluate and compare (size, checksum)
        dest_file = fname
        if extra_path:
            dest_file = os.path.join(extra_path, dest_file)

        ec, pilotErrorDiag, dstfsize, dstfchecksum = SiteMover.getLocalFileInfo(dest_file, csumtype=csumtype)
        if ec != 0:
            return ec, pilotErrorDiag

        if fsize != 0 and fsize != '0' and dstfsize != fsize:
            return error.ERR_GETWRONGSIZE, pilotErrorDiag

        # WARNING: note that currenty only HUSiteMover is using verifyLocalFile(), and only in get_data()
        if fchecksum != 0 and dstfchecksum != fchecksum and not SiteMover.isDummyChecksum(fchecksum):
            if csumtype == "adler32":
                return error.ERR_GETADMISMATCH, pilotErrorDiag
            else:
                return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        return 0, pilotErrorDiag
    verifyLocalFile = staticmethod(verifyLocalFile)

    def getLocalFileSize(self, filename):
        """ Get the file size of a local file (return a string) """

        filesize = ""

        if os.path.exists(filename):
            try:
                filesize = os.path.getsize(filename)
            except Exception, e:
                tolog("!!WARNING!!1232!! Failed to get file size: %s" % (e))
            else:
                # convert to string
                filesize = str(filesize)
        else:
            tolog("!!WARNING!!1233!! Local file does not exist: %s" % (filename))

        return filesize

    def getLocalFileInfo(fname, csumtype="default", date=None):
        """ Return exit code (0 if OK), file size and checksum of a local file, as well as as date string if requested """
        # note that date is mutable

        error = PilotErrors()
        pilotErrorDiag = ""

        # does the file exist?
        if not os.path.isfile(fname):
            if fname.find("DBRelease") >= 0 and os.path.exists(os.path.dirname(fname)):
                pilotErrorDiag = "DBRelease file missing: %s" % (fname)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_MISSDBREL, pilotErrorDiag, 0, 0
            else:
                pilotErrorDiag = "No such file or directory: %s" % (fname)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_MISSINGLOCALFILE, pilotErrorDiag, 0, 0

            # get the modification time if needed and store it in the mutable object
            if date:
                date = SiteMover.getModTime(os.path.dirname(fname), os.path.basename(fname))

        # get the file size
        try:
            tolog("Executing getsize() for file: %s" % (fname))
            fsize = str(os.path.getsize(fname))
        except OSError, e:
            pilotErrorDiag = "Could not get file size: %s" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_FAILEDSIZELOCAL, pilotErrorDiag, 0, 0
        else:
            if fsize == "0":
                pilotErrorDiag = "Encountered zero file size for file %s" % (fname)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_ZEROFILESIZE, pilotErrorDiag, 0, 0
            else:
                tolog("Got file size: %s" % (fsize))

        # get the checksum
        if csumtype == "adler32":
            tolog("Executing adler32() for file: %s" % (fname))
            fchecksum = SiteMover.adler32(fname)
            if fchecksum == '00000001': # "%08x" % 1L
                pilotErrorDiag = "Adler32 failed (returned 1)"
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDADLOCAL, pilotErrorDiag, fsize, 0
            else:
                tolog("Got adler32 checksum: %s" % (fchecksum))
        else:
            _cmd = '%s %s' % (CMD_CHECKSUM, fname)
            tolog("Executing command: %s" % (_cmd))
            try:
                s, o = commands.getstatusoutput(_cmd)
            except Exception, e:
                s = -1
                o = str(e)
                tolog("!!WARNING!!2999!! Exception caught in getstatusoutput: %s" % (o))
            if s != 0:
                o = o.replace('\n', ' ')
                check_syserr(s, o)
                pilotErrorDiag = "Error running checksum command (%s): %s" % (CMD_CHECKSUM, o)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDMD5LOCAL, pilotErrorDiag, fsize, 0
            fchecksum = o.split()[0]
            tolog("Got checksum: %s" % (fchecksum))

        return 0, pilotErrorDiag, fsize, fchecksum

    getLocalFileInfo = staticmethod(getLocalFileInfo)

    def dumpExtendedProxy(setupstr=''):
        """ run voms-proxy-info -all """

        tmp = setupstr.strip()
        if tmp != "" and not tmp.endswith(';'):
            tmp += ";"
        if os.environ.has_key('X509_USER_PROXY') and tmp.find('export X509_USER_PROXY=') == -1:
            tmp += "export X509_USER_PROXY=%s;" % (os.environ['X509_USER_PROXY'])
        tmp = tmp.replace(";;",";")

        cmd = "%svoms-proxy-info -all --file $X509_USER_PROXY" % (tmp)
        tolog("Executing command: %s" % (cmd))
        exitcode, output = commands.getstatusoutput(cmd)
        tolog("Output: %d, %s" % (exitcode, output))
    dumpExtendedProxy = staticmethod(dumpExtendedProxy)

    # Code taken from:
    # http://isscvs.cern.ch/cgi-bin/viewcvs-all.cgi/dq2.filecatalog.lfc/lib/dq2/filecatalog/lfc/lfcconventions.py?revision=1.4&root=atlas-dq2&view=markup&pathrev=dq2-filecatalog-lfc-0-7-0-branch
    def __strip_tag(tag):
        """
        Drop the _sub and _dis suffixes for panda datasets from the lfc path
        they will be registered in
        """

        suffixes_to_drop = ['_dis','_sub','_tid']
        #try:
        #    suffixes_to_drop.extend(LFCFileCatalogConfigurator().getTagSuffixesList())
        #except:
        #    pass

        stripped_tag = tag
        try:
            for suffix in suffixes_to_drop:
                stripped_tag = re.sub('%s.*$' % suffix, '', stripped_tag)
        except IndexError:
            return stripped_tag

        return stripped_tag 
    __strip_tag = staticmethod(__strip_tag)

    # Code taken from same source as above
    def __strip_dsn(dsn):
        """
        Drop the _sub and _dis suffixes for panda datasets from the lfc path
        they will be registered in
        """

        suffixes_to_drop = ['_dis','_sub','_frag']
        #try:
        #    suffixes_to_drop.extend(LFCFileCatalogConfigurator().getPandaSuffixesList())
        #except:
        #    pass

        fields = dsn.split('.')
        last_field = fields[-1]

        try:
            for suffix in suffixes_to_drop:
                last_field = re.sub('%s.*$' % suffix, '', last_field)
        except IndexError:
            return dsn

        fields[-1] = last_field
        stripped_dsn = '.'.join(fields)

        return stripped_dsn
    __strip_dsn = staticmethod(__strip_dsn)

    # old to_native_lfn taken from:
    # http://atlas-sw.cern.ch/cgi-bin/viewcvs-atlas.cgi/offline/DataManagement/DQ2/dq2.filecatalog.lfc/lib/dq2/filecatalog/lfc/lfcconventions.py?view=log

    # Code taken from same source as strip functions above
    def to_native_lfn(dsn, lfn, prefix='dq2/'):
        """
        Return LFN with LFC hierarchical namespace.
        """

        bpath = LFC_HOME
        if bpath[-1] != '/': bpath += '/'

        # add prefix
        bpath += prefix
        if bpath[-1] == '/': bpath = bpath[:-1]

        # check how many dots in dsn
        fields = dsn.split('.')
        nfields = len(fields)

        if nfields == 1:
            stripped_dsn = SiteMover.__strip_dsn(dsn)
            return '%s/other/%s/%s' % (bpath, stripped_dsn, lfn)

        elif nfields == 2:
            project = fields[0]
            stripped_dsn = SiteMover.__strip_dsn(dsn)
            return '%s/%s/%s/%s' % (bpath, project, stripped_dsn, lfn)

        elif nfields < 5 or re.match('user*|group*',fields[0]):
            project = fields[0]
            f2 = fields[1]
            f3 = fields[2]
            stripped_dsn = SiteMover.__strip_dsn(dsn)
            return '%s/%s/%s/%s/%s/%s' % (bpath, project, f2, f3, stripped_dsn, lfn)

        else:
            project = fields[0]
            dataset_type = fields[4]
            if nfields == 5:
                tag='other'
            else:
                tag = SiteMover.__strip_tag(fields[-1])
            stripped_dsn = SiteMover.__strip_dsn(dsn)
            return '%s/%s/%s/%s/%s/%s' % (bpath, project, dataset_type, tag, stripped_dsn, lfn)
    to_native_lfn = staticmethod(to_native_lfn)

    def adler32(filename):
        """ calculate the checksum for a file with the zlib.adler32 algorithm """
        # note: a failed file open will return '1'

        import zlib
        # default adler32 starting value
        sum1 = 1L

        try:
            f = open(filename, 'rb')
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not open file: %s" % (filename))
        else:
            try:
                for line in f:
                    sum1 = zlib.adler32(line, sum1)
            except Exception, e:
                tolog("!!WARNING!!2777!! Exception caught in zlib.adler32: %s" % (e))

            f.close()

        # correct for bug 32 bit zlib
        if sum1 < 0:
            sum1 = sum1 + 2**32

        # convert to hex
        sum2 = "%08x" % sum1

        return str(sum2)

    adler32 = staticmethod(adler32)

    def doFileVerifications():
        """ Should the get operation perform any file size/checksum verifications? """
        # not for storm sites
        # also used to skip input file size checks when mv site mover is used (from Mover)

        _copytool = readpar('copytool')
        _copytoolin = readpar('copytoolin')
        if _copytoolin == "storm" or _copytoolin == "mv" or (_copytoolin == "" and (_copytool == "storm" or _copytool == "mv")):
            doVerification = False
        else:
            doVerification = True

        return doVerification

    doFileVerifications = staticmethod(doFileVerifications)

    def extractPathFromPFN(gpfn):
        """ Remove any host and SFN info from PFN path """
        # gpfn = srm://srm-atlas.gridpp.rl.ac.uk:8443/srm/managerv2?SFN=/castor/ads.rl.ac.uk/prod/atlas/...
        # -> path = /castor/ads.rl.ac.uk/prod/atlas/...
        # gpfn = srm://srm-atlas.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/atlas/...
        # -> path = /castor/ads.rl.ac.uk/prod/atlas/...

        if "SFN" in gpfn:
            src_loc_pfn = gpfn.split('SFN=')[1]
        else:
            src_loc_pfn = '/' + gpfn.split('/', 3)[3] # 0:method, 2:host+port, 3:abs-path

        return src_loc_pfn

    extractPathFromPFN = staticmethod(extractPathFromPFN)

    def getChecksumType(csum, format="long"):
        """ return the checksum type given only the checksum value """
        # format = "long" returns either "adler32" or "md5sum"
        # format = "short" returns either "AD" or "MD"

        # force string conversion in case None or 0 should be sent
        csum = str(csum)
        if len(csum) == 8:
            if format == "long":
                csumtype = "adler32"
            else:
                csumtype = "AD"
        elif len(csum) == 32:
            if format == "long":
                csumtype = "md5sum"
            else:
                csumtype = "MD"
        else:
            csumtype = CMD_CHECKSUM

        return csumtype

    getChecksumType = staticmethod(getChecksumType)

    def getLFCChecksumType(lfn):
        """ get the checksum type (should be MD or AD) """

        try:
            import lfc
        except Exception, e:
            pilotErrorDiag = "getLFCChecksumType() could not import lfc module: %s" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return None

        os.environ['LFC_HOST'] = readpar('lfchost')
        stat = lfc.lfc_filestatg()
        rc = lfc.lfc_statg(lfn, "", stat)
        if rc != 0:
            err_num = lfc.cvar.serrno
            err_string = lfc.sstrerror(err_num)
            tolog("!!WARNING!!2999!! (rc = %d) lfc_statg failed for lfn %s with: %d, %s" %\
                  (rc, lfn, err_num, err_string))
            return None
        else:
            return stat.csumtype

    getLFCChecksumType = staticmethod(getLFCChecksumType)

    def isDummyChecksum(fchecksum):
        """ ignore dummy checksum values, e.g. used in the M4/5 cosmics tests """
        # Also skipping checksum values of "0" from Aug 12, 2008 (v 24g)
        dummy = False
        try:
            if fchecksum == "0":
                tolog("!!WARNING!!2999!! Came across a dummy checksum value (%s). Skipping checksum test" % (fchecksum))
                dummy = True
            elif fchecksum == "00000000000000000000000000000000":
                tolog("!!WARNING!!2999!! Came across a dummy md5sum value (%s). Skipping md5sum test" % (fchecksum))
                dummy = True
            elif fchecksum == "00000000":
                tolog("!!WARNING!!2999!! Came across a dummy adler32 value (%s). Skipping adler32 test" % (fchecksum))
                dummy = True
        except:
            pass

        return dummy

    isDummyChecksum = staticmethod(isDummyChecksum)

    def addFileInfo(lfn, checksum, csumtype='MD', fsize=None):
        """ add MD checksum to lfn (change to AD later) """

        pilotErrorDiag = ""
        tolog("Checksum type to be set: %s" % (csumtype))

        try:
            import lfc
        except Exception, e:
            pilotErrorDiag = "addFileInfo() could not import lfc module: %s" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return -1, pilotErrorDiag

        os.environ['LFC_HOST'] = readpar('lfchost')
        try:
            stat = lfc.lfc_filestatg()
            # Eddie
            # Make sure lfn is not unicode due to a bug in LFC libs that returns the following error:
            # 'lfc_statg', argument 1 of type 'char const *'
            rc = lfc.lfc_statg(str(lfn), "", stat)
        except Exception, e:
            pilotErrorDiag = "lfc function failed in addFileInfo(): %s" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return -1, pilotErrorDiag

        if rc != 0:
            err_num = lfc.cvar.serrno
            errstr = lfc.sstrerror(err_num)
            pilotErrorDiag = "lfc_statg failed for lfn: %s : rc = %d, err_num = %d, errstr = %s" % (lfn, rc, err_num, errstr)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return 1, pilotErrorDiag

        if fsize:
            filesize = long(fsize)
        else:
            filesize = stat.filesize
        rc = lfc.lfc_setfsizeg(stat.guid, filesize, csumtype, checksum)
        if rc != 0:
            err_num = lfc.cvar.serrno
            errstr = lfc.sstrerror(err_num)
            pilotErrorDiag = "lfc_setfsizeg failed for lfn: %s : rc = %d, err_num = %d, errstr = %s" % (lfn, rc, err_num, errstr)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return rc, pilotErrorDiag
        tolog("Successfully set checksum (%s of type %s) and file size %s for %s" % (checksum, csumtype, str(filesize), lfn))
        return rc, pilotErrorDiag

    addFileInfo = staticmethod(addFileInfo)

    def getProperSE(token, alt=False):
        """ get the proper endpoint """

        se = ""
        useDefaultSE = True

        # do we have several endpoints to chose from?
        _seopt = readpar('seopt', alt=alt)
        if _seopt != '':
            # Find the right endpoint, corrsponding to the given space token
            setokens = readpar('setokens', alt=alt).split(",")
            seopt = _seopt.split(",")
            tolog("seopt: %s" % str(seopt))
            tolog("token: %s" % (token))
            tolog("setokens: %s" % str(setokens))
            if len(seopt) != len(setokens):
                tolog("!!WARNING!!2999!! seopt does not have the same length as setokens: %s, %s" % (str(seopt), str(setokens)))
            else:
                # create the dictionary
                sedict = dict(zip(setokens, seopt))
                # lookup the endpoint corresponding to token
                if token in setokens:
                    try:
                        se = sedict[token]
                    except Exception, e:
                        tolog("!!WARNING!!2999!! Could not find a matching endpoint to token %s: %s, %s" %\
                              (token, str(sedict), str(e)))
                    else:
                        useDefaultSE = False
                else:
                    tolog("Token %s is not among valid tokens: %s" % (token, str(setokens)))

        if useDefaultSE:
            # Stick with the default se
            # Maybe be a comma list but take first always
            # (Remember that se can be a list where the first is used for output but any can be used for input)
            tolog("Will use default SE endpoint")
            se = readpar('se', alt=alt).split(",")[0]
            _dummytoken, se = SiteMover.extractSE(se)

        # remove any unwanted stage-in info (present at CERN for atlasdatatape)
        se = SiteMover.filterSE(se)
        tolog("Using SE: %s" % (se))
        return se

    getProperSE = staticmethod(getProperSE)

    def filterSE(se):
        """ Filter away any unwanted stage-in info from the file path """

        # "/castor/cern.ch/grid/atlas/((tzero/prod1/perm)|(tzero/prod2/perm)|(t0/perm)|(DAQ)|(conditions)|(atlasdatatape))"
        # -> /castor/cern.ch/grid/atlas/atlasdatatape

        match = re.findall('\(\S+\)', se)
        if match and "atlasdatatape" in se:
            filtered_path = match[0]
            tolog("Found unwanted stage-in info in SE path, will filter it away: %s" % (filtered_path))
            se = se.replace(filtered_path, "atlasdatatape") 

        return se

    filterSE = staticmethod(filterSE)

    def extractHostname(se):
        """ extract the hostname from the given se info """
        # e.g. token:ATLASPRODDISK:srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv2?SFN= -> dcsrm.usatlas.bnl.gov

        hostname_pattern = re.compile('.+://([^/:]+):\d+/')
        if se != "":
            _hostname = re.findall(hostname_pattern, se)
            if _hostname != []:
                hostname = _hostname[0]
            else:
                tolog("!!WARNING!!2999!! Hostname could not be extracted from se=%s (reg exp pattern too weak?)" % (se))
                hostname = ""
        else:
            tolog("!!WARNING!!2999!! SE not defined, hostname can not be extracted")
            hostname = ""

        return hostname
    extractHostname = staticmethod(extractHostname)

    def getSEMatchFromSEOpt(srm):
        """ Extract the full SE path from seopt that matches srm """

        # seopt = "token:ATLASDATADISK:srm://f-dpm001.grid.sinica.edu.tw:8446/srm/managerv2?SFN=,token:ATLASDATATAPE:srm://srm2.grid.sinica.edu.tw:8443/srm/managerv2?SFN=,token:ATLASMCTAPE:srm://srm2.grid.sinica.edu.tw:8443/srm/managerv2?SFN="
        # srm = srm://srm2.grid.sinica.edu.tw -> sematch = srm://srm2.grid.sinica.edu.tw:8443/srm/managerv2?SFN=

        sematch = ""
        seopt = readpar('seopt')

        if seopt != "":
            seopt_list = seopt.split(",")
            for _seopt in seopt_list:
                token, path = SiteMover.extractSE(_seopt)

                if srm in path:
                    sematch = path
                    break

        return sematch
    getSEMatchFromSEOpt = staticmethod(getSEMatchFromSEOpt)

    def extractSEPath(se):
        """ Extract the sepath from the se info """

        # se='token:ATLASGROUPDISK:srm://head01.aglt2.org:8443/srm/managerv2?SFN=/pnfs/aglt2.org/atlasgroupdisk/perf-muons/'
        # -> '/pnfs/aglt2.org/atlasgroupdisk/perf-muons/'

        sepath = ""
        pattern = re.compile(r"SFN=(.+)")
        found = re.findall(pattern, se)
        if len(found) > 0:
            sepath = found[0]

        return sepath
    extractSEPath = staticmethod(extractSEPath)

    def extractSE(fullSE):
        """ extract the 'se' info from the schedconfig.se field """
        # examples:
        # fullSE = token:ATLASMCDISK:srm://srm-atlas.gridpp.rl.ac.uk:8443/srm/managerv2/something/somethingelse/mcdisk/
        # return: ATLASMCDISK, srm://srm-atlas.gridpp.rl.ac.uk:8443/srm/managerv2/something/somethingelse/mcdisk/
        # fullSE = srm://srm-atlas.gridpp.rl.ac.uk:8443/path
        # return: None, srm://srm-atlas.gridpp.rl.ac.uk:8443/path

        token = None
        path = None

        # does the fullSE contain a space token?
        if fullSE[:len("token")] == "token":
            # splitFullSE = ['token', 'ATLASMCDISK', 'srm', '//srm-atlas.gridpp.rl.ac.uk', '8443/srm/managerv2/something/somethingelse/mcdisk/']
            splitFullSE = fullSE.split(":")
            # token = ATLASMCDISK
            token = splitFullSE[1]
            # remainingFullSE = ['srm', '//srm-atlas.gridpp.rl.ac.uk', '8443/srm/managerv2/something/somethingelse/mcdisk/']
            remainingFullSE = splitFullSE[2:]
            from string import join
            # path = srm://srm-atlas.gridpp.rl.ac.uk:8443/srm/managerv2/something/somethingelse/mcdisk/
            path = join(remainingFullSE, ":")
        else:
            path = fullSE

        return token, path

    extractSE = staticmethod(extractSE)

    def getSEFromToken(token):
        """ Match an SE to a space token descriptor """

        stripped_se = ""
        seopt = readpar("seopt")

        if seopt != "":
            seopt_list = seopt.split(",")

            # match an SE to the space token
            for se in seopt_list:
                _token, _se = SiteMover.extractSE(se)
                if _token == token:
                    # remove the post and version strings
                    stripped_se = SiteMover.stripPortAndVersion(_se)

        return stripped_se

    getSEFromToken = staticmethod(getSEFromToken)
                                                                
    def getTokenFromPath(path):
        """ return the space token from an SRMv2 end point path """
        # example:
        # path = srm://srm-atlas.gridpp.rl.ac.uk:8443/srm/managerv2/something/somethingelse/mcdisk/
        # return: ATLASMCDISK

        # remove any trailing / (..mcdisk/ -> ../mcdisk)
        if path[-1] == '/':
            path = path[:-1]

        # ATLASMCDISK
        return "ATLAS" + os.path.basename(path).upper()

    getTokenFromPath = staticmethod(getTokenFromPath)

    def stripListSEs(listSEs):
        """ remove any space token info from the SE list """
        # ['token:BLAH:srm://path1', 'srm://path2']
        # return ['srm://path1', 'srm://path2']

        strippedListSEs = []
        for se in listSEs:
            if se[:len('token')] == "token":
                # extract the token (not needed) and the path (without the token)
                token, sepath = SiteMover.extractSE(se)
                # remove port and SRM version number as well since it is not needed for replica comparison
                # (replica.sfn does not contain port and version number)
                sepath = SiteMover.stripPortAndVersion(sepath)
                strippedListSEs += [sepath]
            else:
                strippedListSEs += [se]
        return strippedListSEs

    stripListSEs = staticmethod(stripListSEs)

    def stripPortAndVersion(path):
        """ remove port and SFN (e.g. ':8443/srm/managerv2?SFN=' from path (if any) """
        # 'srm://gridka-dcache.fzk.de:8443/srm/managerv2?SFN=/pnfs/gridka.de/atlas/disk-only/mc/simone_test'
        # => 'srm://gridka-dcache.fzk.de/pnfs/gridka.de/atlas/disk-only/mc/simone_test'

        pattern = re.compile(r"(\:\d+/[0-9a-zA-Z/?]+SFN=)")
        found = re.findall(pattern, path)
        if len(found) > 0:
            return path.replace(found[0], '')
        else:
            return path

    stripPortAndVersion = staticmethod(stripPortAndVersion)

    def stripProtocolServer(fullpath):
        """ strip protocol and server info """
        # srm://srm-atlas.gridpp.rl.ac.uk:8443/srm/managerv2/something/somethingelse/mcdisk/
        # -> srm://srm-atlas.gridpp.rl.ac.uk/something/somethingelse/mcdisk/
        # -> /something/somethingelse/mcdisk/

        # remove any port and version if present
        fullpath = SiteMover.stripPortAndVersion(fullpath)

        # extract the protocol and server info
        strippedpath = SiteMover.stripPath(fullpath)

        # and finally remove the protocol and server info
        return fullpath.replace(strippedpath, "")

    stripProtocolServer = staticmethod(stripProtocolServer)

    def stripPath(fullpath):
        """ strip path """
        # srm://srm-atlas.gridpp.rl.ac.uk:8443/srm/managerv2/something/somethingelse/mcdisk/
        # -> srm://srm-atlas.gridpp.rl.ac.uk:8443

        path = ''

        # remove ddm info if present
        if fullpath.find("^"):
            fullpath = fullpath.split("^")[0]

        try:
            fullpath = fullpath.replace("://","|")
            pathList = fullpath.split("/")
            path = pathList[0]
            path = path.replace("|","://")
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not strip path from: %s, %s" % (fullpath, e))
            path = fullpath
            if path == '':
                tolog("!!WARNING!!2999!! Nothing to strip from: %s" % (fullpath))
                path = fullpath

        return path

    stripPath = staticmethod(stripPath)

    def getFileType(filename, setup=None):
        """ use the file command to get the file type """

        status = "unknown"
        if setup:
            _setup = setup
        else:
            _setup = ""

        try:
            cmd = "%s file -b %s" % (_setup, filename)
            tolog("Executing command: %s" % (cmd))
            rs = commands.getoutput(cmd)
        except Exception, e:
            tolog("!!WARNING!!3000!! Could not run file command: %s" % str(e))
        else:
            tolog("Command returned: %s" % (rs))
            if rs != "":
                status = rs

        return status
    getFileType = staticmethod(getFileType)

    def getTransferModes():
        """ should remote I/O and file stager be used? """
        # return the corresponding booleans

        directIn = False
        useFileStager = False
        dInfo = getDirectAccessDic(readpar('copysetupin'))
        # if copysetupin did not contain direct access info, try the copysetup instead
        if not dInfo:
            dInfo = getDirectAccessDic(readpar('copysetup'))

        # check if we should use the copytool
        if dInfo:
            directIn = dInfo['directIn']
            useFileStager = dInfo['useFileStager']

        return directIn, useFileStager
    getTransferModes = staticmethod(getTransferModes)

    def isRootFileName(filename):
        """ Determine whether file is a root file or not by its name """

        filename = filename.lower()
        return not ('.tar.gz' in filename or '.lib.tgz' in filename or '.raw.' in filename)

    isRootFileName = staticmethod(isRootFileName)

    def isRootFile(filename, setup=None, directAccess=False):
        """ check whether filename is a root file or not """

        # always a root file in direct access mode
        if directAccess:
            return True

        status = False
        ftype = SiteMover.getFileType(filename, setup=setup)
        try:
            if ftype[:4].upper() == "ROOT":
                status = True
        except:
            pass

        return status

    isRootFile = staticmethod(isRootFile)

    def getDirList(_dirlist):
        """ return a proper dir list from dirlist string """
        # _dirlist = "/atlas/[atlasdatadisk,atlasdatadisktape,atlasmcdisk]/"
        # -> ['/atlas/atlasdatadisk, '/atlas/atlasdatadisktape', '/atlas/atlasmcdisk']
        # _dirlist = "/atlas/[atlasdatadisk,atlasdatadisktape,atlasmcdisk]/rucio"
        # -> ['/atlas/atlasdatadisk/rucio', '/atlas/atlasdatadisktape/rucio', '/atlas/atlasmcdisk/rucio']
        # _dirlist = "/atlas/atlasdatadisk,/atlas/atlasmcdisk/rucio"
        # -> ['/atlas/atlasdatadisk', '/atlas/atlasmcdisk/rucio']
        # _dirlist = "/[atlasscratchdisk/rucio,atlaslocalgroupdisk/rucio]"
        # -> ['/atlasscratchdisk/rucio', '/atlaslocalgroupdisk/rucio']

        # also works with lists of the following form
        #_dirlist = "/atlas/[atlasdatadisk]"
        #_dirlist = "/atlas/atlasdatadisk"
        #_dirlist = "/atlas/whatever/atlasdatadisk"
        #_dirlist = "/atlas/atlasdatadisk,/atlas/atlasmcdisk"
        #_dirlist = "" (-> [''])

        dirlist = []
        if _dirlist.find('[') > 0 and _dirlist.find(']') > 0:
            # path should have the form /path/[spacetokenlowered1, ...]
            pat = re.compile('(.*)\[(.*)\]')
            mat = pat.match( _dirlist)
            if mat:
                # mat.group(1) = '/atlas/'
                # mat.group(2) = 'atlasdatadisk,atlasdatadisktape,atlasmcdisk'
                _dir = mat.group(1)
                _subdirs = mat.group(2)
                if not _dir == "/":
                    _remainder = _dirlist.replace(mat.group(1), '').replace('['+mat.group(2)+']', '')
                else:
                    _remainder = ""
                if _remainder == "/":
                    _remainder = ""
                for d in _subdirs.split(","):
                    dirlist.append(os.path.join(_dir, d+_remainder))
            else:
                # no match for pattern
                pass
        else:
            # path should have the form /path/spacetokenlowered or /path/st1,/path/st2.,...
            dirlist = _dirlist.split(",")

        return dirlist

    getDirList = staticmethod(getDirList)

    def getMatchingDestinationPath(spacetoken, destinationList, alt=False):
        """ select the destination path that corresponds to the space token """

        destination = ""
        _setokens = readpar('setokens', alt=alt)
        if _setokens == "":
            setokens = []
        else:
            setokens = _setokens.split(",")
        if len(setokens) != len(destinationList):
            tolog("WARNING: setokens (%s) not of the same length as destinationList (%s) - switching to alternative algorithm (match space token)" %\
                  (str(setokens), str(destinationList)))
            # alternative default algorithm: get the path corresponding to the space token in lower case
            for prodpath in destinationList:
                if prodpath.find(spacetoken.lower()) >= 0:
                    tolog("Found matching destination path for space token %s: %s" % (prodpath, spacetoken))
                    destination = prodpath
                    break
        else:
            destDict = dict(zip(setokens, destinationList))

            # get the path corresponding to the relevant spacetoken
            try:
                destination = destDict[spacetoken]
            except Exception, e:
                tolog("!!WARNING!!2999!! Path for spacetoken %s not found in dictionary %s: %s" % (spacetoken, str(destDict), str(e)))
            else:
                tolog("Space token %s corresponds to path %s" % (spacetoken, destination))

        return destination

    getMatchingDestinationPath = staticmethod(getMatchingDestinationPath)

    def getPort(se):
        """ does the se contain a port number? """
        # se = 'srm://atlas.bu.edu:8443/srm/v2/server?SFN='
        # => port = 8443

        port = None
        pattern = re.compile(r'\S+\:([0-9]+)')
        _port = re.findall(pattern, se)
        if _port != [] and _port != ['']:
            port = _port[0]
        return port

    getPort = staticmethod(getPort)

    def stripPortFromSE(se):
        """ remove the port number from se """

        port = SiteMover.getPort(se)
        return se.replace(":%s" % str(port), "")

    stripPortFromSE = staticmethod(stripPortFromSE)

    def addPortToPath(se, gpfn):
        """ add the port number to the gpfn if not already there """
        # se = 'srm://atlas.bu.edu:8443/srm/v2/server?SFN='
        # => port = 8443
        # gpfn = 'srm://atlas.bu.edu/srm/v2/server?SFN=/whatever/DBRelease-6.0.1.tar.gz'
        # => 'srm://atlas.bu.edu:8443/srm/v2/server?SFN=/whatever/DBRelease-6.0.1.tar.gz'

        # remove any ddm info attached to se
        if se.find("^"):
            se = se.split("^")[0]
            tolog("Removed ddm info from se: %s" % (se))

        # don't do anything if gpfn already has a port number
        port = SiteMover.getPort(gpfn)
        if not port:
            strippedSE = SiteMover.stripPortFromSE(se)
            if gpfn[:len(strippedSE)] == strippedSE and gpfn[:len(se)] != se:
                tolog("Updating gpfn with port number from se")
                tolog("Old gpfn: %s" % (gpfn))
                gpfn = gpfn.replace(gpfn[:len(strippedSE)], se)
                tolog("New gpfn: %s" % (gpfn))
            else:
                tolog("!!WARNING!!2999!! gpfn surl does not match se surl: se=%s, gpfn=%s" % (se, gpfn))
        else:
            tolog("gpfn already has a port number, will not update it: %s" % (gpfn))
        return gpfn

    addPortToPath = staticmethod(addPortToPath)

    def genSubpath(dsname, filename, logFile):
        """ Generate BNL specific subpath (used by BNLdCacheSiteMover and SRMSiteMover) """

        fields = dsname.split('.')
        subpath = ''
        os.environ['TZ'] = 'US/Eastern'
        year = time.strftime("%Y")
        month = time.strftime("%m")
        week = time.strftime("%W")
        day = time.strftime("%j")
        try:
            if ( re.search('^user[0-9]{0,2}$', fields[0]) ):
                if filename == logFile:
                    subpath = 'user_log02/%s/%s/%s' % (fields[1], year, week)
                else:
                    subpath = 'user_data02/%s/%s/%s' % (fields[1], year, week)
            elif ( SiteMover.isCond(fields) ):
                subpath = 'conditions01'
            elif ( SiteMover.isProd(fields) ):
                if ( fields[4] == 'RDO' ):
                    subpath = 'RDO02/%s/prod' % (fields[0])
                elif ( fields[4] == 'log' ):
                    subpath = 'log02/%s_%s/%s/prod' % (year, month, fields[0])
                else:
                    subpath = '%s01/%s/prod' % (fields[4], fields[0])
            elif ( fields[0] == 'testpanda' ):
                subpath = 'user_log02/testpanda/%s/%s/%s' % (year, week, day)
            else:
                subpath = 'others03/%s/%s' % (year, week)
        except Exception, e:
            tolog('!!WARNING!!2999!! Error in generating the subpath for %s, using %s: %s' % (dsname, subpath, str(e)))
        return subpath

    genSubpath = staticmethod(genSubpath)

    def isCond(fields):
        if (fields[0] not in SiteMover.CONDPROJ):
            return False
        m = re.search('^[0-9]{6}$', fields[1])
        if m == None:
            return False
        if (fields[2] != 'conditions'):
            return False
        return True
    isCond = staticmethod(isCond)

    def isProd(fields):
        m = re.search('^user', fields[0])
        if m:
            return False
        m = re.search('^[0-9]{6}$', fields[1])
        if m == None:
            return False
        if (fields[4] not in SiteMover.PRODFTYPE):
            return False
        return True
    isProd = staticmethod(isProd)

    def getLFCDir(analyJob, destination, dsname, lfn):
        """ setup the lfc path """

        lfcpath, pilotErrorDiag = SiteMover.getLFCPath(analyJob)
        if lfcpath == "":
            return lfcpath, pilotErrorDiag

        tolog("Got lfcpath: %s" % (lfcpath))

        # set up paths differently for analysis and production jobs
        # use conventional LFC paths or production jobs
        # use OSG style for analysis jobs (for the time being)
        if not analyJob:
            # return full lfc file path (beginning lfcpath might need to be replaced)
            native_lfc_path = SiteMover.to_native_lfn(dsname, lfn)
            # /grid/atlas/dq2/testpanda/testpanda.destDB.b7cd4b56-1b5e-465a-a5d7-38d5e2609724_sub01000457/
            #58f836d5-ff4b-441a-979b-c37094257b72_0.job.log.tgz
            tolog("Native_lfc_path: %s" % (native_lfc_path))

            # replace the default path /grid/atlas/dq2 with lfcpath if different
            # (to_native_lfn returns a path begining with /grid/atlas/dq2)
            default_lfcpath = '/grid/atlas/dq2' # to_native_lfn always returns this at the beginning of the string
            if default_lfcpath != lfcpath:
                final_lfc_path = native_lfc_path.replace(default_lfcpath, lfcpath)
            else:
                final_lfc_path = native_lfc_path

            stripped_lfcpath = os.path.dirname(native_lfc_path[len(default_lfcpath):]) # the rest (to be added to the 'destination' variable)
            # /testpanda/testpanda.destDB.b7cd4b56-1b5e-465a-a5d7-38d5e2609724_sub01000457/58f836d5-ff4b-441a-979b-c37094257b72_0.job.log.tgz
            # tolog("stripped_lfcpath: %s" % (stripped_lfcpath))

            # full file path for disk
            if stripped_lfcpath[0] == "/":
                stripped_lfcpath = stripped_lfcpath[1:]
            destination = os.path.join(destination, stripped_lfcpath)
            # /pnfs/tier2.hep.manchester.ac.uk/data/atlas/dq2/testpanda/testpanda.destDB.fcaf8da5-ffb6-4a63-9963-f31e768b82ef_sub01000345
            # tolog("Updated SE destination: %s" % (destination))

            # name of dir to be created in LFC
            lfcdir = os.path.dirname(final_lfc_path)
            # /grid/atlas/dq2/testpanda/testpanda.destDB.dfb45803-1251-43bb-8e7a-6ad2b6f205be_sub01000492
            # LFC LFN = /grid/atlas/dq2/testpanda/testpanda.destDB.dfb45803-1251-43bb-8e7a-6ad2b6f205be_sub01000492/
            #364aeb74-8b62-4c8f-af43-47b447192ced_0.job.log.tgz
            # lfclfn = '%s/%s' % ( lfcdir, lfn )

        else: # for analysis jobs

            ec, pilotErrorDiag, destination, lfcdir = SiteMover.getUserLFCDir(destination, lfcpath, dsname)
            if ec != 0:
                return lfcdir, pilotErrorDiag

#            pat = re.compile('([^\.]+\.[^\.]+)\..*')
#            mat = pat.match(dsname)
#            if mat:
#                prefixdir = mat.group(1)
#                destination = os.path.join(destination, prefixdir)
#            else:
#                pilotErrorDiag = "getLFCDir encountered unexpected dataset name format: %s" % (dsname)
#                tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
#                return lfcdir, pilotErrorDiag
#            tolog("SE destination: %s" % (destination))
#
#            lfcdir = '%s/%s/%s' % ( lfcpath, prefixdir, dsname )


        return lfcdir, pilotErrorDiag

    getLFCDir = staticmethod(getLFCDir)

    def lfc_mkdir(lfcdir, fake=False):
        """ create the lfc dir """

        error = PilotErrors()
        pilotErrorDiag = ""
        ec = 0

        cmd = 'echo "LFC_HOST=$LFC_HOST"; lfc-mkdir -m 0775 -p %s' % (lfcdir)
        tolog("Executing command: %s" % (cmd))
        try:
            if fake:
                raise Exception("fake exception in lfc-mkdir")
            _ec, telapsed, cout, cerr = timed_command(cmd, 600)
            tolog("_ec: %s" % _ec)
            tolog("telapsed: %s" % telapsed)
            tolog("cout: %s" % cout)
            tolog("cerr: %s" % cerr)
        except Exception, e:
            pilotErrorDiag = "lfc-mkdir threw an exception: %s" % str(e)
            tolog('!!FAILED!!2999!! %s' % (pilotErrorDiag))
            ec = error.ERR_STAGEOUTFAILED
        else:
            rs = cout + cerr
            if _ec != 0:
                if is_timeout(_ec):
                    pilotErrorDiag = "lfc-mkdir get was timed out after %d seconds" % (telapsed)
                    ec = error.ERR_PUTTIMEOUT
                elif rs == 'File exists':
                    tolog("Ignore existing lfc dir error")
                    ec = 0
                elif rs == "Could not establish context":
                    pilotErrorDiag = "Could not establish context: Proxy / VO extension of proxy has probably expired"
                    tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
                    ec = error.ERR_NOPROXY
                else:
                    pilotErrorDiag = "lfc-mkdir failed: %s" % (rs)
                    tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
                    ec = error.ERR_STAGEOUTFAILED
            else:
                tolog("Command output: %s" % (rs))

        return ec, pilotErrorDiag
    lfc_mkdir = staticmethod(lfc_mkdir)

    def lcg_rf(lfclfn, r_gpfn):
        """ register file in lfc using CLI """

        error = PilotErrors()
        pilotErrorDiag = ""
        ec = 0

        cmd = 'echo "LFC_HOST=$LFC_HOST"; lcg-rf --vo atlas -l lfn:%s %s' % (lfclfn, r_gpfn)
        tolog("Executing command: %s" % (cmd))
        try:
            _ec, rs = commands.getstatusoutput(cmd)
        except Exception, e:
            pilotErrorDiag = "lcg-rf threw an exception: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            ec = error.ERR_FAILEDLCGREG
        else:
            if _ec != 0:
                pilotErrorDiag = "lcg-rf failed: %s" % (rs)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                ec = error.ERR_FAILEDLCGREG
            else:
                tolog("Command output: %s" % (rs))
                tolog("LFC registration succeeded")

        return ec, pilotErrorDiag
    lcg_rf = staticmethod(lcg_rf)

    def bulkRegisterFiles(host, guid, scope, dsname, lfn, surl, checksum, fsize):
        """ Use Rucio method to register a file in the catalog """
        # NOTE: not actually a bulk method, files are currently registered one by one
        # Sites are expected to start using lfcregister=server so this should not be a problem

        ec = 0
        pilotErrorDiag = ""
        error = PilotErrors()

        # Create the files dictionary
        files = { guid: { 'dsn': dsname, 'lfn':"%s:%s" % (scope, lfn), 'surl':surl, 'fsize':long(fsize), 'checksum':checksum } }

        try:
            from dq2.filecatalog import create_file_catalog
            from dq2.filecatalog.FileCatalogException import FileCatalogException
            from dq2.filecatalog.FileCatalogUnavailable import FileCatalogUnavailable
        except:
            pilotErrorDiag = "Bad environment: Could not import dq2 modules needed for Rucio"
            tolog("!!WARNING!!3333!! %s" % (pilotErrorDiag))
            ec = error.ERR_FAILEDLFCREG # use LFC error code for now
        else:
            tolog("Using host=%s for file registration" % (host))

            try:
                catalog = create_file_catalog(host)
                catalog.connect()
                dictionaryReplicas = catalog.bulkRegisterFiles(files)
                catalog.disconnect()
            except:
                pilotErrorDiag = "Caught exception while trying to interact with catalog %s" % (host)
                tolog("!!WARNING!!3333!! %s" % (pilotErrorDiag))
                ec = error.ERR_FAILEDLFCREG # use LFC error code for now
            else:
                # Verify that the file registration worked
                if dictionaryReplicas != {}:
                    for guid in dictionaryReplicas.keys():
                        if dictionaryReplicas[guid]:
                            tolog("Confirmed file registration for guid=%s" % (guid))
                        else:
                            pilotErrorDiag = "Could not register guid=%s in file catalog=%s: %s" % (guid, host, str(dictionaryReplicas[guid]))
                            tolog("!!WARNING!!3333!! %s" % (pilotErrorDiag))
                            ec = error.ERR_FAILEDLFCREG # use LFC error code for now
                else:
                    pilotErrorDiag = "bulkRegisterFiles() returned empty dictionary"
                    tolog("!!WARNING!!3333!! %s" % (pilotErrorDiag))
                    ec = error.ERR_FAILEDLFCREG # use LFC error code for now

        return ec, pilotErrorDiag
    bulkRegisterFiles = staticmethod(bulkRegisterFiles)

#     where files is dictionary mapping guid to another dictionary with
#     lfn(scope:lfn), surl. lfn is still concatenated with scope.

    def registerFileInCatalog(host, analyJob, destination, scope, dsname, lfn, gpfn, guid, fchecksum, fsize):
        """ Register file in catalog """

        error = PilotErrors()
        pilotErrorDiag = ""
        ec = 0

        tolog("Preparing for file registration")

        # use rucio methods for all file registrations
        ec, pilotErrorDiag = SiteMover.bulkRegisterFiles(host, guid, scope, dsname, lfn, gpfn, fchecksum, fsize)
        return ec, pilotErrorDiag

        # Use Rucio method if /rucio/ is found in the SURL
#        if "/rucio/" in gpfn:
#            ec, pilotErrorDiag = SiteMover.bulkRegisterFiles(host, guid, scope, dsname, lfn, gpfn, fchecksum, fsize)
#            return ec, pilotErrorDiag

        # get the SE
        se = readpar('se').split(",")[0]
        _dummytoken, se = SiteMover.extractSE(se)

        # get the corresponding hostname
        hostname = SiteMover.extractHostname(se)
        if hostname == "":
            hostname = os.getenv("LFC_HOST")
        tolog("File registration using host: %s" % (hostname))

        try:
            import lfc
        except Exception, e:
            pilotErrorDiag = "registerFileInCatalog() could not import lfc module: %s (unrecoverable)" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_PUTLFCIMPORT, pilotErrorDiag

        # prepare for LFC registration
        lfcdir, pilotErrorDiag = SiteMover.getLFCDir(analyJob, destination, dsname, lfn)
        if lfcdir == "":
            pilotErrorDiag += " (unrecoverable)"
            return error.ERR_FAILEDLFCREG, pilotErrorDiag

        tolog("Got lfcdir: %s" % (lfcdir))
        lfclfn = '%s/%s' % (lfcdir, lfn)

        # create the LFC dir
        try:
            # to provoke a false LFC exception for testing
            if not ".log." in lfn:
                fake = True
            else:
                fake = False
            exitcode, pilotErrorDiag = SiteMover.lfc_mkdir(lfcdir, fake=False)
        except Exception, e:
            exitcode = 1
            pilotErrorDiag = "Caught exception in lfc_mkdir function: %s (unrecoverable)" % str(e)
            tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
        if exitcode != 0:
            return error.ERR_FAILEDLFCREG, pilotErrorDiag
        else:
            tolog("Created lfcdir: %s" % (lfcdir))

        # API version does not create directories recursively, which cause problems for analysis jobs
        #        exitcode = lfc.lfc_mkdir(lfcdir, 0775)

        # create the LFC entry
        tolog("lfc_creatg called with lfclfn: %s" % (lfclfn))
        try:
            exitcode = lfc.lfc_creatg(lfclfn, guid, 0774)
        except Exception, e:
            pilotErrorDiag = "lfc_creatg threw an exception: %s (unrecoverable)" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_FAILEDLFCREG, pilotErrorDiag
        else:
            if exitcode != 0:
                errno = lfc.cvar.serrno
                errstr = lfc.sstrerror(errno)
                pilotErrorDiag = "lfc_creatg failed with (%d, %s)" % (errno, errstr)
                tolog("!!WARNING!!2999!! %s (unrecoverable)" % (pilotErrorDiag))
                return error.ERR_FAILEDLFCREG, pilotErrorDiag
            else:
                tolog("Created LFC entry: (%s, %s, %d)" % (lfclfn, guid, 0774))

        # add the replica
        status = '-'
        f_type = 'D'
        tolog("lfc_addreplica called with gpfn: %s, status: %s, f_type: %s" % (gpfn, status, f_type))
        try:
            tolog("guid = %s (type=%s)" % (guid, type(guid)))
            tolog("hostname = %s (type=%s)" % (hostname, type(hostname)))
            tolog("gpfn = %s (type=%s)" % (gpfn, type(gpfn)))
            tolog("status = %s (type=%s)" % (status, type(status)))
            tolog("f_type = %s (type=%s)" % (f_type, type(f_type)))
            exitcode = lfc.lfc_addreplica(guid, None, hostname, gpfn, status, f_type, "", "")
        except Exception, e:
            pilotErrorDiag = "lfc_addreplica threw an exception: %s (unrecoverable)" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_FAILEDLFCREG, pilotErrorDiag
        else:
            if exitcode != 0:
                errno = lfc.cvar.serrno
                errstr = lfc.sstrerror(errno)
                pilotErrorDiag = "lfc_addreplica failed with (%d, %s) (unrecoverable)" % (errno, errstr)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDLFCREG, pilotErrorDiag
            else:
                tolog("Added LFC replica: %s" % (gpfn))

        # add checksum and file size to LFC
        csumtype = SiteMover.getChecksumType(fchecksum, format="short")
        exitcode, pilotErrorDiag = SiteMover.addFileInfo(lfclfn, fchecksum, csumtype=csumtype, fsize=fsize)
        if exitcode == -1:
            ec = error.ERR_PUTLFCIMPORT
            pilotErrorDiag += " (unrecoverable)"
        elif exitcode != 0:
            ec = error.ERR_LFCADDCSUMFAILED
            pilotErrorDiag += " (unrecoverable)"
        else:
            tolog('Successfully set filesize and checksum')

        return ec, pilotErrorDiag
    registerFileInCatalog = staticmethod(registerFileInCatalog)

    def getLFCPath(analyJob, alt=False):
        """ return the proper schedconfig lfcpath """

        lfcpath = ""
        pilotErrorDiag = ""

        if not analyJob:
            lfcpath = readpar('lfcprodpath', alt=alt)
            if lfcpath == "":
                lfcpath = readpar('lfcpath', alt=alt)
        else:
            lfcpath = readpar('lfcpath', alt=alt)
            if lfcpath == "":
                lfcpath = readpar('lfcprodpath', alt=alt)

        if lfcpath == "":
            pilotErrorDiag = "lfc[prod]path is not set"
            tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))

        return lfcpath, pilotErrorDiag
    getLFCPath = staticmethod(getLFCPath)

    def getModTime(path, filename):
        """ get the modification time of the file """

        t = ""
        _filename = os.path.join(path, filename)
        if os.path.exists(_filename):
            try:
                t = time.strftime("%Y-%m-%d %I:%M:%S",time.localtime(os.path.getmtime(_filename)))
            except Exception, e:
                tolog("!!WARNING!!1880!! Get mod time failed for file %s: %s" % (_filename, e))
                t = ""
        else:
            tolog("WARNING: Cannot get mod time of file since it does not exist: %s" % (_filename))

        if t != "":
            tolog("Mod time for file %s: %s" % (_filename, t))
        return t
    getModTime = staticmethod(getModTime)

    def getDestination(analyJob, token):
        """ get the destination path """

        # for production jobs, the SE path is stored in seprodpath
        # for analysis jobs, the SE path is stored in sepath
        destination = ""
        if not analyJob:
            # process the destination path with getDirList since it can have a complex
            # structure as well as be a list of destination paths matching a corresponding
            # space token
            spath = readpar('seprodpath')
            destinationList = SiteMover.getDirList(spath)

            # decide which destination path to use depending on the space token for the current file
            if token:
                # find the proper path
                destination = SiteMover.getMatchingDestinationPath(token, destinationList)
                if destination == "":
                    tolog("!!WARNING!!2990!! seprodpath could not be used: seprodpath = %s, destinationList = %s, using sepath instead" %\
                          (spath, str(destinationList)))
                    spath = readpar('sepath')
                    destinationList = SiteMover.getDirList(spath)
                    destination = SiteMover.getMatchingDestinationPath(token, destinationList)
                    if destination == "":
                        tolog("!!WARNING!!2990!! sepath could not be used: sepath = %s, destinationList = %s" %\
                              (spath, str(destinationList)))
            else:
                # space tokens are not used
                destination = destinationList[0]
        else:
            spath = readpar('sepath')
            # sepath could be empty in the case of install jobs, if so, try to use seprodpath instead
            if spath == "":
                spath = readpar('seprodpath')
                tolog("Encountered an empty sepath, trying to use seprodpath instead")
            destinationList = SiteMover.getDirList(spath)

            # decide which destination path to use depending on the space token for the current file
            if token:
                # find the proper path
                destination = SiteMover.getMatchingDestinationPath(token, destinationList)
                if destination == "":
                    tolog("!!WARNING!!2990!! sepath could not be used: sepath = %s, destinationList = %s" %\
                          (spath, str(destinationList)))
            else:
                # space tokens are not used
                destination = destinationList[0]

        return destination
    getDestination = staticmethod(getDestination)

    def addPaths(se_list):
        """ add seprodpaths to the se list entries """

        tolog("Adding seprodpath(s) to SE list")
        seprodpath = readpar('seprodpath')
        if seprodpath != "":
            new_se_list = []
            seprodpath_list = SiteMover.getDirList(seprodpath)
            for se in se_list:
                for path in seprodpath_list:
                    # strip any trailing slash
                    if se.endswith("/"):
                        se = se[:-1]
                    new_se_list.append(se + path)
            if new_se_list != []:
                se_list = new_se_list + se_list
        else:
            tolog("WARNING: SiteMover.addPaths(): seprodpath is not set")

        # make sure that any tape areas do not appear before the simple se info
        if se_list != "":
            se_list = SiteMover.pushbackTapeAreas(se_list)
            tolog("Updated SE list:")
            dumpOrderedItems(se_list)

        return se_list
    addPaths = staticmethod(addPaths)

    def pullMatchedAreas(se_list, area):
        """ Pull forward a given area in the se list """
        # e.g.
        # se_list = ['srm://whatever/pnfs/atlasproddisk','srm://whatever','srm://whatever/pnfs/atlasdatatape']
        # area = atlasdatatape
        # -> se_list = ['srm://whatever/pnfs/atlasdatatape','srm://whatever/pnfs/atlasproddisk']

        path_priority = []
        path_no_priority = []
        for path in se_list:
            if area in path:
                path_priority.append(path)
            else:
                path_no_priority.append(path)
        se_list = path_priority + path_no_priority
        return se_list
    pullMatchedAreas = staticmethod(pullMatchedAreas)

    def pushbackTapeAreas(se_list):
        """ Make sure that any tape areas do not appear before the simple se info """
        # e.g. se_list = ['srm://whatever/pnfs/atlasproddisk','srm://whatever/pnfs/atlasprodtape','srm://whatever']
        # -> se_list = ['srm://whatever/pnfs/atlasproddisk','srm://whatever','srm://whatever/pnfs/atlasprodtape']
        # test with:
        # from SiteMover import SiteMover
        # sm = SiteMover()
        # se_list=['srm://whatever/pnfs/atlasproddisk','srm://whatever/pnfs/atlasprodtape','srm://whatever']
        # sm.pushbackTapeAreas(se_list)
        # -> ['srm://whatever/pnfs/atlasproddisk', 'srm://whatever', 'srm://whatever/pnfs/atlasprodtape']

        path_disk = []
        path_tape = []
        for path in se_list:
            if 'tape' in path:
                path_tape.append(path)
            else:
                path_disk.append(path)
        se_list = path_disk + path_tape
        return se_list
    pushbackTapeAreas = staticmethod(pushbackTapeAreas)

    def getdCacheFileSize(directory, filename):
        """
        Special workaround to retrieve file size in SEs using dCache and PNFS that
        works when file size>=2GB. PNFS is NFSv3 compliant: supported file size<2GB.
        Suggested by Ofer, dCache expert at BNL:
        "cat %s/\".(use)(2)(%s)\" | grep \'l=\' | sed \'s/.*l=\(.*\);.*/\\1/\'" % (directory, filename)
        Fixed RE: "cat %s/\".(use)(2)(%s)\" | grep \'l=\' | sed \'s/.*l=\([^;]*\);.*/\\1/\'" % (directory, filename)
        The python version below has been reworked with Charles
        This should be no more necessary once Chimera is adopted.
        """
        
        for attempt in range(1,4):
            f = open("%s/.(use)(2)(%s)" % (directory, filename), 'r')
            data = f.readlines()
            f.close()
            tolog("data = %s" % str(data))
            filesize = None
            for line in data:
                if 'l=' in line:
                    for word in line.split(';'):
                        if word.startswith('l='):
                            filesize = word.split('=')[1]
            if filesize:
                break
            tolog("dCache size retrieval failed. Service (2) file (size: %s) content: %s\n sleeping %s sec." %\
                  (len(data), data, attempt*3))
            time.sleep(attempt*3)
        if not filesize:
            tolog('!!WARNING!!2999!! dCache size retrieval failed all attempts, check: %s/".(use)(2)(%s)"' %\
                  (directory, filename))
            raise OSError("Wrong dCache system file format")
        return filesize
    getdCacheFileSize = staticmethod(getdCacheFileSize)

    def getdCacheChecksum(dir, filename):
        """ Retrieve the remote checksum in SEs using dCache and PNFS """
        # Note: this function is used by both lcgcp2SiteMover and dCacheSiteMover
        # which is why it is not put in dCacheSiteMover even though it is specific to dCache

        fchecksum = None

        # abort check if the remote file system is not visible on the worker node
        if not SiteMover.isPNFSVisible(dir):
            return None

        for attempt in range(1,4):
            try:
                f = open("%s/.(use)(2)(%s)" % (dir, filename), 'r')
            except Exception, e:
                tolog("Warning: Exception caught in getdCacheChecksum(): %s, attempt %d" % (str(e), attempt))
                if "No such file or directory" in str(e):
                    return "NOSUCHFILE"
            else:
                data = f.readlines()
                # data = ['2,0,0,0.0,0.0\n', ':c=1:3ef569d9;h=yes;l=60170430;\n']
                f.close()
                tolog("data = %s" % str(data))
                for line in data:
                    if 'c=' in line:
                        for word in line.split(';'):
                            if word.startswith('c=') or word.startswith(':c='):
                                value = word.split('=')[1]
                                fchecksum = value.split(':')[1]
                if fchecksum:
                    break
                else:
                    tolog("dCache checksum retrieval failed (attempt %d)" % (attempt))
                    fchecksum = None
                    time.sleep(attempt*3)

        if fchecksum:
            # adler32 or md5sum
            if len(fchecksum) == 8 or len(fchecksum) == 32:
                pass # valid checksum
            else:
                tolog("!!WARNING!!2999!! Bad dCache checksum: %s" % (fchecksum))
                fchecksum = None

        return fchecksum
    getdCacheChecksum = staticmethod(getdCacheChecksum)

    def lcgGetChecksum(envsetup, timeout, surl):
        """ get checksum with lcg-get-checksum command """

        remote_checksum = None
        output = None

        # determine which timeout option to use
        if SiteMover.isNewLCGVersion("%s lcg-get-checksum" % (envsetup)):
            timeout_option = "--connect-timeout=300 --sendreceive-timeout=%d" % (timeout)
        else:
            timeout_option = "-t %d" % (timeout)

        cmd = "%s lcg-get-checksum -b -T srmv2 %s %s" % (envsetup, timeout_option, surl)
        tolog("Executing command: %s" % (cmd))
        try:
            ec, output = commands.getstatusoutput(cmd)
        except Exception, e:
            tolog("Warning: (Exception caught) lcg-get-checksum failed: %s" % (e))
            output = None
        else:
            if ec != 0 or "[ERROR]" in output:
                tolog("Warning: lcg-get-checksum failed: %d, %s" % (ec, output))
            else:
                tolog(output)
                try:
                    remote_checksum = output[:8]
                except:
                    tolog("!!WARNING!!1998!! Cannot extract checksum from output: %s" % (output))
                if not remote_checksum.isalnum():
                    tolog("!!WARNING!!1998!! Failed to extract alphanumeric checksum string from output: %s" % (output))
                    remote_checksum = None
#                import re
#                match = re.findall('([a-zA-Z0-9]+) ', output)
#                if match:
#                    if len(match[0]) == 8:
#                        remote_checksum = match[0]
#                    else:
#                        tolog("!!WARNING!!1998!! Remote checksum is not eight characters long: %s" % (match[0]))
#                else:
#                    tolog("!!WARNING!!1998!! No checksum match in lcg-get-checksum output")
        return remote_checksum
    lcgGetChecksum = staticmethod(lcgGetChecksum)

    def getRemoteFileInfo(envsetup, timeout, filename):
        """ extract checksum and file size from lcg-ls output """

        remote_checksum = None
        remote_fsize = None

        # get output from lcg-ls
        output = SiteMover.getLCGLS(envsetup, timeout, filename)

        # interpret lcg-ls output
        if output and output != "":
            remote_checksum = SiteMover.getRemoteChecksumLCGLS(output)
            remote_fsize = SiteMover.getRemoteFileSizeLCGLS(output)

        return remote_checksum, remote_fsize
    getRemoteFileInfo = staticmethod(getRemoteFileInfo)

    def getLCGLS(envsetup, timeout, filename):
        """ try to get the checksum with lcg-ls """

        output = None

        # determine which timeout option to use
        if SiteMover.isNewLCGVersion("%s lcg-ls" % (envsetup)):
            timeout_option = "--connect-timeout=300 --sendreceive-timeout=%d" % (timeout)
        else:
            timeout_option = "-t %d" % (timeout)

        # get the output
        cmd = '%s lcg-ls -l -b %s -T srmv2 %s' % (envsetup, timeout_option, filename)
        tolog("Executing command: %s" % (cmd))
        try:
            ec, output = commands.getstatusoutput(cmd)
        except Exception, e:
            tolog("Warning: (Exception caught) lcg-ls failed: %s" % str(e))
            output = None
        else:
            if ec != 0:
                tolog("Warning: lcg-ls failed: %d, %s" % (ec, output))
                output = None
            else:
                tolog(output)
        return output
    getLCGLS = staticmethod(getLCGLS)

    def getRemoteChecksumLCGLS(output):
        """ extract checksum from lcg-ls output """

        # interpret lcg-ls output
        ec, pilotErrorDiag, remote_checksum = SiteMover._getRemoteChecksumLCGLS(output)
        if ec == 0:
            pass
        elif ec == -1:
            # general error
            remote_checksum = ""
        elif ec == -2:
            # outdated lcg-ls or not supported checksum feature
            tolog("lcg-ls: cannot extract checksum from command output (not supported or command version too old)")
            remote_checksum = ""
        else:
            tolog("Warning: getRemoteChecksumLCGLS() failed")
            remote_checksum = ""

        return remote_checksum
    getRemoteChecksumLCGLS = staticmethod(getRemoteChecksumLCGLS)

    def getRemoteFileSizeLCGLS(output):
        """ extract file size from lcg-ls output """

        # interpret lcg-ls output
        ec, pilotErrorDiag, remote_fsize = SiteMover._getRemoteFileSizeLCGLS(output)
        if ec == 0:
            pass
        elif ec == -1:
            # general error
            remote_fsize = ""
        else:
            tolog("Warning: getRemoteFileSizeLCGLS() failed")
            remote_fsize = ""

        return remote_fsize
    getRemoteFileSizeLCGLS = staticmethod(getRemoteFileSizeLCGLS)

    def getRemoteFileSize(envsetup, timeout, filename):
        """ extract the remote file size using lcg-ls """
        # Exit: file size (string)

        remote_fsize = ""

        # determine which timeout option to use
        if SiteMover.isNewLCGVersion("%s lcg-ls" % (envsetup)):
            timeout_option = "--connect-timeout=300 --sendreceive-timeout=%d" % (timeout)
        else:
            timeout_option = "-t %d" % (timeout)

        # does the file exist?
        cmd = '%s lcg-ls -l -b %s -T srmv2 %s' % (envsetup, timeout_option, filename)
        ec, pilotErrorDiag, remote_fsize = SiteMover._getRemoteFileSize(cmd)
        if ec == 0:
            pass
        elif ec == -1:
            # general error
            remote_fsize = ""
        else:
            tolog("Warning: getRemoteFileSize() failed")
            remote_fsize = ""

        return remote_fsize
    getRemoteFileSize = staticmethod(getRemoteFileSize)

    def getEnvsetup(get=False, alt=False):
        """ return a proper envsetup """

        if get:
            envsetup = readpar('envsetupin', alt=alt)
            if envsetup == "":
                tolog("Using envsetup since envsetupin is not set")
                envsetup = readpar('envsetup', alt=alt)
        else:
            envsetup = readpar('envsetup', alt=alt)

        # get the user proxy if available
        envsetup = envsetup.strip()
        if envsetup != "" and not envsetup.endswith(';'):
            envsetup += ";"
        if os.environ.has_key('X509_USER_PROXY'):
            envsetup += "export X509_USER_PROXY=%s;" % (os.environ['X509_USER_PROXY'])

        return envsetup.replace(";;",";")
    getEnvsetup = staticmethod(getEnvsetup)

    def _getRemoteChecksumLCGLS(output):
        """ extract the remote checksum from lcg-ls output """
        # Exit: file size (string)

        # example command plus output
        # lcg-ls -l -b -T srmv2 srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv2?SFN=/pnfs/usatlas.bnl.gov/BNLT0D1/data10_7TeV/
        #  NTUP_TRKVALID/RAW_r1239/data10_7TeV.00152878.physics_MinBias.merge.NTUP_TRKVALID.RAW_r1239_tid134218_00/
        #  NTUP_TRKVALID.134218._001789.root.2
        # -rw-r--r--   1     2     2 241781863               ONLINE /pnfs/usatlas.bnl.gov/BNLT0D1/data10_7TeV/NTUP_TRKVALID/
        #  RAW_r1239/data10_7TeV.00152878.physics_MinBias.merge.NTUP_TRKVALID.RAW_r1239_tid134218_00/NTUP_TRKVALID.134218._001789.root.2
        #       * Checksum: 89526bb2 (adler32)
        #       * Space tokens: 10000

        remote_checksum = ""
        pilotErrorDiag = ""
        ec = 0

        # lcg-ls exits with error code 0 even if there was a problem
        if "CacheException" in output:
            pilotErrorDiag = "lcg-ls failed: %s"  % (output)
            tolog("!!WARNING!!2999!! %s (CacheException found)" % (pilotErrorDiag))
            ec = -1
        elif "is deprecated" in output:
            pilotErrorDiag = "Deprecated option(s) in lcg-ls command: %s" % (output)
            tolog("WARNING: %s" % (pilotErrorDiag))
            ec = -2
        else:
            # extract checksum * Checksum: 89526bb2 (adler32)
            pattern = re.compile(r"Checksum\:\ ([0-9a-zA-Z\-]+)")
            found = re.findall(pattern, output)
            if len(found) == 0:
                tolog("Checksum pattern not found in output (skip this checksum extraction method)")
                ec = -2
            else:
                # did we get a valid checksum?
                try:
                    remote_checksum = found[0]
                    # adler32
                    if len(remote_checksum) == 8:
                        if remote_checksum == "0"*8:
                            tolog("Encountered dummy checksum: %s" % (remote_checksum))
                            remote_checksum = ""
                            ec = -2
                        elif remote_checksum.isalnum():
                            tolog("Remote adler32 checksum has correct length and is alphanumeric")
                        else:
                            tolog("!!WARNING!!2999!! Adler32 checksum is not alphanumeric")
                            remote_checksum = ""
                            ec = -1
                    # md5sum
                    elif len(remote_checksum) == 32:
                        if remote_checksum == "0"*32:
                            tolog("Encountered dummy checksum: %s" % (remote_checksum))
                            remote_checksum = ""
                            ec = -2
                        else:
                            tolog("Remote md5 checksum has correct length")
                    # unknown
                    else:
                        tolog("!!WARNING!!2999!! Remote checksum does not have correct length: %d, %s (must have length 8 or 32)" %\
                              (len(remote_checksum), remote_checksum))
                        remote_checksum = ""
                        ec = -1
                except Exception, e:
                    pilotErrorDiag = "Checksum is not valid: %s (reset to empty string): %s" % (remote_checksum, str(e))
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    remote_checksum = ""
                    ec = -1

        return ec, pilotErrorDiag, remote_checksum
    _getRemoteChecksumLCGLS = staticmethod(_getRemoteChecksumLCGLS)

    def _getRemoteFileSizeLCGLS(output):
        """ extract the remote file size from lcg-ls output """
        # Exit: file size (string)

        # usage: lcg-ls [-l] [-d] [-D,--defaultsetype se|srmv1|srmv2] [-T,--setype se|srmv1|srmv2]
        # lcg-ls -l -b -t 18000 -T srmv2 srm://iut2-dc1.iu.edu:8443/srm/managerv2?SFN=/pnfs/iu.edu/atlasproddisk/testpanda/
        #  testpanda.destDB.8b34ab22-a320-4822-ab7d-863db674b565_sub05026945/312ea784-bb8a-4465-945e-5894eea18ba5_0.evgen.pool.root
        # -rw-r--r--  1     2     2 80601940               ONLINE /pnfs/iu.edu/atlasproddisk/testpanda/testpanda.destDB.8b34ab22 ...

        remote_fsize = ""
        pilotErrorDiag = ""
        ec = 0

        # lcg-ls exits with error code 0 even if there was a problem
        if "CacheException" in output:
            pilotErrorDiag = "lcg-ls failed: %s"  % (output)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            ec = -1
        elif "is deprecated" in output:
            pilotErrorDiag = "Deprecated option(s) in lcg-ls command: %s" % (output)
            tolog("WARNING: %s" % (pilotErrorDiag))
            ec = -2
        else:
            # extract file size
            try:
                # remove extra spaces
                while "  " in output:
                    output = output.replace("  ", " ")
                _output = output.split(" ")
                remote_fsize = _output[4]
            except Exception, e:
                pilotErrorDiag = "_getRemoteFileSizeLCGLS caught an exception: %s" % str(e)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                ec = -1
            else:
                # did we get an integer?
                try:
                    _dummy = int(remote_fsize)
                except:
                    pilotErrorDiag = "File size is not an integer: %s (reset to empty string)" % (remote_fsize)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    remote_fsize = ""
                    ec = -1

        return ec, pilotErrorDiag, remote_fsize
    _getRemoteFileSizeLCGLS = staticmethod(_getRemoteFileSizeLCGLS)

    def _getRemoteFileSize(cmd):
        """ extract the remote file size using lcg-ls """
        # Exit: file size (string)

        # usage: lcg-ls [-l] [-d] [-D,--defaultsetype se|srmv1|srmv2] [-T,--setype se|srmv1|srmv2]
        # lcg-ls -l -b -t 18000 -T srmv2 srm://iut2-dc1.iu.edu:8443/srm/managerv2?SFN=/pnfs/iu.edu/atlasproddisk/testpanda/
        #  testpanda.destDB.8b34ab22-a320-4822-ab7d-863db674b565_sub05026945/312ea784-bb8a-4465-945e-5894eea18ba5_0.evgen.pool.root
        # -rw-r--r--  1     2     2 80601940               ONLINE /pnfs/iu.edu/atlasproddisk/testpanda/testpanda.destDB.8b34ab22 ...

        remote_fsize = ""
        pilotErrorDiag = ""
        ec = 0

        # does the file exist?
        tolog("Executing command: %s" % (cmd))
        try:
            _ec, rs = commands.getstatusoutput(cmd)
        except Exception, e:
            pilotErrorDiag = "lcg-ls failed: %s" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            ec = -1
        else:
            if _ec != 0:
                pilotErrorDiag = "lcg-ls failed: %d, %s" % (_ec, rs)
                tolog("Warning: %s" % (pilotErrorDiag))
                ec = -1
            else:
                # lcg-ls exits with error code 0 even if there was a problem
                if "CacheException" in rs:
                    pilotErrorDiag = "lcg-ls failed: %s"  % (rs)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    ec = -1
                elif "is deprecated" in rs:
                    pilotErrorDiag = "Deprecated option(s) in lcg-ls command: %s" % (rs)
                    tolog("WARNING: %s" % (pilotErrorDiag))
                    ec = -2
                else:
                    tolog(rs)

                    # extract file size
                    try:
                        # remove extra spaces
                        while "  " in rs:
                            rs = rs.replace("  ", " ")
                        _rs = rs.split(" ")
                        remote_fsize = _rs[4]
                    except Exception, e:
                        pilotErrorDiag = "_getRemoteFileSize caught an exception: %s" % str(e)
                        tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                        ec = -1
                    else:
                        # did we get an integer?
                        try:
                            _dummy = int(remote_fsize)
                        except:
                            pilotErrorDiag = "File size is not an integer: %s (reset to empty string)" % (remote_fsize)
                            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                            remote_fsize = ""
                            ec = -1

        return ec, pilotErrorDiag, remote_fsize
    _getRemoteFileSize = staticmethod(_getRemoteFileSize)

    def removeLocal(filename):
        """ Remove the local file in case of failure to prevent problem with get retry attempt """

        status = False

        if os.path.exists(filename):
            try:
                os.remove(filename)
            except OSError:
                tolog("Could not remove the local file: %s" % (filename))
            else:
                status = True
                tolog("Removed local file: %s" % (filename))
        else:
            tolog("Nothing to remove (file %s does not exist)" % (filename))

        return status
    removeLocal = staticmethod(removeLocal)

    def removeFile(envsetup, timeout, filename, nolfc=False, lfcdir=None):
        """ Remove file from SE and unregister from LFC (if necessary) """
        # Occationally when lcg-cp fails during transfer it does not remove the touched or partially transferred file
        # in those cases the pilot will be in charge of removing the file
        # Exit: returns True if the method managed to remove the file
        # if nolfc=True, the file will not need to be removed from the LFC

        ec = -1

        # take a 1 m nap before trying to reach the file (it might not be available immediately after a transfer)
        tolog("Taking a 1 m nap before the file removal attempt")
        time.sleep(60)

        # get the remote file size (i.e. verify that the file exists)
        try:
            remote_fsize = SiteMover.getRemoteFileSize(envsetup, timeout, filename)
        except Exception, e:
            tolog('Warning: Could not get remote file size: %s (test will be skipped)' % str(e))
            remote_fsize = None

        if remote_fsize and remote_fsize != "":
            tolog("Remote file exists (has file size: %s)" % (remote_fsize))
        else:
            tolog("Remote file does not exist (attempting removal from LFC at least)")
            if nolfc:
                # change this flag since there might be a registered LFC entry
                tolog("Will not use --nolfc with lcg-del")
                nolfc = False

        # try to remove the file
        if nolfc:
            extra = "--nolfc"
        else:
            extra = ""
        cmd = '%s lcg-del --vo atlas --verbose -b -l -T srmv2 -t %d %s %s' % (envsetup, timeout, extra, filename)
        tolog("Executing command: %s" % (cmd))
        try:
            ec, rs = commands.getstatusoutput(cmd)
        except Exception, e:
            tolog("Warning: Exception caught in removeFile: %s" % (e))
        else:
            tolog(rs)
            if ec == 0:
                tolog("Successfully removed file: %s" % (filename))
            else:
                tolog("Could not remove file: ec = %d" % (ec))
                ec = -2 # code for 'do not retry stage-out'

        # remove the created LFC file and dir
#        if lfcdir:
#            # first remove the LFC file
#            SiteMover.lfcrm(envsetup, os.path.join(lfcdir, os.path.basename(filename)))
#            # then remove the LFC dir
#            SiteMover.lfcrm(envsetup, lfcdir)

        return ec
    removeFile = staticmethod(removeFile)

    def lfcrm(envsetup, fname):
        """ Remove a file or directory in LFC with lfc-rm """

        cmd = "%s lfc-rm -rf %s" % (envsetup, fname)
        tolog("Executing command: %s" % (cmd))
        try:
            ec, rs = commands.getstatusoutput(cmd)
        except Exception, e:
            tolog("Warning: Exception caught in removeFile: %s" % (e))
        else:
            if ec == 0:
                tolog("Removed LFC entry: %s" % (fname))
            else:
                tolog("Could not remove LFC entry: %d, %s (ignore)" % (ec, rs))
    lfcrm = staticmethod(lfcrm)

    def isPNFSVisible(dir):
        """ Is /pnfs/subdir visible on the worker node? """
        #>>> dir='/pnfs/aglt2.org/atlasproddisk/testpanda/testpanda.Nebraska-Lincoln-red_Install_7f103b6a-93b9-4ae1-'
        #>>> a=dir.split('/')
        #>>> os.path.join('/',a[1],a[2])
        #'/pnfs/aglt2.org'

        status = False
        subdir = dir.split('/')
        path = os.path.join('/', subdir[1], subdir[2])
        tolog('subdir: %s' % str(subdir))
        tolog('path: %s' % path)
        tolog('dir: %s' % dir)
        if subdir[1] != 'pnfs':
            tolog("/pnfs is not visible")
        else:
            try:
                dummy = os.listdir(path)
            except:
                tolog("%s is not visible" % (path))
            else:
                tolog("%s is visible: %s" % (path, dummy))
                status = True

        return status
    isPNFSVisible = staticmethod(isPNFSVisible)

    def isNewLCGVersion(cmd):
        """ return True if LCG command is newer than version ... """
        # New versions of the lcg-cr/cp commands deprecates -t option
        # --timeout-* will be used instead

        status = True

        lcg_help = commands.getoutput("%s -h" % (cmd))
        tolog("%s help: %s" % (cmd, lcg_help))
        if not "--connect-timeout" in lcg_help:
            status = False

        return status
    isNewLCGVersion = staticmethod(isNewLCGVersion)

    def getTier3Path(dsname, DN):
        """ Create a simple path for Tier 3 files """
        # e.g. 2010/TorreWenaus/FullDatasetName/Filename

        # get the base path
        path = readpar("se")
        if path == "":
            return None

        # get current year
        year = time.strftime("%Y")

        # extract user name from DN
        username = SiteMover.extractUsername(DN)
        if username != "":
            # create final SE path
            return os.path.join(path, year, username, dsname)
        else:
            return None

    getTier3Path = staticmethod(getTier3Path)

    def extractUsername(DN):
        """ Extract the user name without whitespaces from the DN """

        try:
            pattern = re.compile(r"./CN=([A-Za-z0-9\.\s]+).")
            txt = re.findall(pattern, DN)[0]

            # remove the numbers and spaces
            pattern = re.compile(r".([0-9]+).")
            numbers = re.findall(pattern, txt)[0]
            username = txt[:txt.find(numbers)]
        except Exception, e:
            tolog("!!WARNING!!2999!! Exception caught in extractUsername(): %s" % str(e))
            username = ""
        else:
            username = username.replace(' ', '')
            username = username.replace('.', '')
            tolog("Will use username %s for path creation" % (username))
        return username
    extractUsername = staticmethod(extractUsername)

    def removeSubFromDatasetName(dsname):
        """ Tmp function """

        pattern = re.compile('\S+(\_sub[0-9]+)')
        match = pattern.match(dsname)
        if match:
            # strip away the _subNNN string
            try:
                dsname = dsname.replace(match.group(1), '')
            except Exception, e:
                tolog("!!WARNING!!1119!! Failed to remove _sub string (%s) from dataset name: %s" % (match.group(1), e))
            else:
                tolog("Updated dataset name (removed %s): %s" % (match.group(1), dsname))
        else:
            tolog("Found no _subNNN string in the dataset name")

        return dsname
    removeSubFromDatasetName = staticmethod(removeSubFromDatasetName)

    def isOneByOneFileTransfer(self):
        """ Should files be transferred one by one or all at once? """
        # override this method in the relevant site mover (e.g. aria2cSiteMover)

        return True

    def checkForDirectAccess(self, lfn, useCT, workDir, jobId, prodDBlockToken):
        """ Should the file be transferred or read directly [later] by athena? """

        # get the transfer modes (direct access, file stager)
        directIn, useFileStager = self.getTransferModes()
        if directIn:

            from FileStateClient import updateFileState

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
                    if useFileStager:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="file_stager", type="input")
                    else:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="remote_io", type="input")
                else:
                    tolog("Normal file transfer")
        else:
            tolog("not directIn")

        return directIn

    def getPathFromScope(self, scope, lfn):
        """ Construct a partial PFN using the scope and the LFN """

        # /<site_prefix>/<space_token>/rucio/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>

        try:
            # for python 2.6
            import hashlib
            hash = hashlib.md5()
        except:
            # for python 2.4
            import md5
            hash = md5.new()

        correctedscope = "/".join(scope.split('.'))
        hash.update('%s:%s' % (scope, lfn))
        hash_hex = hash.hexdigest()[:6]
        return  'rucio/%s/%s/%s/%s' % (correctedscope, hash_hex[0:2], hash_hex[2:4], lfn)

    def getFullPath(self, scope, spacetoken, lfn, analyJob, prodSourceLabel, alt=False):
        """ Construct a full PFN using site prefix, space token, scope and LFN """

        # <protocol>://<hostname>:<port>/<protocol_prefix>/ + <site_prefix>/<space_token>/rucio/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>

        full_path = ""

        # Get the SE info
        se = readpar('se', alt=alt).split(",")[0]
        _spacetoken, se = SiteMover.extractSE(se)
        tolog("Extracted spacetoken=%s, se=%s" % (_spacetoken, se))

        # remove any unwanted stage-in info (present at CERN for atlasdatatape)
        se = SiteMover.filterSE(se)
        tolog("Full path will use SE: %s" % (se))

        # Use default space token from se field unless specified
        if spacetoken == "" or spacetoken == "NULL":
            # get the default space token from se
            spacetoken = _spacetoken
            tolog("Full path will use default space token descriptor: %s" % (spacetoken))
        else:
            tolog("Full path will use space token descriptor: %s" % (spacetoken))

        # Get the SE path from se[prod]path
        # E.g. /dpm/grid.sinica.edu.tw/home/atlas/atlasscratchdisk/
        destination = self.getPreDestination(analyJob, spacetoken, prodSourceLabel, alt=alt)
        tolog("Full path will use source/destination: %s" % (destination))

        # if the source/destination already has a trailing /rucio, remove it since it will be added by the scope path below
        if destination.endswith("/rucio"):
            destination = destination[:-len("/rucio")] # cut away the trailing /rucio bit

        # Get the path from the scope and LFN
        scope_path = self.getPathFromScope(scope, lfn)
        tolog("Full path will use path from scope: %s" % (scope_path))

        # Construct final full path
        full_path = se + destination
        full_path = os.path.join(full_path, scope_path)

        return full_path

    def getGlobalFilePaths(self, surl, dataset, computingSite, sourceSite):
        """ Get the global file paths """

        # Note: this method depends on the site mover used, so should be defined there, and as virtual here
        # (see e.g. FAXSiteMover, aria2cSiteMover for implementations)

        return []

    def getTimeOut(self, filesize):
        """ Get a proper time-out limit based on the file size """

        # timeout_default = 3600
        timeout_max = 6*3600
        timeout_min = 5*60

        # timeout = min + k*size
        timeout = timeout_min + int(filesize/400000.0)
        if timeout > timeout_max:
            timeout = timeout_max

        return timeout
