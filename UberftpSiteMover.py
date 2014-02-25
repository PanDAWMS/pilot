import os
import commands

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog
from config import config_sm
from timed_command import timed_command

PERMISSIONS_DIR = config_sm.PERMISSIONS_DIR
PERMISSIONS_FILE = config_sm.PERMISSIONS_FILE

class UberftpSiteMover(SiteMover.SiteMover):
    """
    SiteMover for SEs remotely accessible using uberftp from the WNs
    Provides the same interface as SiteMover

    uberftp, client provided from VDT, allows to interact with a gsiftp server
    and perform actions like creation of directories and verification of MD5
    checksum.

    The functions in this class parse the output of the list(ls,dir) command sent
    to the ftp server to retrieve the file size.
    Replies are server dependent (non standard).
    This class has been tested with the globus ftp server (returns a 'ls -l' output):
    "drwx------    2  usatlas1  usatlas    4096 May  2 18:38  gram_scratch_dS20YaiYVx"
    and the dCache server (shorter output):
    "-rw  413957423  mc12.005200.T1_McAtNlo_Jimmy.evgen.EVNT.v12000604_tid007939._00010.pool.root.1"
    """

    copyCommand = "uberftp"
    checksum_command = "md5sum"
    has_mkdir = True
    has_df = True
    has_getsize = True
    has_md5sum = True
    has_chmod = True

    def __init__(self, setup_path, *args, **kwrds):
        """ default init """
        self._setup = setup_path

    def _check_space(self, ub):
        """ Space availability is not verifiable in a remote gridftp server """
        return 999999

    def _check_md5sum(_setup_str, src_host, src_pfn):
        """
        Checks MD5 checksum using the gridftp server
        The sommand works only on filesystems/gridftp servers that support MD5 checksum
        It does not work on dCache
        Returns 0 and the cksum if succesful, else ERR_FAILEDMD5
        """

        error = PilotErrors()

        _cmd = '%suberftp %s "quote cksm md5sum 0 -1 %s"' % (_setup_str, src_host, src_pfn)
        estat, coutp = commands.getstatusoutput(_cmd)
        tolog('md5 uberftp done <%s> (%s): %s' % (_cmd, estat, coutp))

        if estat != 0:
            check_syserr(estat, coutp)
            if coutp.find('not understood') >= 0:
                tolog('!!WARNING!!2999!! MD5 unsupported by the server')
            return error.ERR_FAILEDMD5, coutp
        try:
            tmp0 = coutp.split('\n')[-1]
            fmd5usm = tmp0.split()[1]
            # split removes also  the trailing "\r" that uberftp returns, no fmd5sum.strip()
        except:
            tolog('!!WARNING!!2999!! Unable to parse MD5')
            fmd5usm = ''
        return 0, fmd5usm
    _check_md5sum = staticmethod(_check_md5sum)

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """
        Fetch a input file
        Use uberftp, invoked as shell command, to copy a file on a Gridftp server to a local directory
        gpfn -- full URL of the source file (gsiftp://... )
        path -- destination directory (local path, absolute or relative to the execution dir, in a local file system).
                It is assumed to be there. get_data returns an error if the path is missing
        fsize -- source file size, if not provided it is evaluated using uberftp as execution agent
        fchecksum -- source file MD5 checsum, if not provided it is evaluated using uberftp as execution agent
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        try:
            timeout = pdict['timeout']
        except:
            timeout = 5*3600

        # hot fix for OSCER, remove troubling port number if present
        gpfn = gpfn.replace(":2811","")

        import socket
        if( gpfn.find('SFN') != -1 ):
            s = gpfn.split('SFN=')
            src_pfn = s[1]
            # in this case the file is local 
            src_host = socket.getfqdn()
        else:
            # it assumes gpfn to be a URL: the host to have a GSIftp server, and it to see the file in the path of the URL
            _tmp = gpfn.split('/', 3)
            src_pfn = '/'+_tmp[3]
            src_host = _tmp[2]
        src_dirname = os.path.dirname(src_pfn)
        src_filename = os.path.basename(src_pfn)
        dest_file = os.path.join(path, src_filename)

        # when the file has been copied we will rename it to the lfn (to remove the __DQ2-string on some files)
        final_dest_file = os.path.join(path, lfn)

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        # do we have a proxy?
        s, pilotErrorDiag = self.verifyProxy(envsetup=_setup_str)
        if s != 0:
            return s, pilotErrorDiag

        # protect against double slashes that might cause problems
        # [path] /lscratch/usatlas1_912327//Panda_Pilot... -> /lscratch/usatlas1_912327/Panda_Pilot...
        # [src_dirname] //ibrix/data/dq2-cache -> /ibrix/data/dq2-cache
        path = path.replace('//', '/')
        src_dirname = src_dirname.replace('//', '/')

        # download the remote file to the local directory
        # if you end the command string with ';' you'll always receive error (ec=1)
        _cmd_str_remote = '%suberftp %s "lcd %s; cd %s; get %s; ls %s"|grep %s|grep -v uberftp' %\
                          (_setup_str, src_host, path, src_dirname, src_filename, src_filename, src_filename)

        tolog("Executing command: %s" % (_cmd_str_remote))
        try:
            s, telapsed, cout, cerr = timed_command(_cmd_str_remote, timeout)
        except Exception, e:
            tolog("!!WARNING!!2999!! timed_command() threw an exception: %s" % str(e))
            s = 1
            o = str(e)
            telapsed = timeout
        else:
            o = cout + cerr
            tolog("Elapsed time: %d" % (telapsed))
        
        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str_remote))
            o = o.replace('\n', ' ')
            check_syserr(s, o)

            if is_timeout(s):
                pilotErrorDiag = "Uberftp get was timed out after %d seconds" % (telapsed)
                ec = error.ERR_GETTIMEOUT
            else:
                if o.find("No such file or directory") >= 0:
                    if src_filename.find("DBRelease") >= 0:
                        pilotErrorDiag = "Missing DBRelease file: %s" % (src_filename)
                        ec = error.ERR_MISSDBREL
                    else:
                        pilotErrorDiag = "No such file or directory: %s" % (src_filename)
                        ec = error.ERR_NOSUCHFILE
                else:
                    pilotErrorDiag = "uberftp failed to transfer file: %d, %s" % (s, o)
                    ec = error.ERR_STAGEINFAILED

            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return ec, pilotErrorDiag

        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # check local file size and MD5 sum (destination file)
        ec, pilotErrorDiag, dstfsize, dstfchecksum =  self.getLocalFileInfo(dest_file, csumtype=csumtype)
        if ec != 0:
            return ec, pilotErrorDiag

        # check the remote file size
        # dCache file system
        # "-rw  413957423  mc12.005200.T1_McAtNlo_Jimmy.evgen.EVNT.v12000604_tid007939._00010.pool.root.1"
        # NFS or local file system
        # "drwx------    2  usatlas1  usatlas    4096 May  2 18:38  gram_scratch_dS20YaiYVx"
        if fsize == 0:
            tmp1 = o.split("\n")[-1].split()
            if len(tmp1) == 3:
                fsize = tmp1[1]
            else:
                try:
                    fsize = tmp1[4]
                except:
                    fsize = 0
                if len(tmp1) != 9:
                    tolog('!!WARNING!!2999!! There may be a problem with the LIST format of the ftp server. File size: %s' % fsize)
        tolog("remote_file_size=%s" % fsize)
        tolog("local_file_size=%s" % dstfsize)

        # And verify the local and remote file fileSizes to make sure that they are equal 
        # check file size
        if fsize != dstfsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(gpfn), str(dstfsize), str(fsize))
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_GETWRONGSIZE, pilotErrorDiag

        # check md5sum to make sure that the file transfer was successful
        if fchecksum == 0:
            s, fchecksum = UberftpSiteMover._check_md5sum(_setup_str, src_host, src_pfn)
            if s != 0:
                if fchecksum.find('not understood') >= 0:
                    tolog('!!WARNING!!2999!! MD5 check skipped, unsupported by the server')
                    return 0, pilotErrorDiag
                pilotErrorDiag = "!!WARNING!!2999!! check_md5sum failed: %d, %s" % (s, fchecksum)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDMD5, pilotErrorDiag

        if fchecksum != dstfchecksum and not self.isDummyChecksum(fchecksum):
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        # rename the file since it might contain the __DQ2-string which will confuse the trf
        if dest_file.find("__DQ2") != -1:
            tolog("Original file name renamed by DQ2: %s" % (dest_file))
            try:
                s, o = commands.getstatusoutput("mv %s %s" % (dest_file, final_dest_file))
            except Exception, e:
                pilotErrorDiag = "Failed to rename output file: %s" % str(e)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                return error.ERR_STAGEINFAILED, pilotErrorDiag
            else:
                if s != 0:
                    # print the ouput returned if exited abnormally
                    o = o.replace('\n', ' ')
                    check_syserr(s, o)
                    pilotErrorDiag = "Failed to rename file: %d, %s" % (s, o)
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    return error.ERR_STAGEINFAILED, pilotErrorDiag
                else:
                    tolog('Renamed input file to: %s' % (final_dest_file))

        tolog("Copy completed succesfully") 
        return 0, pilotErrorDiag

    def _checkDir(host, rootdir, dirlist, setupstring='', port=''):
        """
        Check directory existance on a gridftp server
        host -- Grid-ftp server
        port -- port used by the server (if different from the default one)
        rootdir -- root dir, supposed to exist on the server (lover level dirs are not checked)
        dirlist -- list of subdirectories to check for existance (ordered)
        setupstring -- setup to be able to invoke uberftp

        _checkDir is returning -1 in case of failure or N>=0, the number of dirs that exist and don't have to be created
        Normally 1 or more (root dir is supposed to exist)
        
        Caller can decide what to do in case of failure, the recommendation for a resilient program is
        to retry or to ignore the error and assume that all the dirs are there
        """

        # add all the commands
        if port:
            port_str = '-P %s ' % port
        else:
            port_str = ''
        cdCommandChain = "cd %s;" % rootdir
        for i in dirlist:
            cdCommandChain += " cd %s;" % i
        cmd = '%suberftp %s%s "%s"|grep successful|grep -v grep|wc -l'%(setupstring, port_str, host, cdCommandChain)
        tolog("Executing command (check dir): %s" % (cmd))
        s, output = commands.getstatusoutput(cmd)
        if s != 0:
            check_syserr(s, output)
            return -1, output
        try:
            existingParentDirCount = int(output)
        except:
            return -1, output
        return existingParentDirCount, ""
    _checkDir = staticmethod(_checkDir)

    def put_data(self, src_pfn, destination, fsize=0, fchecksum=0, dsname='', extradirs='', **pdict):
        """
        Copy all output file to the local SE
        Check SiteMover.put_data for a description of the first parameters
        src_pfn (source), destination, fsize, fchecksum
        dsname -- name of the dataset, used as additional path for the destination file
        extradirs -- additional directories
        The function tries to create the destination directory if missing
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        try:
            timeout = pdict['timeout']
        except:
            timeout = 5*3600

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        # do we have a proxy?
        s, pilotErrorDiag = self.verifyProxy(envsetup=_setup_str, limit=2)
        if s != 0:
            return self.put_data_retfail(s, pilotErrorDiag)

        # hot fix for OSCER, remove troubling port number if present
        src_pfn = src_pfn.replace(":2811","")

        # preparing variables
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(src_pfn)
            if ec != 0:
                return self.put_data_retfail(ec, pilotErrorDiag)

        dst_se = destination
        if dst_se.find('SFN') != -1:  # srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv1?SFN=/pnfs/usatlas.bnl.gov/
            s = dst_se.split('SFN=')
            dst_loc_se = s[1]
            dst_prefix = s[0]
        else:
            _sentries = dst_se.split('/', 3)
            dst_serv = _sentries[0] + '//' + _sentries[2] # 'method://host:port' is it always a ftp server? can it be srm? something else?
            dst_host = _sentries[2] # host and port
            dst_loc_se = '/'+ _sentries[3]
            dst_prefix = dst_serv
        dst_relative_path = os.path.join(extradirs, dsname)
        # dst_loc_se = base directory, not including the destination sub dir
        dst_loc_sedir = os.path.join(dst_loc_se, dst_relative_path)
        # dst_loc_sedir = full path to the final destination directory
        dst_relpath_list = [i for i in dst_relative_path.split('/') if i]
        # >>> a='/root/asd/sss'
        # >>> b=[i for i in a.split('/') if i]
        #  ['root', 'asd', 'sss']
        src_filename = os.path.basename(src_pfn)
        src_dirname = os.path.dirname(src_pfn)

        # Destination file name has to be the same, else all the code of the function has to be checked
        dst_filename = src_filename
        dst_loc_pfn = os.path.join(dst_loc_sedir, dst_filename)
        dst_gpfn = dst_prefix + dst_loc_pfn

        # get the number of directories that don't have to be created [from the dst_relpath_list]
        # TODO: check if the syntax host:port works for uberftp
        existing_dir_count_ev,__err = UberftpSiteMover._checkDir(dst_host, dst_loc_se, dst_relpath_list, _setup_str)
        if existing_dir_count_ev < 0:
            if existing_dir_count_ev == -2:
                pilotErrorDiag = ": %s" % (str(__err))
                return self.put_data_retfail(error.ERR_PUTTIMEOUT, pilotErrorDiag)
            else:
                tolog('!!WARNING!!2999!! Control of existing directories failed: %s' % existing_dir_count_ev)
                # If you try to copy a file and the directory does not exist the copy fails
                # If you try to create an already existing directory you get 'File exists' error
                # If the counting of existing directories failed, try to create all of them
                existing_dir_count = 0
                # to try to copy the file directly (creating no directory)
                # existing_dir_count = len(dst_relpath_list)
        else:
            existing_dir_count = existing_dir_count_ev

        # Preparing and executing the transfer command
        _start_dir = dst_loc_se
        _additional_dir = ''
        _relative_dir = ''
        # eg. _start_dir = /ibrix/data/dq2-cache

        if existing_dir_count < len(dst_relpath_list):
            # if existing_dir_count >= 0:
            for i in dst_relpath_list:
                if existing_dir_count > 0:                   
                    # for every loop iteration, add directory i to _start_dir
                    _start_dir = os.path.join(_start_dir, i)
                    # eg. _start_dir = /ibrix/data/dq2-cache/testpanda.destDB.72c916bd-c490-461b-8665-9fb9990081cf_sub0
                    # eg. i = testpanda.destDB.72c916bd-c490-461b-8665-9fb9990081cf_sub0
                    existing_dir_count -= 1
                else:
                    _relative_dir = os.path.join(_relative_dir, i)
                    _additional_dir += "mkdir %s;" % (_relative_dir)
                    _additional_dir += "chmod %s %s;" % (str(oct(PERMISSIONS_DIR)), _relative_dir)
            if _relative_dir:
                _relative_dir = "cd %s;" % _relative_dir
            _ftp_cmd_0 = "cd %s; %s%s" % (_start_dir, _additional_dir, _relative_dir)
        else:
            # directories are already all there
            _ftp_cmd_0 = "cd %s; " % (os.path.join(_start_dir, dst_relative_path))
        
        # protect against double slashes that might cause problems
        # [src_dirname] //ibrix/data/dq2-cache -> /ibrix/data/dq2-cache
        src_dirname = src_dirname.replace('//', '/')

        # uberftp $host "cd $directory; get $fileName; ls $fileName"|grep $fileName|grep -v uberftp > /tmp/fileSize.txt

        _ftp_cmd_str = '%s lcd %s; put %s; ls %s' %\
                       (_ftp_cmd_0, src_dirname, src_filename, dst_filename)
        _cmd_str = "%swhich uberftp;uberftp %s '%s'" %\
                   (_setup_str, dst_host, _ftp_cmd_str)

        # local dir should exist
        # _cmd_str = Copying uberftp: source /hep/home/gridapp/atlas_app/UberFTP/setup.sh; uberftp tier2-02.ochep.ou.edu
        #            'mkdir /ibrix/data/dq2-cache;
        #            cd /ibrix/data/dq2-cache/testpanda.destDB.72c916bd-c490-461b-8665-9fb9990081cf_sub0;
        #            lcd /lscratch/usatlas1_207555/Panda_Pilot_12498_1174944165/PandaJob_1280039_1174944165;
        #            put 4cd66105-bf50-499d-a81d-9f8c3ba3bf69.evgen.pool.root;
        #            lls 4cd66105-bf50-499d-a81d-9f8c3ba3bf69.evgen.pool.root;
        #            ls 4cd66105-bf50-499d-a81d-9f8c3ba3bf69.evgen.pool.root;'|
        #            grep 4cd66105-bf50-499d-a81d-9f8c3ba3bf69.evgen.pool.root|grep -v uberftp
        tolog("Executing command: %s" % (_cmd_str))
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

        tolog("All output: %d %d %s" % (s, telapsed, str(o)))
        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            check_syserr(s, o)
            if is_timeout(s):
                pilotErrorDiag = "Uberftp get was timed out after %d seconds" % (telapsed)
                return self.put_data_retfail(error.ERR_GETTIMEOUT, pilotErrorDiag)
            else:
                pilotErrorDiag = "Copy command failed: %d, %s" % (s, o)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                tolog('Additional info: SE:%s, additional dirs:%s, existing:%s' % (dst_loc_se, dst_relative_path, existing_dir_count_ev))
                if o.find("No such file or directory") >= 0:
                    return self.put_data_retfail(error.ERR_NOSUCHFILE, pilotErrorDiag)
                elif o.find(dst_filename) >= 0:
                    tolog('!!WARNING!!2999!! Exit code > 0, but file seems to have been transferred, trying to continue')
                    pilotErrorDiag = ""
                else:
                    return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        # check the remote file size
        # dCache file system
        # "-rw  413957423  mc12.005200.T1_McAtNlo_Jimmy.evgen.EVNT.v12000604_tid007939._00010.pool.root.1"
        # NFS or local file system
        # "drwx------    2  usatlas1  usatlas    4096 May  2 18:38  gram_scratch_dS20YaiYVx"

        tmp0 = [i for i in o.split("\n") if i.find(dst_filename)>=0]
        tmp1 = tmp0[-1].split()
        if len(tmp1) == 3:
            dest_fsize = tmp1[1]
        else:
            try:
                dest_fsize = tmp1[4]
            except:
                dest_fsize = 0
            if len(tmp1) != 9:
                tolog('!!WARNING!!2999!! There may be a problem with the LIST format of the ftp server (%s). File size: %s' % \
                         (tmp0[-1], dest_fsize))
        tolog("remote_dest_fsize = %s" % dest_fsize)
        tolog("local_file_size (src) = %s" % fsize)

        # compare remote and local file size
        if fsize != dest_fsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(dst_loc_pfn), str(dest_fsize), str(fsize))
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return self.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag)

        # get the checksum type (only md5sum is currently supported for uberftp)
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        fail = 0
        # check checksum to make sure that the file transfer was successful
        s, checksum_remote = UberftpSiteMover._check_md5sum(_setup_str, dst_host, dst_loc_pfn) 
        if s != 0:
            checksum_remote = checksum_remote.replace('\n', ' ')
            check_syserr(s, checksum_remote)
            if checksum_remote.find('not understood') >= 0:
                tolog('!!WARNING!!2999!! MD5 check skipped, unsupported by the server')
                # exiting with fail = 0
                s = 0
                checksum_remote = fchecksum
            else:
                pilotErrorDiag = "Error running md5sum: %d, %s" % (s, checksum_remote)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
        else:
            # compare remote and local file checksum
            if checksum_remote != fchecksum:
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(src_pfn), checksum_remote, fchecksum)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                fail = error.ERR_PUTMD5MISMATCH

        if fail != 0:
            # An error occorred during md5 evaluation/comparison
            return self.put_data_retfail(fail, pilotErrorDiag)

        if s != 0: # should never happen!
            return self.put_data_retfail(s, pilotErrorDiag)

        return 0, pilotErrorDiag, dst_gpfn, fsize, fchecksum, self.arch_type




