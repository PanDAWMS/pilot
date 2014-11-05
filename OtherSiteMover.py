from pUtil import tolog
from PilotErrors import PilotErrors

class OtherSiteMover(SiteMover):
    """
    SiteMover for unknown command
    """

    copyCommand = "unknown"
    checksum_command = "adler32"
    has_mkdir = True
    has_df = False
    has_getsize = True
    has_md5sum = False
    has_chmod = True
    timeout = 5*3600

    def __init__(self, setup_path, mover_command, *args, **kwrds):
        self._copycmd = mover_command
        self.copyCommand = mover_command
        self._setup = setup_path

    def get_timeout(self):
        return self.timeout

    def getID(self):
        return self.copyCommand
    
    def check_space(self, ub):
        """For when space availability is not verifiable"""
        return 999999
    
    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        "Executes setup and command after it"
        error = PilotErrors()
        ec = 0
        pilotErrorDiag = ""
        s, o = commands.getstatusoutput('source %s; %s %s %s' % (self._setup, self._copycmd, gpfn, path))
        if s != 0:
            pilotErrorDiag = "Error during copy: %s" % (o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            ec = error.ERR_STAGEINFAILED
        # TODO: how are md5 and size controlled? some processing?
        return ec, pilotErrorDiag

    def put_data(self, pfn, ddm_storage, fsize=0, fchecksum=0, dsname='', extradirs='', **pdict):
        """ Should be generic: executes setup and command after it """

        error = PilotErrors()
        pilotErrorDiag = ""

        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = getLocalFileInfo(source, csumtype="adler32")
            if ec != 0:
                return SiteMover.put_data_retfail(ec, pilotErrorDiag)

        s, o = commands.getstatusoutput('source %s; %s %s %s' % (self._setup, self._copycmd, source, destination))
        if s != 0:
            check_syserr(s, o)
            pilotErrorDiag = "Error during copy: %s" % (o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return SiteMover.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
        # TODO: how are md5 and size controlled?
        return 0, pilotErrorDiag, destination, fsize, fchecksum, ARCH_DEFAULT
                
