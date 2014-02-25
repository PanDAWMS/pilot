# Class responsible for file registration in the LRC and LFC catalogs

# Note: currently, the LFC registration code is kept in SiteMover. Should be moved here.
# Note: LFC registration in EGEE is done in the site mover with lcg-cr

from PilotErrors import PilotErrors
from pUtil import createLockFile, tolog

class FileRegistration:
    """ File registration handling """

    error = PilotErrors()

    def registerFilesLRC(self, ub, rf, islogfile, jobrec, workdir, errorLabel):
        """ Error handling of LRC file registration """

        ec = 0
        pilotErrorDiag = ""
        state = ""
        msg = ""
        latereg = False
        rc, ret = self.performLRCFileRegistration(rf, ub=ub)
        if ret == None:
            ret = "(None)"
        if rc == 0:
            tolog("LRC registration succeeded")
            state = "finished"
            # create a weak lock file for the log registration
            if islogfile:
                createLockFile(jobrec, workdir, lockfile="LOGFILEREGISTERED")
        elif rc == error.ERR_DDMREG:
            # remove any trailing "\r" or "\n" (there can be two of them)
            ret = ret.rstrip()
            tolog("Error string: %s" % (ret))
            if ret.find("string Limit exceeded 250") >= 0:
                pilotErrorDiag = "LRC registration error: file name string limit exceeded 250: %s" % (ret)
                ec = error.ERR_LRCREGSTRSIZE
                state = "failed"
                msg = errorLabel
            elif ret.find("Connection refused") >= 0:
                pilotErrorDiag = "LRC registration error: %s" % (ret)
                # postpone LRC registration if site supports job recovery
                ec = error.ERR_LRCREGCONNREF
                state = "holding"
                msg = "WARNING"
                latereg = True
            else:
                pilotErrorDiag = "LRC registration error: %s" % (ret)
                # postpone LRC registration if site supports job recovery
                ec = error.ERR_LRCREG
                state = "holding"
                msg = "WARNING"
                latereg = True
        elif rc == error.ERR_LRCREGDUP:
            pilotErrorDiag = "LRC registration error: Non-unique LFN: %s" % (ret)
            ec = rc
            state = "failed"
            msg = errorLabel
        elif rc == error.ERR_GUIDSEXISTSINLRC:
            pilotErrorDiag = "LRC registration error: Guid-metadata entry already exists: %s" % (ret)
            ec = rc
            state = "failed"
            msg = errorLabel
        else:
            pilotErrorDiag = "LRC registration failed: %d, %s" % (rc, ret)
            # postpone LRC registration if site supports job recovery
            ec = error.ERR_LRCREG
            latereg = True
            state = "holding"
            msg = "WARNING"

        return ec, pilotErrorDiag, state, msg, latereg

    def performLRCFileRegistration(self, fields, ub=None):
        """ actually register all files in the LRC """

        from urllib import urlencode, urlopen
        ret = '1'
        ec = 0
        if ub != "None" and ub != None and ub != "": # ub is 'None' outside the US
            # find out if checksum or adler32 should be added
            from SiteMover import SiteMover
            _checksum = fields[4].split("+")[0] # first one, assume same type for the rest
            if len(_checksum) > 0:
                csumtype = SiteMover.getChecksumType(_checksum)
            else:
                csumtype = CMD_CHECKSUM # use default (md5sum)

            if csumtype == "adler32":
                params = urlencode({'pfns': fields[0], 'lfns': fields[1], 'guids': fields[2], 'fsizes': fields[3],\
                                    'md5sums': '', 'adler32s': fields[4], 'archivals': fields[5]})        
            else:
                params = urlencode({'pfns': fields[0], 'lfns': fields[1], 'guids': fields[2], 'fsizes': fields[3],\
                                    'md5sums': fields[4], 'adler32s': '', 'archivals': fields[5]})        
            try:
                url = ub + '/lrc/files'
                if url.find('//lrc') > 0:
                    url = url.replace('//lrc','/lrc')
                tolog("Will send params: %s" % str(params))
                tolog("Trying urlopen with: %s" % (url))
                f = urlopen(url, params)
            except Exception, e:
                tolog("!!WARNING!!4000!! Unexpected exception: %s" % str(e))
                ec = error.ERR_DDMREG
                ret = str(e)
            else:
                ret = f.read()
                if ret != '1':
                    ret = ret.replace('\n', ' ')
                    tolog('!!WARNING!!4000!! LRC registration error: %s' % str(ret))
                    tolog('!!WARNING!!4000!! LRC URL requested: %s' % f.geturl())
                    if ret == 'LFNnonunique':
                        ec = error.ERR_LRCREGDUP
                    elif ret.find("guid-metadata entry already exists") >= 0:
                        ec = error.ERR_GUIDSEXISTSINLRC
                    else:
                        ec = error.ERR_DDMREG

        return ec, ret
