import os, commands

from lcgcpSiteMover import lcgcpSiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, getExperiment

class stormSiteMover(lcgcpSiteMover):
    """ SiteMover for storm sites
    """

    copyCommand = "storm"
    checksum_command = "adler32"

    def core_get_data(self, envsetup, token, source_surl, local_fullname, experiment):
        """ special get function developed for storm sites """

        error = PilotErrors()

        # Transform the surl into a full surl
        full_se_endpoint = self.extractSE(readpar('se').split(",")[0])[1]
        prefix = os.path.commonprefix([source_surl, full_se_endpoint])
        if prefix:
            # Can use the bdii-free form
            source_surl = full_se_endpoint + source_surl[len(prefix):]
            _cmd_str = '%s lcg-gt --nobdii --setype srmv2 "%s" file' % (envsetup, source_surl)
        else:
            # Fallback solution, use old lcg-gt form 
            # get the TURL using the SURL
            tolog("!!WARNING!1234!! Source surl does not match %s, cannot use the bdii-independent lcg-gt" % full_se_endpoint)
            _cmd_str = '%s lcg-gt "%s" file' % (envsetup, source_surl)

        tolog("Executing command: %s" % (_cmd_str))
        t0 = os.times()
        s, o = commands.getstatusoutput(_cmd_str)
        t1 = os.times()
        t = t1[4] - t0[4]
        tolog("Command finished after %f s" % (t))
        if s == 0:
            # get the experiment object
            thisExperiment = getExperiment(experiment)

            # add the full stage-out command to the job setup script
            to_script = _cmd_str
            to_script = to_script.lstrip(' ') # remove any initial spaces
            if to_script.startswith('/'):
                to_script = 'source ' + to_script
            thisExperiment.updateJobSetupScript(os.path.dirname(local_fullname), to_script=to_script)

            source_turl, req_token = o.split('\n')
            source_turl = source_turl.replace('file://','')
            tolog("Creating link from %s to %s" % (source_turl, local_fullname))
            try:
                os.symlink(source_turl, local_fullname)
                _cmd_str = '%s lcg-sd %s %s 0' % (envsetup, source_surl, req_token)
                tolog("Executing command: %s" % (_cmd_str))
                s,o = commands.getstatusoutput(_cmd_str)
                # Do we need to check the exit status of lcg-sd? What do we do if it fails?
                tolog("get_data succeeded")
            except Exception, e:
                pilotErrorDiag = "Exception caught: %s" % str(e)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                tolog("get_data failed")
                return error.ERR_STAGEINFAILED, pilotErrorDiag
        else:
            o = o.replace('\n', ' ')
            check_syserr(s, o)

            # Fall back if file protocol is not supported
            if o.find("Can not handle specified protocol") >= 0:
                tolog("'file' protocol not supported, reverting to lcg-cp standard mover")
                return lcgcpSiteMover.core_get_data(self, envsetup, token, source_surl, local_fullname, experiment)

            tolog("!!WARNING!!2990!! get_data failed. Status=%s Output=%s" % (s, str(o)))
            if o.find("No space left on device") >= 0:
                pilotErrorDiag = "No available space left on local disk: %s" % (o)
                tolog("No available space left on local disk")
                return error.ERR_NOLOCALSPACE, pilotErrorDiag
            elif o.find("No such file or directory") >= 0:
                if source_surl.find("DBRelease") >= 0:
                    pilotErrorDiag = "Missing DBRelease file: %s" % (source_surl)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    return error.ERR_MISSDBREL, pilotErrorDiag
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (source_surl)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    return error.ERR_NOSUCHFILE, pilotErrorDiag
            else:
                if len(o) == 0 and t >= self.timeout:
                    pilotErrorDiag = "Copy command self timed out after %d s" % (t)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    return error.ERR_GETTIMEOUT, pilotErrorDiag
                else:
                    if len(o) == 0:
                        pilotErrorDiag = "Copy command returned error code %d but no output" % (s)
                    else:
                        pilotErrorDiag = o
                    return error.ERR_STAGEINFAILED, pilotErrorDiag

        return None
