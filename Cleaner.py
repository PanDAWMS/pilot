import os
import time
import commands
from dircache import listdir
from glob import glob
from pUtil import tolog
from JobState import JobState

class Cleaner:
    """
    This class is used to clean up lingering old/lost jobs.
    The clean-up criteria is that for a found Panda job directory,
    if the pilotlog.txt has not been updated for at least <limit>
    hours, and if the job state is 'running' then the assumption is
    that the job was unexpectedly terminated and should be erased from disk.
    The class defines the clean-up limit, but overrides this value if set
    in schedconfig.
    The cleanup() method should be executed after queuedata has been downloaded
    and after job recovery (which might or might not be turned on).

    Usage:
           from Cleaner import Cleaner
           cleaner = Cleaner(limit=<limit>, path=<path>, uflag=<uflag>)
           ec = cleaner.cleanup()

    cleanup() will return True for a successful/performed cleanup, False otherwise.

    <path> should normally be thisSite.wntmpdir
    <limit> should be an integer > 0 [hours]
    <uflag> user flag needed to distinguish job type (an analysis pilot is not allowed
            to touch production job directories on some sites)
    """

    def __init__(self, limit=12, path="/tmp", uflag=None):
        """ Default init with verification """

        self.clean = True
        self.uflag = None
        # verify the clean-up limit
        _type = str(limit.__class__)
        if limit and _type.find('int') == -1:
            tolog("Trying to convert limit from type %s to int" % (_type))
            try:
                limit = int(limit)
            except:
                tolog("Failed to convert, reset to default")
                limit = 12
        if limit == 0:
            tolog("Clean-up limit set to zero (no clean-up will be done)")
            self.clean = False
        elif limit < 0 or not limit:
            limit = 12
            tolog("!!WARNING!!5500!! Clean-up limit out of bounds, reset to default: %d" % (limit))

        self.limit = limit
        tolog("Cleaner initialized with clean-up limit: %d hours" % (self.limit))

        # verify the clean-up path and set the uflag if necessary
        if self.clean:
            if not path:
                path = "/tmp"
                tolog("Requested path reset to default: %s" % (path))
            if os.path.exists(path):
                self.path = path
                tolog("Cleaner will scan for lost directories in verified path: %s" % (self.path))

                if uflag:
                    self.uflag = uflag
            else:
                tolog("!!WARNING!!5500!! No such directory: %s (clean-up not possible)" % (path))
                self.path = None
                self.clean = False

    def cleanup(self):
        """ execute the clean-up """

        status = True
        number_of_cleanups = 0

        if self.clean:
            tolog("Executing empty dirs clean-up, stage 1/5")
            Cleaner.purgeEmptyDirs(self.path)

            tolog("Executing work dir clean-up, stage 2/5")
            Cleaner.purgeWorkDirs(self.path)

            tolog("Executing maxed-out dirs clean-up, stage 3/5")
            Cleaner.purgeMaxedoutDirs(self.path)

            tolog("Executing AthenaMP clean-up, stage 4/5 <SKIPPED>")
            #files = ['AthenaMP_*', 'fifo_*', 'TokenExtractorChannel*', 'zmq_EventService*', 'asetup*', 'tmp*.pkl']
            #for f in files:
            #    Cleaner.purgeFiles(self.path, f, limit=48*3600)

            tolog("Executing PanDA Pilot dir clean-up, stage 5/5")
            JS = JobState()

            # grab all job state files in all work directories
            job_state_files = glob(self.path + "/Panda_Pilot_*/jobState-*.pickle")
            number_of_files = len(job_state_files)
            file_number = 0
            max_cleanups = 30
            tolog("Number of found job state files: %d" % (number_of_files))
            if job_state_files:
                # loop over all found job state files
                for file_path in job_state_files:
                    file_number += 1
                    if file_number > max_cleanups:
                        tolog("Maximum number of job recoveries exceeded for this pilot: %d" % (max_cleanups))
                        break
                    tolog("Processing job state file %d/%d: %s" % (file_number, number_of_files, file_path))
                    current_time = int(time.time())

                    # when was file last modified?
                    try:
                        file_modification_time = os.path.getmtime(file_path)
                    except:
                        # skip this file since it was not possible to read the modification time
                        pass
                    else:
                        # was the job state file updated longer than the time limit? (convert to seconds)
                        mod_time = current_time - file_modification_time
                        if mod_time > self.limit*3600:
                            tolog("File was last modified %d seconds ago (proceed)" % (mod_time))
                            cmd = "whoami; ls -lF %s; ls -lF %s" % (file_path, os.path.dirname(file_path))
                            tolog("Executing command: %s" % (cmd))
                            ec, rs = commands.getstatusoutput(cmd)
                            if ec == 0:
                                tolog("%s" % (rs))
                            else:
                                tolog("!!WARNING!!2999!! %d, %s" % (ec, rs))

                            # open the job state file
                            if JS.get(file_path):
                                # decode the job state info
                                _job, _site, _node, _recoveryAttempt = JS.decode()

                                # add member if it doesn't exist (new Job version)
                                try:
                                    _tmp = _job.prodSourceLabel
                                except:
                                    _job.prodSourceLabel = ''

                                if _job and _site and _node:
                                    # query the job state file for job information
                                    if _job.result[0] == 'running' or _job.result[0] == 'starting' or (_job.result[0] == 'holding' and mod_time > 7*24*3600):
                                        if _job.result[0] == 'holding':
                                            tolog("Job %s was found in %s state but has not been modified for a long time - will be cleaned up" % (_job.jobId, _job.result[0]))
                                        else:
                                            tolog("Job %s was found in %s state - will be cleaned up" % (_job.jobId, _job.result[0]))
                                        tolog("Erasing directory: %s" % (_site.workdir))
                                        cmd = "rm -rf %s" % (_site.workdir)
                                        try:
                                            ec, rs = commands.getstatusoutput(cmd)
                                        except:
                                            tolog("!!WARNING!!5500!! Could not erase lost job workdir: %d, %s" % (ec, rs))
                                            status = False
                                            break
                                        else:
                                            tolog("Lost job workdir removed")
                                    else:
                                        tolog("Job found in state: %s" % (_job.result[0]))
                        else:
                            tolog("File was last modified %d seconds ago (skip)" % (mod_time))
            else:
                tolog("No job state files were found, aborting clean-up")
        else:
            tolog("Clean-up turned off")
            status = False

        return status

    def purgeEmptyDirs(path):
        """ locate and remove empty lingering dirs """

        all_dirs = glob("%s/Panda_Pilot_*" % (path))
        max_dirs = 50
        purged_nr = 0
        dir_nr = 0

        for _dir in all_dirs:
            if dir_nr >= max_dirs:
                break
            # when was the dir last modified?
            current_time = int(time.time())
            try:
                file_modification_time = os.path.getmtime(_dir)
            except:
                # skip this dir since it was not possible to read the modification time
                pass
            else:
                mod_time = current_time - file_modification_time
                if mod_time > 2*3600:
                    try:
                        ls = listdir(_dir)
                    except Exception, e:
                        tolog("!!WARNING!!2999!! Exception caught: %s" % str(e))
                    else:
                        if len(ls) == 0 or len(ls) == 1:
                            if len(ls) == 0:
                                tolog("Found empty dir: %s (last modified %d s ago, will now purge it)" % (_dir, mod_time))
                            else:
                                tolog("Found empty dir: %s (last modified %d s ago, will now purge it, 1 sub dir: %s)" % (_dir, mod_time, ls[0]))

                            ec, rs = commands.getstatusoutput("rm -rf %s" % (_dir))
                            if ec != 0:
                                tolog("Failed to remove dir: %d, %s (belonging to user %d, pilot is run by user %d)" %\
                                      (ec, rs, os.stat(_dir)[4], os.getuid()))
                            else:
                                purged_nr += 1
            dir_nr += 1
        tolog("Purged %d empty directories" % (purged_nr))
            
    purgeEmptyDirs = staticmethod(purgeEmptyDirs)    

    def purgeWorkDirs(path):
        """ locate and remove lingering athena workDirs """

        all_dirs = glob("%s/Panda_Pilot_*/PandaJob*" % (path))
        max_dirs = 50
        purged_nr = 0
        dir_nr = 0

        for _dir in all_dirs:
            if dir_nr >= max_dirs:
                break
            # when was the dir last modified?
            current_time = int(time.time())
            try:
                file_modification_time = os.path.getmtime(_dir)
            except:
                # skip this dir since it was not possible to read the modification time
                pass
            else:
                mod_time = current_time - file_modification_time
                if mod_time > 2*3600:
                    try:
                        ls = listdir(_dir)
                    except Exception, e:
                        tolog("!!WARNING!!2999!! Exception caught: %s" % str(e))
                    else:
                        if len(ls) == 1:
                            if "workDir" in ls:
                                ec, rs = commands.getstatusoutput("ls -lF %s" % (_dir))
                                tolog("ls: %s" % str(rs))
                                tolog("Found single workDir: %s (will now purge it)" % (_dir))
                                ec, rs = commands.getstatusoutput("rm -rf %s" % (_dir))
                                if ec != 0:
                                    tolog("Failed to remove dir: %s" % (rs))
                                else:
                                    purged_nr += 1
            dir_nr += 1
        tolog("Purged %d single workDirs directories" % (purged_nr))

    purgeWorkDirs = staticmethod(purgeWorkDirs)    

    def purgeFiles(path, filename, limit=12*3600):
        """ locate and remove lingering directories/files """

        all_files = glob("%s/%s" % (path, filename))
        max_files = 50
        file_nr = 0

        for _file in all_files:
            if file_nr >= max_files:
                break

            # when was the dir last modified?
            current_time = int(time.time())
            try:
                file_modification_time = os.path.getmtime(_file)
            except:
                # skip this dir since it was not possible to read the modification time
                pass
            else:
                mod_time = current_time - file_modification_time
                if mod_time > limit:
                    tolog("Found file %s last modified %d s ago (will now try to purge it)" % (_file, mod_time))
                    ec, rs = commands.getstatusoutput("rm -f %s" % (_file))
                    if ec != 0:
                        tolog("Failed to remove dir: %s" % (rs))
            file_nr += 1

    purgeFiles = staticmethod(purgeFiles)

    def purgeMaxedoutDirs(path):
        """ locate and remove maxedout lingering dirs """

        all_dirs = glob("%s/Panda_Pilot_*" % (path))
        max_dirs = 50
        purged_nr = 0
        dir_nr = 0

        for _dir in all_dirs:
            if dir_nr >= max_dirs:
                break
            # when was the dir last modified?
            current_time = int(time.time())
            try:
                file_modification_time = os.path.getmtime(_dir)
            except:
                # skip this dir since it was not possible to read the modification time
                pass
            else:
                mod_time = current_time - file_modification_time
                if mod_time > 2*3600:
                    try:
                        ls = listdir(_dir)
                    except Exception, e:
                        tolog("!!WARNING!!2999!! Exception caught: %s" % str(e))
                    else:
                        if len(ls) > 0:
                            purge = False
                            for f in ls:
                                if ".MAXEDOUT" in f:
                                    tolog("Found MAXEDOUT job state file: %s (will now purge the work dir: %s)" % (f, _dir))
                                    purge = True
                                    break
                            if purge:
                                ec, rs = commands.getstatusoutput("rm -rf %s" % (_dir))
                                if ec != 0:
                                    tolog("Failed to remove dir: %d, (belonging to user %d, pilot is run by user %d)" %\
                                          (ec, os.stat(_dir)[4], os.getuid()))
                                else:
                                    purged_nr += 1
            dir_nr += 1
        tolog("Purged %d empty directories" % (purged_nr))
            
    purgeMaxedoutDirs = staticmethod(purgeMaxedoutDirs)    
