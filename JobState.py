import os
import commands
from pUtil import tolog
from FileHandling import getExtension

class JobState:
    """
    This class is used to set the current job state.
    When the job is running, the file jobState-<JobID>.[pickle|json]
    is created which contains the state of the Site, Job and Node
    objects. The job state file is updated at every heartbeat.
    """
    def __init__(self):
        """ Default init """
        self.job = None            # Job class object
        self.site = None           # Site class object
        self.node = None           # Node class object
        self.filename = ""         # jobState-<jobId>.[pickle|json]
        self.recoveryAttempt = 0   # recovery attempt number
        self.objectDictionary = {} # file dictionary holding all objects

    def decode(self):
        """ Decode the job state file """

        if self.objectDictionary:
            try:
                # setup the internal structure (needed for cleanup)
                self.job = self.objectDictionary['job']
                self.site = self.objectDictionary['site']
                self.node = self.objectDictionary['node']
            except:
                tolog("JOBSTATE WARNING: Objects are missing in the recovery file (abort recovery)")
                pass
            try:
                self.recoveryAttempt = self.objectDictionary['recoveryAttempt']
            except:
                tolog("JOBSTATE WARNING: recoveryAttempt number is missing (set to 0)")
                self.recoveryAttempt = 0
                pass
        else:
            tolog("JOBSTATE FAILURE: Object dictionary not defined")

        return self.job, self.site, self.node, self.recoveryAttempt

    def get(self, filename):
        """ Read job state dictionary from file """
        self.filename = filename
        status = True

        # De-serialize the job state file
        try:
            fp = open(self.filename, "r")
        except:
            tolog("JOBSTATE FAILURE: JobState get function could not open file: %s" % self.filename)
            status = False
            pass
        else:
            tolog("Managed to open job state file")
            # is it a json or pickle file? import the relevant loader
            importedLoad = False
            if self.filename.endswith('json'):
                try:
                    from json import load
                except Exception, e:
                    tolog("JOBSTATE FAILURE: Could not import load function from json module (too old python version?): %s" % str(e))
                    status = False
                    pass
                else:
                    tolog("Imported json load")
                    importedLoad = True
            else:
                from pickle import load
                importedLoad = True
                tolog("Imported pickle load")

            if importedLoad:
                # load the dictionary from file
                try:
                    # load the dictionary from file
                    self.objectDictionary = load(fp)
                except:
                    tolog("JOBSTATE FAILURE: JobState could not deserialize file: %s" % self.filename)
                    status = False
                    pass
                else:
                    tolog("Managed to load object dictionary")
            else:
                tolog("Failed to import load function")
            fp.close()

        return status

    def getCurrentFilename(self):
        """ return the current file name """

        return self.filename

    def getFilename(self, workdir, jobId):
        """ get the name of the job state file """

        # force pickle format for the time being, there seems to be problems storing the node structure in json format
        extension = "pickle"
        return "%s/jobState-%s.%s" % (workdir, jobId, extension)
        # return "%s/jobState-%s.%s" % (workdir, jobId, getExtension())

    def put(self, job, site, node, recoveryAttempt=0, mode=""):
        """
        Create/Update the job state file
        """
        status = True

        self.job = job
        self.node = node
        self.site = site
        self.recoveryAttempt = recoveryAttempt
        if not (self.node and self.job and self.site):
            status = False
        else:
            # get the appropriate filename
            self.filename = self.getFilename(self.site.workdir, self.job.jobId)

            # add mode variable if needed
            if mode != "":
                if "json" in self.filename:
                    self.filename = self.filename.replace(".json", "-%s.json" % (mode))
                else:
                    self.filename = self.filename.replace(".pickle", "-%s.pickle" % (mode))

            # create object dictionary
            objectDictionary = {}
            objectDictionary['job'] = self.job
            objectDictionary['site'] = self.site
            objectDictionary['node'] = self.node
            objectDictionary['recoveryAttempt'] = self.recoveryAttempt

            # write the dictionary
            if "json" in self.filename:
                from json import dump
            else:
                from pickle import dump
            try:
                fp = open(self.filename, "w")
            except Exception, e:
                tolog("JOBSTATE FAILURE: Could not open job state file: %s, %s" % (self.filename, str(e)))
                _cmd = "whoami; ls -lF %s" % (self.filename)
                tolog("Executing command: %s" % (_cmd))
                ec, rs = commands.getstatusoutput(_cmd)
                tolog("%d, %s" % (ec, rs))
                status = False
            else:
                try:
                    # write the dictionary to file
                    dump(objectDictionary, fp)
                except Exception, e:
                    tolog("JOBSTATE FAILURE: Could not encode data to job state file: %s, %s" % (self.filename, str(e)))
                    status = False

                fp.close()
                                                                                                        
        return status

    def rename(self, site, job):
        """
        Rename the job state file. Should only be called for
        holding jobs that have passed the maximum number of recovery attempts.
        """
        status = True

        # get the file extension
        extension = getExtension()

        fileNameOld = "%s/jobState-%s.%s" % (site.workdir, job.jobId, extension)
        fileNameNew = "%s/jobState-%s.%s.MAXEDOUT" % (site.workdir, job.jobId, extension)
        if os.path.isfile(fileNameOld):
            # rename the job state file
            try:
                os.system("mv %s %s" % (fileNameOld, fileNameNew))
            except OSError:
                tolog("JOBSTATE FAILURE: Failed to rename job state file: %s" % (fileNameOld))
                status = False
            else:
                tolog("Job state file renamed to: %s" % (fileNameNew))
        else:
            tolog("JOBSTATE FAILURE: Job state file does not exist: %s" % (fileNameOld))
            status = False
            
        return status

    def remove(self, site, job):
        """ Remove the job state file. Should only be called for
        finished jobs after the last server update. """
        status = True

#        # get the file extension
#        extension = getExtension()
#
#        # do not use self.filename in this case since this function is only
#        # used in pilot.cleanup() where self.filename has not been set
#        fileName = "%s/jobState-%s.%s" % (site.workdir, job.jobId, extension)

        # get the appropriate filename
        fileName = self.getFilename(site.workdir, job.jobId)

        if os.path.isfile(fileName):
            # remove the job state file
            try:
                os.system("rm -f %s" % fileName)
            except OSError:
                tolog("JOBSTATE FAILURE: Failed to remove job state file: %s" % fileName)
                status = False
        else:
            tolog("JOBSTATE FAILURE: Job state file does not exist: %s" % fileName)
            status = False
            
        return status
    
    def cleanup(self):
        """ Cleanup job state file as well as work and site directory (when needed) """
        status = True

        # remove the job state file
        ec = -1
        try:
            cmd = "rm -f %s" % (self.filename)
            tolog("Executing command: %s" % (cmd))
            ec, rs = commands.getstatusoutput(cmd)
        except Exception, e:
            tolog("JOBSTATE FAILURE: JobState cleanup failed to remove file: %s, %s" % (self.filename, str(e)))
#            status = False
        else:
            if ec == 0:
                tolog("Removed lost job state file: %s" % (self.filename))
            else:
                tolog("JOBSTATE FAILURE: Could not delete job state file: %d, %s" % (ec, rs))

        # remove the site directory only if there are no other job state files
        from glob import glob
        ec = -1
        files = glob("%s/jobState-*" % self.site.workdir)
        if len(files) == 0:
            # remove the site directory
            try:
                cmd = "rm -rf %s" % (self.site.workdir)
                tolog("Executing command: %s" % (cmd))
                ec, rs = commands.getstatusoutput(cmd)
            except Exception, e:
                tolog("JOBSTATE FAILURE: Could not delete site workdir: %s" % str(e))
                status = False
                pass
            else:
                if ec == 0:
                    tolog("Removed site dir: %s" % (self.site.workdir))
                else:
                    tolog("JOBSTATE FAILURE: Could not delete site workdir: %d, %s" % (ec, rs))
        else:
            # remove only the work directory (if it exists)
            if os.path.isdir(self.job.newDirNM):
                ec = -1
                try:
                    cmd = "rm -rf %s" % (self.job.newDirNM)
                    tolog("Executing command: %s" % (cmd))
                    ec, rs = commands.getstatusoutput(cmd)
                except Exception, e:
                    tolog("JOBSTATE FAILURE: Could not delete job workdir: %s" % str(e))
                    status = False
                    pass
                else:
                    if ec == 0:
                        tolog("Removed job workdir: %s" % (self.job.newDirNM))
                    else:
                        tolog("JOBSTATE FAILURE: Could not delete job workdir: %d, %s" % (ec, rs))
        return status
    
