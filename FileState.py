import os
import commands
from pUtil import tolog

class FileState:
    """
    This class is used to set and update the current output file state dictionary.
    When the job is running, the file fileState-<JobID>.pickle is created which contains
    the current state of the output file dictionary
    File state dictionary format:
      { file_name1 : state1, ..., file_nameN : stateN }
    where file_name does not contain file path since it will change for holding jobs (from work dir to data dir), and
    the state variables have the list form "file_state", "reg_state" (for output files) and "file_state", "transfer_mode" (input files).

    "file_state" can assume the following values for "output" files:
      "not_created"     : initial value for all files at the beginning of the job
      "created"         : file was created and is waiting to be transferred
      "not_transferred" : file has not been transferred
      "transferred"     : file has already been transferred (no further action)
      "missing"         : file was never created, the job failed (e.g. output file of a failed job; a log should never be missing)

    "file_state" can assume the following values for "input" files:
      "not_transferred" : file has not been transferred (can remain in this state for FileStager and directIO modes)
      "transferred"     : file has already been transferred (no further action)

    "reg_state" can assume the following values (relevant for output files):
      "not_registered"  : file was not registered in the LFC
      "registered"      : file was already registered in the LFC (no further action)

    "transfer_mode" can assume the following values (relevant for input files)
      "copy_to_scratch" : default file transfer mode
      "remote_io"       : direct access / remote IO tranfer mode
      "file_stager"     : file stager tranfer mode
      "no_transfer"     : input file has been skipped

    E.g. a file with state = "created", "not_registered" should first be transferred and then registered in the LFC.
    The file state dictionary should be created with "not_created" states as soon as the output files are known (pilot).
    The "created" states should be set after the payload has run and if the file in question were actually created.
    "transferred" should be set by the mover once the file in question has been transferred.
    "registered" should be added to the file state once the file has been registered.
    "copy_to_scratch" is to set for all input files by default. In case remote IO / FileStager instructions are found in copysetup[in]
    the state will be changed to "remote_io" / "file_stager". Brokerage can also decide that remote IO is to be used. In that case,
    "remote_io" will be set for the relevant input files (e.g. DBRelease and lib files are excluded, i.e. they will have "copy_to_scratch"
    transfer mode).
    """

    def __init__(self, workDir, jobId="0", mode="", type="output", fileName=""):
        """ Default init """

        self.fileStateDictionary = {} # file dictionary holding all objects
        self.mode = mode              # test mode

        # use default filename unless specified by initiator
        if fileName == "" and type != "": # assume output files
            self.filename = os.path.join(workDir, "fileState-%s-%s.pickle" % (type, jobId))
        else:
            self.filename = os.path.join(workDir, fileName)

        # add mode variable if needed (e.g. mode="test")
        if self.mode != "":
            self.filename = self.filename.replace(".pickle", "-%s.pickle" % (self.mode))

        # load the dictionary from file if it exists
        if os.path.exists(self.filename):
            tolog("Using file state dictionary: %s" % (self.filename))
            status = self.get()
        else:
            tolog("File does not exist: %s (will be created)" % (self.filename))

    def get(self):
        """ Read job state dictionary from file """

        status = False

        # De-serialize the file state file
        try:
            fp = open(self.filename, "r")
        except:
            tolog("FILESTATE FAILURE: get function could not open file: %s" % self.filename)
            pass
        else:
            from pickle import load
            try:
                # load the dictionary from file
                self.fileStateDictionary = load(fp)
            except:
                tolog("FILESTATE FAILURE: could not deserialize file: %s" % self.filename)
                pass
            else:
                status = True
            fp.close()

        return status
    
    def put(self):
        """
        Create/Update the file state file
        """

        status = False

        # write pickle file
        from pickle import dump
        try:
            fp = open(self.filename, "w")
        except Exception, e:
            tolog("FILESTATE FAILURE: Could not open file state file: %s, %s" % (self.filename, str(e)))
            _cmd = "whoami; ls -lF %s" % (self.filename)
            tolog("Executing command: %s" % (_cmd))
            ec, rs = commands.getstatusoutput(_cmd)
            tolog("%d, %s" % (ec, rs))
        else:
            try:
                # write the dictionary to file
                dump(self.fileStateDictionary, fp)
            except Exception, e:
                tolog("FILESTATE FAILURE: Could not pickle data to file state file: %s, %s" % (self.filename, str(e)))
            else:
                status = True
            fp.close()
                                                                                                        
        return status

    def getNumberOfFiles(self):
        """ Get the number of files from the file state dictionary """

        return len(self.fileStateDictionary.keys())

    def getStateList(self, filename):
        """ Get the current state list for a file """

        if self.fileStateDictionary.has_key(filename):
            state_list = self.fileStateDictionary[filename]
        else:
            state_list = ["", ""]

        return state_list

    def updateStateList(self, filename, state_list):
        """ Update the state list for a file """

        status = False
        try:
            self.fileStateDictionary[filename] = state_list
        except Exception, e:
            tolog("FILESTATE FAILURE: could not update state list for file: %s, %s" % (filename, str(e)))
        else:
            status = True

        return status

    def getFileState(self, filename):
        """ Return the current state of a given file """

        # get current state list
        return self.getStateList(filename)

    def updateState(self, filename, mode="file_state", state="not_transferred"):
        """ Update the file or registration state for a file """

        status = False

        # get current state list
        state_list = self.getStateList(filename)

        # update file state
        try:
            if mode == "file_state":
                state_list[0] = state
            elif mode == "reg_state" or mode == "transfer_mode":
                state_list[1] = state
            else:
                tolog("FILESTATE FAILURE: unknown state: %s" % (mode))
        except Exception, e:
            tolog("FILESTATE FAILURE: %s" % str(e))
        else:
            # update state list
            status = self.updateStateList(filename, state_list)

        # update the file state file for every update (necessary since a failed put operation can abort everything)
        if status:
            status = self.put()

        return status

    def resetStates(self, file_list, type="output"):
        """ Set all states in file list to not_created, not_registered """
        # note: file state will be reset completely

        tolog("Resetting file list: %s" % str(file_list))

        # initialize file state dictionary
        self.fileStateDictionary = {}
        if type == "output":
            for filename in file_list:
                self.fileStateDictionary[filename] = ['not_created', 'not_registered']
        else: # input
            for filename in file_list:
                self.fileStateDictionary[filename] = ['not_transferred', 'copy_to_scratch']

        # write to file
        status = self.put()

    def hasOnlyCopyToScratch(self):
        """ Check if there are only copy_to_scratch transfer modes in the file dictionary """

        status = True

        # loop over all input files and see if there is any non-copy_to_scratch transfer mode
        for filename in self.fileStateDictionary.keys():
            # get the file states
            states = self.fileStateDictionary[filename]
            tolog("filename=%s states=%s"%(filename,str(states)))
            if states[1] != 'copy_to_scratch':
                status = False
                break
        return status

    def dumpFileStates(self, type="output"):
        """ Print all the files and their states """

        if type == "output":
            tolog("File name  /  File state  /  Registration state")
        else:
            tolog("File name  /  File state  /  Transfer mode")
        tolog("-"*100)
        n = self.getNumberOfFiles()
        i = 1
        if n > 0:
            sorted_keys = self.fileStateDictionary.keys()
            sorted_keys.sort()
            for filename in sorted_keys:
                states = self.fileStateDictionary[filename]
                if len(states) == 2:
                    tolog("%d. %s\t%s\t%s" % (i, filename, states[0], states[1]))
                else:
                    tolog("%s\t-\t-" % (filename))
                i += 1
        else:
            tolog("(No files)")
