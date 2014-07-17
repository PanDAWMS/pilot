# Class definition:
#   OtherExperiment
#   This class is the prototype of an experiment class inheriting from Experiment
#   Instances are generated with ExperimentFactory via pUtil::getExperiment()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
from Experiment import Experiment  # Main experiment class
from pUtil import tolog            # Logging method that sends text to the pilot log
from pUtil import readpar          # Used to read values from the schedconfig DB (queuedata)
from PilotErrors import PilotErrors

class OtherExperiment(Experiment):

    # private data members
    __experiment = "Other"
    __instance = None
    __error = PilotErrors()                # PilotErrors object
    __doFileLookups = False                # True for LFC based file lookups (basically a dummy data member here since singleton object is static)
    __cache = ""                           # Cache URL used e.g. by LSST

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(OtherExperiment, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def setParameters(self, *args, **kwargs):
        """ Set any internally needed variables """

        # set initial values
        self.__job = kwargs.get('job', None)
        if self.__job:
            self.__analysisJob = isAnalysisJob(self.__job.trf)

    def getJobExecutionCommand(self):
        """ Define and test the command(s) that will be used to execute the payload """
        # E.g. cmd = "source <path>/setup.sh; <path>/python "

        cmd = ""

        return cmd

    def willDoFileLookups(self):
        """ Should (LFC) file lookups be done by the pilot or not? """

        return False

    def willDoFileRegistration(self):
        """ Should (LFC) file registration be done by the pilot or not? """

        return False

    def doFileLookups(self, doFileLookups):
        """ Update the file lookups boolean """

        # Only implement this method if class really wants to update the __doFileLookups boolean
        # ATLAS wants to implement this, but not CMS
        # Method is used by Mover
        # self.__doFileLookups = doFileLookups
        pass

    def isOutOfMemory(self, **kwargs):
        """ Try to identify out of memory errors in the stderr/out """

        return False

    def getNumberOfEvents(self, **kwargs):
        """ Return the number of events """

        return 0

    def specialChecks(self, **kwargs):
        """ Implement special checks here """
        # Return False if fatal failure, otherwise return True
        # The pilot will abort if this method returns a False

        status = False

        tolog("No special checks for \'%s\'" % (self.__experiment))

        return True # obviously change this to 'status' once implemented

    # Optional
    def setCache(self, cache):
        """ Cache URL """
        # Used e.g. by LSST

        self.__cache = cache

    # Optional
    def getCache(self):
        """ Return the cache URL """
        # Used e.g. by LSST

        return self.__cache

    # Optional
    def useTracingService(self):
        """ Use the DQ2 Tracing Service """
        # A service provided by the DQ2 system that allows for file transfer tracking; all file transfers
        # are reported by the pilot to the DQ2 Tracing Service if this method returns True

        return False

if __name__ == "__main__":

    print "Implement test cases here"
    
