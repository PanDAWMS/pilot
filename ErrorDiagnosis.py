# Class definition:
#   ErrorDiagnosis
#

import os
import commands
from Diagnosis import Diagnosis
from PilotErrors import PilotErrors
from pUtil import tolog, getExperiment

class ErrorDiagnosis(Diagnosis):

    # private data members
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(ErrorDiagnosis, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def interpretPayload(self, job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode):
        """ Interpret the payload, look for specific errors in the stdout """

        # get the experiment object
        thisExperiment = getExperiment(job.experiment)
        if not thisExperiment:
            job.pilotErrorDiag = "ErrorDiagnosis did not get an experiment object from the factory"
            job.result[2] = error.ERR_GENERALERROR # change to better/new error code
            tolog("!!WARNING!!3234!! %s" % (job.pilotErrorDiag))
            return job

        # Extract job information (e.g. number of events)
        job = self.extractJobInformation(job, runCommandList) # add more arguments as needed

        # interpret the stdout (the stdout is experiment specific so use the corresponding method)
        job = thisExperiment.interpretPayloadStdout(job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode)

        return job

    def extractJobInformation(self, job, runCommandList):
        """ Extract relevant job information, e.g. number of events """

        # get the experiment object
        thisExperiment = getExperiment(job.experiment)
        tolog("extractJobInformation: thisExperiment=%s" % (thisExperiment))
        if not thisExperiment:
            job.pilotErrorDiag = "ErrorDiagnosis did not get an experiment object from the factory"
            job.result[2] = error.ERR_GENERALERROR # change to better/new error code
            tolog("!!WARNING!!3234!! %s" % (job.pilotErrorDiag))
            return job

        # note that this class should not be experiment specific, so move anything related to ATLAS to ATLASExperiment.py
        # and use thisExperiment.whatever() to retrieve it here

        # grab the number of events
        try:
            # nEvents_str can be a string of the form N|N|..|N with the number of jobs in the trf(s) [currently not used]
            # Add to Job class if necessary
            job.nEvents, job.nEventsW, nEvents_str = thisExperiment.getNumberOfEvents(job=job, number_of_jobs=len(runCommandList))
        except Exception, e:
            tolog("!!WARNING!!2999!! Failed to get number of events: %s (ignore)" % str(e))

        return job

    
if __name__ == "__main__":

    print "Implement test cases here"

    ed = ErrorDiagnosis()
    # ed.hello()
    
