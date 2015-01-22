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
            job.result[2] = self.__error.ERR_GENERALERROR # change to better/new error code
            tolog("!!WARNING!!3234!! %s" % (job.pilotErrorDiag))
            return job

        ### WARNING: EXPERIMENT SPECIFIC, MOVE LATER
        try:
            ec, pilotErrorDiag = self.processJobReport(job.workdir)
        except Exception, e:
            tolog("!!WARNING!!1114!! Caught exception: %s" % (e))
        else:
            if ec != 0:
                job.pilotErrorDiag = pilotErrorDiag
                job.result[2] = ec
                return job
                
        # Extract job information (e.g. number of events)
        job = self.extractJobInformation(job, runCommandList) # add more arguments as needed

        # interpret the stdout (the stdout is experiment specific so use the corresponding method)
        job = thisExperiment.interpretPayloadStdout(job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode)

        return job

    ### WARNING: EXPERIMENT SPECIFIC, MOVE LATER
    def getJobReport(self, workDir):
        """ Get the jobReport.json dictionary """

        fileName = os.path.join(workDir, "jobReport.json")
        if os.path.exists(fileName):
            # the jobReport file exists, read it back                                                                                                                                                                                                          
            try:
                f = open(fileName, "r")
            except Exception, e:
                tolog("!!WARNING!!1001!! Could not open file: %s, %s" % (fileName, e))
                jobReport_dictionary = {}
            else:
                from json import load
                try:
                    # load the dictionary
                    jobReport_dictionary = load(f)
                except Exception, e:
                    tolog("!!WARNING!!1001!! Could not read back jobReport dictionary: %s" % (e))
                    jobReport_dictionary = {}

                # done with the file
                f.close()
        else:
            tolog("!!WARNING!!1111!! File %s does not exist" % (fileName))
            jobReport_dictionary = {}

        return jobReport_dictionary

    ### WARNING: EXPERIMENT SPECIFIC, MOVE LATER
    def getJobReportErrors(self, jobReport_dictionary):
        """ Extract the error list from the jobReport.json dictionary """
        # WARNING: Currently compatible with version <= 0.9.4

        jobReportErrors = []
        if jobReport_dictionary.has_key('reportVersion'):
            tolog("Scanning jobReport (v %s) for error info" % jobReport_dictionary['reportVersion'])
        else:
            tolog("WARNING: jobReport does not have the reportVersion key")

        if jobReport_dictionary.has_key('executor'):
            try:
                error_details = jobReport_dictionary['executor'][0]['logfileReport']['details']['ERROR']
            except Exception, e:
                tolog("WARNING: Aborting jobReport scan: %s"% (e))
            else:
                try:
                    for m in error_details:
                        jobReportErrors.append(m['message'])
                except Exception, e:
                    tolog("!!WARNING!!1113!! Did not get a list object: %s" % (e))
        else:
            tolog("WARNING: jobReport does not have the executor key (aborting)")

        return jobReportErrors

    ### WARNING: EXPERIMENT SPECIFIC, MOVE LATER
    def isBadAlloc(self, jobReportErrors):
        """ Check for bad_alloc errors """

        bad_alloc = False
        pilotErrorDiag = ""
        for m in jobReportErrors:
            if "bad_alloc" in m:
                tolog("!!WARNING!!1112!! Encountered a bad_alloc error: %s" % (m))
                bad_alloc = True
                pilotErrorDiag = m
                break

        return bad_alloc, pilotErrorDiag

    ### WARNING: EXPERIMENT SPECIFIC, MOVE LATER
    def processJobReport(self, workDir):
        """ Scan the jobReport.json for specific errors """

        # Specific errors
        ec = 0
        pilotErrorDiag = ""
        bad_alloc = False

        jobReport_dictionary = self.getJobReport(workDir)
        if jobReport_dictionary != {}:
            jobReportErrors = self.getJobReportErrors(jobReport_dictionary)

            # Check for specific errors
            if jobReportErrors != []:
                bad_alloc, pilotErrorDiag = self.isBadAlloc(jobReportErrors)
                if bad_alloc:
                    ec = self.__error.ERR_BADALLOC # get the corresponding error code

        return ec, pilotErrorDiag

    def extractJobInformation(self, job, runCommandList):
        """ Extract relevant job information, e.g. number of events """

        # get the experiment object
        thisExperiment = getExperiment(job.experiment)
        if not thisExperiment:
            job.pilotErrorDiag = "ErrorDiagnosis did not get an experiment object from the factory"
            job.result[2] = self.__error.ERR_GENERALERROR # change to better/new error code
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
    
