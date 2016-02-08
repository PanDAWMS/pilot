# Class definition:
#   EventService
#   This class is the main Event Service class; ATLAS etc will inherit from this class
#   Instances are generated with EventServiceFactory
#   Subclasses should implement all needed methods prototyped in this class
#   Note: not compatible with Singleton Design Pattern due to the subclassing

# The dispatcher response for an event server job is a dictionary with the following format
# 'eventRangeID':???, 'startEvent':???, 'lastEvent':???, 'lfn':'???', 'guid':???, 'attemptNr':???}

import os
import re
import commands

from PilotErrors import PilotErrors
from pUtil import tolog                    # Dump to pilot log
from pUtil import readpar                  # Used to read values from the schedconfig DB (queuedata)
from pUtil import PFCxml                   # Used to create XML metadata

class EventService(object):

    # private data members
    __experiment = "generic"               # String defining the experiment
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object
    __eventRangeID = ""
    __startEvent = None
    __lastEvent = None
    __lfn = None
    __guid = None
    __attemptNr = None
    __eventCounter = 0                     # Main event counter
    __eventCounterStepLength = 1           # Event counter step length (if 1, event range is trivial [i, i], if 5 event range is [i, i+5-1])
    
    def __init__(self, eventCounterStepLength=1):
        """ Default initialization """

        self.__eventCounter = 0
        self.__eventCounterStepLength = eventCounterStepLength

    def getEventService(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def processEvents(self):
        """ Process events from Event Server """

        # In development: the idea is that the pilot will not process events from staged-in input files,
        # but download events from an Event Server and then process them.
        # This method is used to process events downloaded from an Event Server

        import time
        time.sleep(4)

    def updatePandaServer(self):
        """ Send an update to the Dispatcher """

        status = False

        return status

    def getEvents(self, job):
        """ Use the event ranges from the Dispatcher (PanDA Server) and download the events from the Event Server """

        # Note: the event ranges is specified in the initial PanDA job downloaded by the pilot 

        status = False

        # First extract the current event ranges from the job object (list format: [start_event_ID, end_event_ID])
        event_range = self.extractEventRange(job)

        # Get the actual events from the Event Server
        if status:
            status = self.getEventsFromEventServer(event_range=event_range, PanDAID=job.jobId)

        return status

    def getEventsFromEventServer(event_range=[], PanDAID=""):
        """ Get new events from the Event Server within the current event range """

        status = False

        # ..

        return status

    def getEventRangeID(self):
        """ Return the event range ID """

        return self.__eventRangeID

    def getStartEvent(self):
        """ Return start event """

        return self.__startEvent

    def getLastEvent(self):
        """ Return last event """

        return self.__lastEvent

    def getLFNs(self):
        """ Return the list of LFNs """

        return self.__lfn

    def getGUIDs(self):
        """ Return the list of GUIDs """

        return self.__guid

    def getAttemptNr(self):
        """ Return the list of attempt numbers """

        return self.__attemptNr

    def extractEventRange(self, job):
        """ Return a dictionary with event ranges from the Event Server """

        event_range = []

        # Create a list of event numbers ranging from the start event to the last event
#        for i in range(job.lastEvent - job.startEvent + 1):
#            event_range.append(job.startEvent + long(i))

        return event_range

#    def createPFC4TRF(self, pfc_name, guidfname):
#        """ Create PFC to be used by trf/runAthena """
#
#        # First create a SURL based PFC then convert to TURL based?
#
#        tolog("Creating %s" % (pfc_name))
#
#        # get the PFC from the proper source
#        ec, pilotErrorDiag, _xml_from_PFC, _xml_source, replicas_dic, surl_filetype_dictionary, copytool_dictionary = \
#            getPoolFileCatalog(lfchost, ub, guids, lfns, pinitdir, analysisJob, tokens, workdir, dbh,\
#                                   DBReleaseIsAvailable, scope_dict, filesizeIn, checksumIn,\
#                                   sitemover, pfc_name=pfc_name, thisExperiment=thisExperiment)
#
#        return ec, pilotErrorDiag, replicas_dic

    # Optional
    def extractSetup(self, runCommand, trfName):
        """ Extract the 'source /path/asetup.sh ...' from the run command """

        # Use this method to extract the setup script from (e.g.) the run command string or otherwise define the proper setup for the Token Extractor

        cmd = ""
        # first remove the trf so that is not included
        runCommand = runCommand[:runCommand.find(trfName)]

        _cmd = re.search('(source.+\;)', runCommand)
        if _cmd:
            cmd = _cmd.group(1)

        return cmd

    # Optional
    def getEventIndexURL(self):
        """ Return the proper URL for the Event Index """

        if os.environ.has_key('EVENT_INDEX_URL'):
            url = os.environ['EVENT_INDEX_URL']
        else:
            url = "http://wn181.ific.uv.es:8080/getIndex.jsp?format=txt2&guid="
            #url = "http://atlas-service-eies.web.cern.ch/atlas-service-EIES/EI.jsp?guid="

        return url

if __name__ == "__main__":

    es = EventService(eventCounterStepLength=3)

    # Add unit tests here

    from Job import Job
    job = Job()
    job.eventService = True
#    job.startEvent = 123456L
#    job.lastEvent = 123459L
#    print es.extractEventRange(job)
