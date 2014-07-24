# Class definition:
#   ATLASEventService
#   This class is inheriting from the main Event Service class
#   Instances are generated with ExperimentFactory via pUtil::getEventService()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# PanDA Pilot modules
from EventService import EventService
from PilotErrors import PilotErrors
from pUtil import tolog                    # Dump to pilot log
from pUtil import readpar                  # Used to read values from the schedconfig DB (queuedata)

# Standard python modules
import os
import re
import commands

class ATLASEventService(EventService):

    # private data members
    __experiment = "ATLAS"               # String defining the experiment
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(ATLASEventService, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

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

