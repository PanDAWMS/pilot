# Class definition:
#   RunJobMira
#   [Add description here]
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from RunJob import RunJob
from RunJobHPC import RunJobHPC                  # Parent RunJobHPC class
from pUtil import tolog                          # Logging method that sends text to the pilot log

# Standard python modules
#import re
#import os
#import commands

class RunJobMira(RunJobHPC):

    # private data members
    __runjob = "Mira"                            # String defining the sub class
    __instance = None                            # Boolean used by subclasses to become a Singleton
#    __error = PilotErrors()                     # PilotErrors object

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobHPC, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getRunJob(self):
        """ Return a string with the experiment name """

        return self.__runjob

    def getRunJobFileName(self):
        """ Return the filename of the module """

        return super(RunJobMira, self).getRunJobFileName()

    # def argumentParser(self):  <-- see example in RunJob.py

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return False

if __name__ == "__main__":

    tolog("Starting RunJobMira")
