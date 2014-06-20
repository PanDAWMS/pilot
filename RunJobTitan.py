# Class definition:
#   RunJobTitan
#   [Add description here]
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from RunJobHPC import RunJobHPC                  # Parent RunJob class
from pUtil import tolog                         # Logging method that sends text to the pilot log
def tolog(s): print s

# Standard python modules
#import re
#import os
#import commands

class RunJobTitan(RunJobHPC):

    # private data members
    __runjob = "Titan"                          # String defining the sub class
    __instance = None                           # Boolean used by subclasses to become a Singleton
#    __error = PilotErrors()                    # PilotErrors object

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobTitan, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getRunJob(self):
        """ Return a string with the experiment name """

        return self.__runjob

    def getRunJobFileName(self):
        """ Return the filename of the module """

        return super(RunJobTitan, self).getRunJobFileName()

if __name__ == "__main__":

    tolog("Starting RunJobTitan")
