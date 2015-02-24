# Class definition:
#   RunJobHPC
#   This class is the base class for the HPC classes. It inherits from RunJob
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from RunJob import RunJob                # Parent RunJob class
from pUtil import tolog                         # Logging method that sends text to the pilot log

# Standard python modules
#import re
#import os
#import commands

class RunJobHPC(RunJob):

    # private data members
    __runjob = "HPC"                            # String defining the sub class
    __instance = None                           # Boolean used by subclasses to become a Singleton
    
    # public data members
    cpu_number_per_node = 16                  # Number of CPU per core, used for proper requesting of resources
    partition_comp = ''                       # Name of partition for checking of available resources
    project_id = ''                           # Name of associated project on HPC, may needed for proper job declaration
    executed_queue = 'batch'                  # Name of executed queue

    walltime = 60                             # Default walltime limit (min)
    max_nodes = 100 # None                    # Upper limitation for requested resources, needed to throttle IO
    number_of_threads = 1                     # Number of threads for MPI task
    min_walltime = 50 * 60                    # Minimum walltime (minimum time limit) 
    waittime = 7 * 60                         # Waittime limit. Cancel of reschedule job after this time (sec.)
    nodes = 1                                 # Minimum number of requested nodes
     
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

        return super(RunJobHPC, self).getRunJobFileName()

    # def argumentParser(self):  <-- see example in RunJob.py

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return False

if __name__ == "__main__":

    tolog("Starting RunJobHPC")
