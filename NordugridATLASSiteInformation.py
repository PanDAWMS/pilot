# Class definition:
#   NordugridATLASSiteInformation
#   This class is the Nordugrid-ATLAS site information class inheriting from ATLASSiteInformation
#   Instances are generated with SiteInformationFactory via pUtil::getSiteInformation()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
import os
import commands
import SiteMover
from SiteInformation import SiteInformation  # Main site information class
from ATLASSiteInformation import ATLASSiteInformation  # Main site information class
from pUtil import tolog                                # Logging method that sends text to the pilot log
from pUtil import readpar                              # Used to read values from the schedconfig DB (queuedata)
from PilotErrors import PilotErrors                    # Error codes

class NordugridATLASSiteInformation(ATLASSiteInformation):

    # private data members
    __experiment = "Nordugrid-ATLAS"
    __instance = None

    # Required methods

    def __init__(self):
        """ Default initialization """

        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(ATLASSiteInformation, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment


if __name__ == "__main__":

    os.environ['PilotHomeDir'] = os.getcwd()

    si = NordugridATLASSiteInformation()
    tolog("Experiment: %s" % (si.getExperiment()))

    cloud = "CERN"
    queuename = si.getTier1Queue(cloud)
    if queuename != "":
        tolog("Cloud %s has Tier-1 queue %s" % (cloud, queuename))
    else:
        tolog("Failed to find a Tier-1 queue name for cloud %s" % (cloud))
    
