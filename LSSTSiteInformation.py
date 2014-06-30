# Class definition:
#   LSSTSiteInformation
#   This class is the prototype of a site information class inheriting from SiteInformation
#   Instances are generated with SiteInformationFactory via pUtil::getSiteInformation()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
from SiteInformation import SiteInformation  # Main site information class
from pUtil import tolog                      # Logging method that sends text to the pilot log
from pUtil import readpar                    # Used to read values from the schedconfig DB (queuedata)
from PilotErrors import PilotErrors          # Error codes

class LSSTSiteInformation(SiteInformation):

    # private data members
    __experiment = "LSST"
    __instance = None
    __experiment_sw_dir_var = "VO_%s_SW_DIR" % (__experiment)
    __experiment_python_pilot_var = "%s_PYTHON_PILOT" % (__experiment)

    # Required methods

    def __init__(self):
        """ Default initialization """
# not needed?

        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(LSSTSiteInformation, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def isTier1(self, sitename):
        """ Is the given site a Tier-1? """

        return False

    def isTier2(self, sitename):
        """ Is the given site a Tier-2? """

        return False

    def isTier3(self):
        """ Is the given site a Tier-3? """

        return False



    def extractAppdir(self, appdir, processingType, homePackage):
        """ extract and (re-)confirm appdir from possibly encoded schedconfig.appdir """
        # e.g. for CERN:
        # processingType = unvalid
        # schedconfig.appdir = /afs/cern.ch/atlas/software/releases|release^/afs/cern.ch/atlas/software/releases|unvalid^/afs/cern.ch/atlas/software/unvalidated/caches
        # -> appdir = /afs/cern.ch/atlas/software/unvalidated/caches
        # if processingType does not match anything, use the default first entry (/afs/cern.ch/atlas/software/releases)
        # NOTE: this function can only be called after a job has been downloaded since processType is unknown until then

        ec = 0

        _appdir = appdir
        if "|" in _appdir and "^" in _appdir:
            # extract appdir by matching with processingType
            appdir_split = _appdir.split("|")
            appdir_default = appdir_split[0]
            # loop over all possible appdirs
            sub_appdir = ""
            for i in range(1, len(appdir_split)):
                # extract the processingType and sub appdir
                sub_appdir_split = appdir_split[i].split("^")
                if processingType == sub_appdir_split[0]:
                    # found match
                    sub_appdir = sub_appdir_split[1]
                    break
            if sub_appdir == "":
                _appdir = appdir_default
                tolog("Using default appdir: %s (processingType = \'%s\')" % (_appdir, processingType))
            else:
                _appdir = sub_appdir
                tolog("Matched processingType %s to appdir %s" % (processingType, _appdir))
        else:
            # check for empty appdir's on LCG
            if _appdir == "":
                if os.environ.has_key(self.__experiment_sw_dir_var):
                    _appdir = os.environ[self.__experiment_sw_dir_var]
                    tolog("Set site.appdir to %s" % (_appdir))
            else:
                tolog("Got plain appdir: %s" % (_appdir))

        # verify the existence of appdir
        if os.path.exists(_appdir):
            tolog("Software directory %s exists" % (_appdir))

            # force queuedata update
            _ec = self.replaceQueuedataField("appdir", _appdir)
            del _ec
        else:
            if _appdir != "":
                tolog("!!FAILED!!1999!! Software directory does not exist: %s" % (_appdir))
            else:
                tolog("!!FAILED!!1999!! Software directory (appdir) is not set")
            ec = self.__error.ERR_NOSOFTWAREDIR

        return ec, _appdir



#    def readpar(self, par, alt=False):
#        """ Read parameter variable from queuedata """
#
#    def getQueuedataFileName(self, useExtension=None, check=True, alt=False):
#        """ Define the queuedata filename """
#
#    def verifyQueuedata(self, queuename, filename, _i, _N, url):
#        """ Verify the consistency of the queuedata """
#
#    def getQueuedata(self, queuename, forceDownload=False, alt=False, url=""):
#        """ Download the queuedata if not already downloaded """
#
#    def postProcessQueuedata(self, queuename, pshttpurl, thisSite, _jobrec, force_devpilot):
#        """ Update queuedata fields if necessary """
#
#    def verifySoftwareDirectory(self):
#        """ Should the software directory (schedconfig.appdir) be verified? """
#
#    def allowAlternativeStageOut(self):
#        """ Is alternative stage-out allowed? """
#
#    def getProperPaths(self, error, analyJob, token, prodSourceLabel, dsname, filename, **pdict):
#        """ Return proper paths for the storage element used during stage-out """
#

if __name__ == "__main__":
    a = LSSTSiteInformation()
    tolog("Experiment: %s" % (a.getExperiment()))

#    os.environ['PilotHomeDir'] = os.getcwd()
#
#    si = ATLASSiteInformation()
#    tolog("Experiment: %s" % (si.getExperiment()))
#
#    cloud = "CERN"
#    queuename = si.getTier1Queue(cloud)
#    if queuename != "":
#        tolog("Cloud %s has Tier-1 queue %s" % (cloud, queuename))
#    else:
#        tolog("Failed to find a Tier-1 queue name for cloud %s" % (cloud))


