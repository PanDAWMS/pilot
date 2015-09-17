1# Class definition:
#   ATLASSiteInformation
#   This class is the ATLAS site information class inheriting from SiteInformation
#   Instances are generated with SiteInformationFactory via pUtil::getSiteInformation()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
import os
import sys, httplib, cgi, urllib
import commands
import SiteMover
from SiteInformation import SiteInformation  # Main site information class
from pUtil import tolog                      # Logging method that sends text to the pilot log
from pUtil import readpar                    # Used to read values from the schedconfig DB (queuedata)
from pUtil import timedCommand               # Used by executeBenchmark
from FileHandling import getExtension        # Used to determine file type of Tier-1 info file
from FileHandling import getJSONDictionary   # Used by executeBenchmark
from PilotErrors import PilotErrors          # Error codes


class ATLASSiteInformation(SiteInformation):

    # private data members
    __experiment = "ATLAS"
    __instance = None
    __error = PilotErrors()                  # PilotErrors object
    __securityKeys = {}

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

    def isTier1(self, sitename):
        """ Is the given site a Tier-1? """
        # E.g. on a Tier-1 site, the alternative stage-out algorithm should not be used
        # Note: sitename is PanDA sitename, not DQ2 sitename

        status = False

        for cloud in self.getCloudList():
            if sitename in self.getTier1List(cloud):
                status = True
                break
        return status

    def isTier2(self, sitename):
        """ Is the given site a Tier-2? """
        # Logic: it is a T2 if it is not a T1 or a T3

        return (not (self.isTier1(sitename) or self.isTier3()))

    def isTier3(self):
        """ Is the given site a Tier-3? """
        # Note: defined by DB

        if readpar('ddm') == "local":
            status = True
        else:
            status = False

        return status

    def getCloudList(self):
        """ Return a list of all clouds """

        tier1 = self.setTier1Info()
        return tier1.keys()

    def setTier1Info(self):
        """ Set the Tier-1 information """

        tier1 = {"CA": ["TRIUMF", ""],
                 "CERN": ["CERN-PROD", ""],
                 "DE": ["FZK-LCG2", ""],
                 "ES": ["pic", ""],
                 "FR": ["IN2P3-CC", ""],
                 "IT": ["INFN-T1", ""],
                 "ND": ["ARC", ""],
                 "NL": ["SARA-MATRIX", ""],
                 "OSG": ["BNL_CVMFS_1", ""],
                 "RU": ["RRC-KI-T1", ""],
                 "TW": ["Taiwan-LCG2", ""],
                 "UK": ["RAL-LCG2", ""],
                 "US": ["BNL_PROD", "BNL_PROD-condor"]
                 }
        return tier1

    def getTier1Name(self, cloud):
        """ Return the the site name of the Tier 1 """

        return self.getTier1List(cloud)[0]

    def getTier1List(self, cloud):
        """ Return a Tier 1 site/queue list """
        # Cloud : PanDA site, queue

        tier1 = self.setTier1Info()
        return tier1[cloud]

    def getTier1InfoFilename(self):
        """ Get the Tier-1 info file name """

        filename = "Tier-1_info.%s" % (getExtension())
        path = "%s/%s" % (os.environ['PilotHomeDir'], filename)

        return path

    def downloadTier1Info(self):
        """ Download the Tier-1 info file """

        ec = 0

        path = self.getTier1InfoFilename()
        filename = os.path.basename(path)
        dummy, extension = os.path.splitext(filename)

        # url = "http://adc-ssb.cern.ch/SITE_EXCLUSION/%s" % (filename)
        if extension == ".json":
            _cmd = "?json"
#            _cmd = "?json&preset=ssbpilot"
        else:
            _cmd = "?preset=ssbpilot"
        url = "http://atlas-agis-api.cern.ch/request/site/query/list/%s" % (_cmd)
        cmd = 'curl --connect-timeout 20 --max-time 120 -sS "%s" > %s' % (url, path)

        if os.path.exists(path):
            tolog("File %s already available" % (path))
        else:
            tolog("Will download file: %s" % (filename))

            try:
                tolog("Executing command: %s" % (cmd))
                ret, output = commands.getstatusoutput(cmd)
            except Exception, e:
                tolog("!!WARNING!!1992!! Could not download file: %s" % (e))
                ec = -1
            else:
                tolog("Done")

        return ec

    def getTier1Queue(self, cloud):
        """ Download the queuedata for the Tier-1 in the corresponding cloud and get the queue name """

        # Download the entire set of queuedata
        all_queuedata_dict = self.getAllQueuedata()

        # Get the name of the Tier 1 for the relevant cloud, e.g. "BNL_PROD"
        pandaSiteID = self.getTier1Name(cloud)

        # Return the name of corresponding Tier 1 queue, e.g. "BNL_PROD-condor"
        return self.getTier1Queuename(pandaSiteID, all_queuedata_dict)

    def getTier1Queue2(self, cloud):
        """ Download the queuedata for the Tier-1 in the corresponding cloud and get the queue name """

        queuename = ""

        path = self.getTier1InfoFilename()
        ec = self.downloadTier1Info()
        if ec == 0:
            # Process the downloaded T-1 info
            f = open(path, 'r')
            if getExtension() == "json":
                from json import loads
                data = loads(f.read())
            else:
                from pickle import load
                data = load(f)
            f.close()

            # Extract the relevant queue info for the given cloud
            T1_info = [x for x in data if x['cloud']==cloud]

            # finally get the queue name
            if T1_info != []:
                info = T1_info[0]
                if info.has_key('PanDAQueue'):
                    queuename = info['PanDAQueue']
                else:
                    tolog("!!WARNING!!1222!! Returned Tier-1 info object does not have key PanDAQueue: %s" % str(info))
            else:
                tolog("!!WARNING!!1223!! Found no Tier-1 info for cloud %s" % (cloud))

        return queuename

    def getAllQueuedataFilename(self):
        """ Get the file name for the entire schedconfig dump """

        return os.path.join(os.getcwd(), "queuenames.json")

    def downloadAllQueuenames(self):
        """ Download the entire schedconfig from AGIS """

        ec = 0

        # Do not even bother to download anything if JSON is not supported
        try:
            from json import load
        except:
            tolog("!!WARNING!!1231!! JSON is not available, cannot download schedconfig dump")
            ec = -1
        else:
            # url = "http://atlas-agis-api-dev.cern.ch/request/pandaqueue/query/list/?json"
            url = "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&tier_level=1&type=production"
            schedconfig_dump = self.getAllQueuedataFilename()
            cmd = "curl \'%s\' >%s" % (url, schedconfig_dump)

            if os.path.exists(schedconfig_dump):
                tolog("File %s already downloaded" % (schedconfig_dump))
            else:
                tolog("Executing command: %s" % (cmd))
                ec, out = commands.getstatusoutput(cmd)
                if ec != 0:
                    tolog("!!WARNING!!1234!! Failed to download %s: %d, %s" % (schedconfig_dump, ec, out))
                else:
                    tolog("Downloaded schedconfig dump")

        return ec

    def getAllQueuedata(self):
        """ Get the dictionary containing all the queuedata (for all sites) """

        all_queuedata_dict = {}

        # Download the entire schedconfig
        ec = self.downloadAllQueuenames()
        if ec == 0:
            # Parse the schedconfig dump
            schedconfig_dump = self.getAllQueuedataFilename()
            try:
                f = open(schedconfig_dump)
            except Exception, e:
                tolog("!!WARNING!!1001!! Could not open file: %s, %s" % (schedconfig_dump, e))
            else:
                # Note: json is required since the queuedata dump is only available in json format
                from json import load

                # Load the dictionary
                all_queuedata_dict = load(f)

                # Done with the file
                f.close()

        return all_queuedata_dict

    def getTier1Queuename(self, pandaSiteID, all_queuedata_dict):
        """ Find the T-1 queuename from the schedconfig dump """

        t1_queuename = ""

        # Loop over all schedconfig entries
        for queuename in all_queuedata_dict.keys():
            if all_queuedata_dict[queuename].has_key("panda_resource"):
                if all_queuedata_dict[queuename]["panda_resource"] == pandaSiteID:
                    t1_queuename = queuename
                    break

        return t1_queuename

    def allowAlternativeStageOut(self, flag=False):
        """ Is alternative stage-out allowed? """
        # E.g. if stage-out to primary SE (at Tier-2) fails repeatedly, is it allowed to attempt stage-out to secondary SE (at Tier-1)?
        # For ATLAS, flag=isAnalysisJob(). Alt stage-out is currently disabled for user jobs, so do not allow alt stage-out to be forced.

        if "allow_alt_stageout" in readpar('catchall') and not flag:
            status = True
        else:
            status = False

#        if enableT1stageout.lower() == "true" or enableT1stageout.lower() == "retry":
#            status = True
#        else:
#            status = False

        return status

    def forceAlternativeStageOut(self, flag=False):
        """ Force stage-out to use alternative SE """
        # See allowAlternativeStageOut()
        # For ATLAS, flag=isAnalysisJob(). Alt stage-out is currently disabled for user jobs, so do not allow alt stage-out to be forced.

        tolog("ATLAS")
        if "force_alt_stageout" in readpar('catchall') and not flag:
            status = True
        else:
            status = False

        return status

    def getProperPaths(self, error, analyJob, token, prodSourceLabel, dsname, filename, **pdict): # in current implementation this function completely depends on site mover so that it needs to be either reimplmented or completely moved outside SiteInformation
        """ Get proper paths (SURL and LFC paths) """

        ec = 0
        pilotErrorDiag = ""
        tracer_error = ""
        dst_gpfn = ""
        lfcdir = ""
        surl = ""

        alt = pdict.get('alt', False)
        scope = pdict.get('scope', None)

        # Get the proper endpoint
        # sitemover = SiteMover.SiteMover()
        sitemover = pdict.get('sitemover', SiteMover.SiteMover()) # quick workaround HACK: to be properly implemented later

        se = sitemover.getProperSE(token, alt=alt)

        # For production jobs, the SE path is stored in seprodpath
        # For analysis jobs, the SE path is stored in sepath

        destination = sitemover.getPreDestination(analyJob, token, prodSourceLabel, alt=alt)
        if destination == '':
            pilotErrorDiag = "put_data destination path in SE not defined"
            tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
            tracer_error = 'PUT_DEST_PATH_UNDEF'
            ec = error.ERR_STAGEOUTFAILED
            return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl
        else:
            tolog("Going to store job output at: %s" % (destination))
            # /dpm/grid.sinica.edu.tw/home/atlas/atlasscratchdisk/

            # rucio path:
            # SE + destination + SiteMover.getPathFromScope(scope,lfn)

        # Get the LFC path
        lfcpath, pilotErrorDiag = sitemover.getLFCPath(analyJob, alt=alt)
        if lfcpath == "":
            tracer_error = 'LFC_PATH_EMPTY'
            ec = error.ERR_STAGEOUTFAILED
            return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl

        tolog("LFC path = %s" % (lfcpath))
        # /grid/atlas/users/pathena

        ec, pilotErrorDiag, dst_gpfn, lfcdir = sitemover.getFinalLCGPaths(analyJob, destination, dsname, filename, lfcpath, token, prodSourceLabel, scope=scope, alt=alt)
        if ec != 0:
            tracer_error = 'UNKNOWN_DSN_FORMAT'
            return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl

        # srm://f-dpm001.grid.sinica.edu.tw:8446/srm/managerv2?SFN=/dpm/grid.sinica.edu.tw/home/atlas/atlasscratchdisk/rucio/data12_8TeV/55/bc/NTUP_SUSYSKIM.01161650._000003.root.1
        # surl = srm://f-dpm001.grid.sinica.edu.tw:8446/srm/managerv2?SFN=/dpm/grid.sinica.edu.tw/home/atlas/atlasscratchdisk/user/apetrid/0328091854/user.apetrid.0328091854.805485.lib._011669/user.apetrid.0328091854.805485.lib._011669.lib.tgz

        # Define the SURL
        if "/rucio" in destination:
            surl = sitemover.getFullPath(scope, token, filename, analyJob, prodSourceLabel, alt=alt)
        else:
            surl = "%s%s" % (se, dst_gpfn)

            # Correct the SURL which might start with something like 'token:ATLASMCTAPE:srm://srm-atlas.cern.ch:8443/srm/man/..'
            # If so, remove the space token before the srm info
            if surl.startswith('token'):
                tolog("Removing space token part from SURL")
                dummy, surl = sitemover.extractSE(surl)

        tolog("SURL = %s" % (surl))
        tolog("dst_gpfn = %s" % (dst_gpfn))
        tolog("lfcdir = %s" % (lfcdir))

        return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl

    def verifyRucioPath(self, spath, seprodpath='seprodpath'):
        """ Make sure that the rucio path in se[prod]path is correctly formatted """

        # A correctly formatted rucio se[prod]path should end with /rucio
        if "rucio" in spath:
            if spath.endswith('rucio'):
                if spath.endswith('/rucio'):
                    tolog("Confirmed correctly formatted rucio %s" % (seprodpath))
                else:
                    tolog("!!WARNING!!1234!! rucio path in %s is not correctly formatted: %s" % (seprodpath, spath))
                    spath = spath.replace('rucio','/rucio')
                    ec = self.replaceQueuedataField(seprodpath, spath)
                    tolog("Updated %s to: %s" % (seprodpath, spath))
            elif spath.endswith('rucio/'):
                tolog("!!WARNING!!1234!! rucio path in %s is not correctly formatted: %s" % (seprodpath, spath))
                if spath.endswith('/rucio/'):
                    spath = spath.replace('rucio/','rucio')
                else:
                    spath = spath.replace('rucio/','/rucio')
                ec = self.replaceQueuedataField(seprodpath, spath)
                tolog("Updated %s to: %s" % (seprodpath, spath))

    def postProcessQueuedata(self, queuename, pshttpurl, thisSite, _jobrec, force_devpilot):
        """ Update queuedata fields if necessary """

        if 'pandadev' in pshttpurl or force_devpilot or thisSite.sitename == "CERNVM":
            ec = self.replaceQueuedataField("status", "online")

#        if thisSite.sitename == "RAL-LCG2_SL6":
#            ec = self.replaceQueuedataField("catchall", "log_to_objectstore")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw.usatlas.bnl.gov:8443//atlas_eventservice|logs^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs|http^http://cephgw02.usatlas.bnl.gov:8443//atlas_logs")
#            ec = self.replaceQueuedataField("copytoolin", "fax")

#            ec = self.replaceQueuedataField("objectstore", "root://atlas-objectstore.cern.ch/|eventservice^/atlas/eventservice|logs^/atlas/logs")
#            ec = self.replaceQueuedataField("catchall", "log_to_objectstore")

#        if thisSite.sitename == "GoeGrid":
#            ec = self.replaceQueuedataField("catchall", "allow_alt_stageout")
#            ec = self.replaceQueuedataField("catchall", "force_alt_stageout allow_alt_stageout")

#        if thisSite.sitename == "ANALY_CERN_SLC6":
#            ec = self.replaceQueuedataField("catchall", "stdout_to_text_indexer")
#        if thisSite.sitename == "ANALY_CERN_SLC6":
#            ec = self.replaceQueuedataField("copysetupin", "/cvmfs/atlas.cern.ch/repo/sw/local/xrootdsetup.sh^False^True")

#        ec = self.replaceQueuedataField("timefloor", "0")

#        if thisSite.sitename == "CERN-PROD_MCORE":
#            ec = self.replaceQueuedataField("appdir", "/cvmfs/atlas.cern.ch/repo/sw|nightlies^/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^root://atlas-objectstore.cern.ch//atlas/eventservice|logs^root://atlas-objectstore.cern.ch//atlas/logs|https^https://atlas-objectstore.cern.ch:1094//atlas/logs")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cs3.cern.ch:443//atlas_eventservice|logs^s3://cs3.cern.ch:443//atlas_logs")
#            ec = self.replaceQueuedataField("catchall", "log_to_objectstore")

        if thisSite.sitename == "UTA_PAUL_TEST" or thisSite.sitename == "ANALY_UTA_PAUL_TEST":
            ec = self.replaceQueuedataField("status", "online")
            ec = self.replaceQueuedataField("direct_access_lan", "True")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^root://atlas-objectstore.cern.ch//atlas/eventservice|logs^root://xrados.cern.ch//atlas/logs")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/eventservice|logs^s3://cephgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs|https^s3://cephgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("objectstore", "s3://cephgw.usatlas.bnl.gov:8443/|eventservice^/atlas_pilot_bucket/eventservice|logs^/atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw.usatlas.bnl.gov:8443//atlas_pilot_bucket/eventservice|logs^s3://cephgw.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs|http^http://cephgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs")
            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw.usatlas.bnl.gov:8443//atlas_eventservice|logs^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs|http^http://cephgw02.usatlas.bnl.gov:8443//atlas_logs")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw.usatlas.bnl.gov:8443//atlas_eventservice|logs^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs|https^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs")
            ec = self.replaceQueuedataField("catchall", "log_to_objectstore stdout_to_text_indexer")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://atlasgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/eventservice|logs^s3://atlasgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs|https^s3://atlasgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^root://atlas-objectstore.cern.ch//atlas/eventservice|logs^root://atlas-objectstore.cern.ch//atlas/logs|https^https://atlas-objectstore.cern.ch:1094//atlas/logs")
#            ec = self.replaceQueuedataField("objectstore", "root://atlas-objectstore.cern.ch/|eventservice^/atlas/eventservice|logs^/atlas/logs")
            #ec = self.replaceQueuedataField("retry", "False")
            ec = self.replaceQueuedataField("allowfax", "True")
            ec = self.replaceQueuedataField("timefloor", "0")
            ec = self.replaceQueuedataField("copytoolin", "lsm")
#            ec = self.replaceQueuedataField("copytool", "lsm")
#            ec = self.replaceQueuedataField("catchall", "stdout_to_text_indexer")
            ec = self.replaceQueuedataField("faxredirector", "atlas-xrd-us.usatlas.org:1094/")
#            ec = self.replaceQueuedataField("faxredirector", "root://glrd.usatlas.org/")
#            ec = self.replaceQueuedataField("copyprefixin", "srm://gk05.swt2.uta.edu^gsiftp://gk01.swt2.uta.edu")
#            ec = self.replaceQueuedataField("copyprefixin", "^srm://gk05.swt2.uta.edu")
# Event Service tests:
# now set in AGIS   ec = self.replaceQueuedataField("copyprefixin", "srm://gk05.swt2.uta.edu^root://xrdb.local:1094")
#            ec = self.replaceQueuedataField("corecount", "4")
            ec = self.replaceQueuedataField("appdir", "/cvmfs/atlas.cern.ch/repo/sw|nightlies^/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies")

        if os.environ.get("COPYTOOL"):
            ec = self.replaceQueuedataField("copytool", os.environ.get("COPYTOOL"))
        if os.environ.get("COPYTOOLIN"):
            ec = self.replaceQueuedataField("copytoolin", os.environ.get("COPYTOOLIN"))

#        if thisSite.sitename == "MWT2_SL6":
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw01.usatlas.bnl.gov:8443//atlas_pilot_bucket/eventservice|logs^s3://cephgw01.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs|https^https://cephgw01.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("timefloor", "0")

#        if thisSite.sitename == "CERN_PROD_MCORE":
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw.usatlas.bnl.gov:8443//atlas_eventservice|logs^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs|https^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs")
#            ec = self.replaceQueuedataField("timefloor", "0")

#        if thisSite.sitename == "MWT2_MCORE":
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw.usatlas.bnl.gov:8443//atlas_eventservice|logs^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs|https^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs")
#            ec = self.replaceQueuedataField("copyprefixin", "srm://uct2-dc1.uchicago.edu^srm://uct2-dc1.uchicago.edu")
#            ec = self.replaceQueuedataField("timefloor", "0")

#        if thisSite.sitename == "BNL_PROD_MCORE":
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw.usatlas.bnl.gov:8443//atlas_eventservice|logs^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs|https^s3://cephgw.usatlas.bnl.gov:8443//atlas_logs")
#            ec = self.replaceQueuedataField("catchall", "log_to_objectstore stdout_to_text_indexer")
#            ec = self.replaceQueuedataField("timefloor", "0")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/eventservice|logs^s3://cephgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs|https^s3://cephgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://ceph003.usatlas.bnl.gov:8443//atlas/eventservice|logs^s3://ceph003.usatlas.bnl.gov:8443//atlas/logs|https^https://ceph007.usatlas.bnl.gov:8443//atlas/logs")
#            ec = self.replaceQueuedataField("copyprefixin", "srm://dcsrm.usatlas.bnl.gov^root://dcdcap01.usatlas.bnl.gov:1094")
#            ec = self.replaceQueuedataField("appdir", "/cvmfs/atlas.cern.ch/repo/sw|nightlies^/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies")

#        if thisSite.sitename == "SWT2_CPB" or thisSite.sitename == "AGLT2_SL6":
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://cephgw.usatlas.bnl.gov:8443//atlas_pilot_bucket/eventservice|logs^s3://cephgw.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs|https^s3://cephgw.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("catchall", "log_to_objectstore,stdout_to_text_indexer")
#            ec = self.replaceQueuedataField("copyprefixin", "srm://uct2-dc1.uchicago.edu,root://xrddoor.mwt2.org:1096")
#            ec = self.replaceQueuedataField("objectstore", "eventservice^s3://atlasgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/eventservice|logs^s3://atlasgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs|https^s3://atlasgw02.usatlas.bnl.gov:8443//atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("timefloor", "0")

#        if thisSite.sitename == 'BNL_EC2E1':
#            ec = self.replaceQueuedataField("objectstore", "s3://cephgw.usatlas.bnl.gov:8443/|eventservice^/atlas_pilot_bucket/eventservice|logs^/atlas_pilot_bucket/logs")
            #ec = self.replaceQueuedataField("copyprefixin", "srm://dcsrm.usatlas.bnl.gov/pnfs/usatlas.bnl.gov/BNLT0D1/rucio^s3://ceph003.usatlas.bnl.gov:8443//atlas/eventservice")
            #ec = self.replaceQueuedataField("copyprefix", "srm://dcsrm.usatlas.bnl.gov/pnfs/usatlas.bnl.gov/BNLT0D1/rucio^s3://ceph003.usatlas.bnl.gov:8443//atlas/eventservice")
#            ec = self.replaceQueuedataField("copyprefixin", "srm://dcsrm.usatlas.bnl.gov^s3://s3.amazonaws.com:80//atlas_pilot_bucket/eventservice")
#            ec = self.replaceQueuedataField("copyprefix", "srm://dcsrm.usatlas.bnl.gov:8443^s3://s3.amazonaws.com:80//atlas_pilot_bucket/eventservice")
#            ec = self.replaceQueuedataField("copyprefixin", "srm://aws01.racf.bnl.gov/mnt/atlasdatadisk,srm://aws01.racf.bnl.gov/mnt/atlasuserdisk,srm://aws01.racf.bnl.gov/mnt/atlasproddisk^s3://s3.amazonaws.com:80//s3-atlasdatadisk-racf,s3://s3.amazonaws.com:80//s3-atlasuserdisk-racf,s3://s3.amazonaws.com:80//s3-atlasproddisk-racf")
#            ec = self.replaceQueuedataField("copyprefix", "srm://aws01.racf.bnl.gov/mnt/atlasdatadisk,srm://aws01.racf.bnl.gov/mnt/atlasuserdisk,srm://aws01.racf.bnl.gov/mnt/atlasproddisk^s3://s3.amazonaws.com:80//s3-atlasdatadisk-racf,s3://s3.amazonaws.com:80//s3-atlasuserdisk-racf,s3://s3.amazonaws.com:80//s3-atlasproddisk-racf")
#            ec = self.replaceQueuedataField("se", "token:ATLASPRODDISK:srm://aws01.racf.bnl.gov:8443/srm/managerv2?SFN=")
#            ec = self.replaceQueuedataField("seprodpath", "/mnt/atlasproddisk/rucio,/mnt/atlasdatadisk/rucio,/mnt/atlasuserdisk/rucio")
#            ec = self.replaceQueuedataField("setokens", "ATLASPRODDISK,ATLASDATADISK,ATLASUSERDISK")

#            ec = self.replaceQueuedataField("copytoolin", "S3")
#            ec = self.replaceQueuedataField("copytool", "S3")
#        if  thisSite.sitename == "NERSC_Edison":
#            ec = self.replaceQueuedataField("copytool", "gfal-copy")
#            ec = self.replaceQueuedataField("copytoolin", "gfal-copy")
#            ec = self.replaceQueuedataField("appdir", "/cvmfs/atlas.cern.ch/repo/sw|nightlies^/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies")
            #ec = self.replaceQueuedataField("appdir", "/global/homes/w/wguan/software/Athena")

            # Edison
            #ec = self.replaceQueuedataField("appdir", "/project/projectdirs/atlas/software")
#            ec = self.replaceQueuedataField("appdir", "/scratch1/scratchdirs/tsulaia/sw/software")
            ##ec = self.replaceQueuedataField("envsetup", "source /global/homes/w/wguan/software/emi/current/setup.sh")
#            ec = self.replaceQueuedataField("envsetup", "source /global/project/projectdirs/atlas/pilot/grid_env/emi/current/setup.sh")
#            ec = self.replaceQueuedataField("copysetup", "/global/project/projectdirs/atlas/pilot/grid_env/emi/current/setup.sh")
#            ec = self.replaceQueuedataField("copysetup", "/global/project/projectdirs/atlas/pilot/grid_env/setup.sh")
#            ec = self.replaceQueuedataField("objectstore", "s3://s3.amazonaws.com:80/|eventservice^/atlas_pilot_bucket/eventservice|logs^/atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("objectstore", "s3://cephgw.usatlas.bnl.gov:8443/|eventservice^/atlas_pilot_bucket/eventservice|logs^/atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("timefloor", "0")
#            ec = self.replaceQueuedataField("coreCount", "0")
#            ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore")
            #ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore,mode=backfill,queue=debug,backfill_queue=regular,max_events=2100,initialtime_m=8,time_per_event_m=10,repo=m2015,nodes=26,min_nodes=2,max_nodes=50,partition=edison,min_walltime_m=28,walltime_m=30,max_walltime_m=30,cpu_per_node=24,mppnppn=1,ATHENA_PROC_NUMBER=23")
#            ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore,mode=normal,queue=regular,backfill_queue=regular,max_events=2000,initialtime_m=8,time_per_event_m=10,repo=m2015,nodes=50,min_nodes=45,max_nodes=50,partition=edison,min_walltime_m=118,walltime_m=120,max_walltime_m=120,cpu_per_node=24,mppnppn=1,ATHENA_PROC_NUMBER=23")
#            ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore,mode=normal,queue=regular,backfill_queue=regular,max_events=20000,initialtime_m=13,time_per_event_m=13,repo=m2015,nodes=20,min_nodes=20,max_nodes=60,partition=edison,min_walltime_m=178,walltime_m=180,max_walltime_m=180,cpu_per_node=24,mppnppn=1,ATHENA_PROC_NUMBER=23")
            #ec = self.replaceQueuedataField("catchall", "HPC_HPC,mode=backfill,backfill_queue=regular")
#            ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore,mode=normal,queue=regular,backfill_queue=regular,max_events=200000,initialtime_m=13,time_per_event_m=13,repo=m1523,nodes=50,min_nodes=20,max_nodes=600,partition=edison,min_walltime_m=58,walltime_m=60,max_walltime_m=180,cpu_per_node=24,mppnppn=1,ATHENA_PROC_NUMBER=24,stageout_threads=8,copy_input_files=false")
#            ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore,mode=normal,queue=regular,backfill_queue=regular,max_events=2000,initialtime_m=13,time_per_event_m=13,repo=m670,nodes=11,min_nodes=10,max_nodes=20,partition=edison,min_walltime_m=60,walltime_m=90,max_walltime_m=185,cpu_per_node=24,mppnppn=1,ATHENA_PROC_NUMBER=24,stageout_threads=8,copy_input_files=false")
            #backfill
            #ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore,mode=backfill,queue=regular,backfill_queue=regular,max_events=2000,initialtime_m=13,time_per_event_m=13,repo=m670,nodes=13,min_nodes=8,max_nodes=15,partition=edison,min_walltime_m=60,walltime_m=180,max_walltime_m=240,cpu_per_node=24,mppnppn=1,ATHENA_PROC_NUMBER=24,stageout_threads=8,copy_input_files=false")
            #debug
            #ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore,mode=normal,queue=debug,backfill_queue=regular,max_events=200000,initialtime_m=13,time_per_event_m=13,repo=m1523,nodes=2,min_nodes=2,max_nodes=3,partition=edison,min_walltime_m=28,walltime_m=30,max_walltime_m=30,cpu_per_node=24,mppnppn=1,ATHENA_PROC_NUMBER=24,stageout_threads=8,copy_input_files=false")

#        if  thisSite.sitename == "NERSC_Hopper":
#            ec = self.replaceQueuedataField("copytool", "gfal-copy")
#            ec = self.replaceQueuedataField("copytoolin", "gfal-copy")
#            ec = self.replaceQueuedataField("appdir", "/cvmfs/atlas.cern.ch/repo/sw|nightlies^/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies")
            #ec = self.replaceQueuedataField("appdir", "/global/homes/w/wguan/software/Athena")

            # Hopper
#            ec = self.replaceQueuedataField("appdir", "/scratch/scratchdirs/tsulaia/software")
            ##ec = self.replaceQueuedataField("envsetup", "source /global/homes/w/wguan/software/emi/current/setup.sh")
#            ec = self.replaceQueuedataField("envsetup", "source /global/project/projectdirs/atlas/pilot/grid_env/emi/current/setup.sh")
#            ec = self.replaceQueuedataField("copysetup", "/global/project/projectdirs/atlas/pilot/grid_env/emi/current/setup.sh")
#            ec = self.replaceQueuedataField("copysetup", "/global/project/projectdirs/atlas/pilot/grid_env/setup.sh")
            #ec = self.replaceQueuedataField("objectstore", "s3://s3.amazonaws.com:80/|eventservice^/atlas_pilot_bucket/eventservice|logs^/atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("objectstore", "s3://cephgw02.usatlas.bnl.gov:8443/|eventservice^/atlas_pilot_bucket/eventservice|logs^/atlas_pilot_bucket/logs")
#            ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore")
#            ec = self.replaceQueuedataField("catchall", "HPC_HPC,log_to_objectstore,queue=debug,max_events=2000,initialtime_m=8, time_per_event_m=10,repo=m2015,nodes=2,min_nodes=2,max_nodes=3,partition=hopper,min_walltime_m=28,walltime_m=30,max_walltime_m=30,cpu_per_node=24,mppnppn=1,ATHENA_PROC_NUMBER=23")

        _status = self.readpar('status')
        if _status != None and _status != "":
            if _status.upper() == "OFFLINE":
                tolog("Site %s is currently in %s mode - aborting pilot" % (thisSite.sitename, _status.lower()))
                return -1, None, None
            else:
                tolog("Site %s is currently in %s mode" % (thisSite.sitename, _status.lower()))

        # Override pilot run options
        temp_jobrec = self.readpar('retry')
        if temp_jobrec.upper() == "TRUE":
            tolog("Job recovery turned on")
            _jobrec = True
        elif temp_jobrec.upper() == "FALSE":
            tolog("Job recovery turned off")
            _jobrec = False
        else:
            tolog("Job recovery variable (retry) not set")

        # Make sure that se[prod]path does not contain a malformed /rucio string (rucio/)
        # if so, correct it
        self.verifyRucioPath(readpar('sepath'), seprodpath='sepath')
        self.verifyRucioPath(readpar('seprodpath'), seprodpath='seprodpath')

        # Evaluate the queuedata if needed
        self.evaluateQueuedata()

        # Set pilot variables in case they have not been set by the pilot launcher
        thisSite = self.setUnsetVars(thisSite)

        return 0, thisSite, _jobrec

    def getQueuedata(self, queuename, forceDownload=False, alt=False, url='http://pandaserver.cern.ch'):
        """ Download the queuedata if not already downloaded """

        ec = 0
        hasQueuedata = False

        if queuename != "":
            ec, hasQueuedata = super(ATLASSiteInformation, self).getQueuedata(queuename, forceDownload=forceDownload, alt=alt, url=url)
            if ec != 0:
                tolog("!!FAILED!!1999!! getQueuedata failed: %d" % (ec))
                ec = self.__error.ERR_QUEUEDATA
            if not hasQueuedata:
                tolog("!!FAILED!!1999!! Found no valid queuedata - aborting pilot")
                ec = self.__error.ERR_QUEUEDATANOTOK
            else:
                tolog("curl command returned valid queuedata")
        else:
            tolog("WARNING: queuename not set (queuedata will not be downloaded and symbols not evaluated)")

        return ec, hasQueuedata

    def getSpecialAppdir(self, value):
        """ Get a special appdir depending on whether env variable 'value' exists """

        ec = 0
        _appdir = ""

        # does the directory exist?
        if os.environ.has_key(value):
            # expand the value in case it contains further environmental variables
            _appdir = os.path.expandvars(os.environ[value])
            tolog("Environment has variable $%s = %s" % (value, _appdir))
            if _appdir == "":
                tolog("!!WARNING!!2999!! Environmental variable not set: %s" % (value))
                ec = self.__error.ERR_SETUPFAILURE
            else:
                # store the evaluated symbol in appdir
                if self.replaceQueuedataField('appdir', _appdir, verbose=False):
                    tolog("Updated field %s in queuedata: %s" % ('appdir', _appdir))
                else:
                    tolog("!!WARNING!!2222!! Queuedata field could not be updated, cannot continue")
                    ec = self.__error.ERR_SETUPFAILURE
        else:
            tolog("!!WARNING!!2220!! Environmental variable %s is not defined" % (value))

        return ec, _appdir

    def extractAppdir(self, appdir, processingType, homePackage):
        """ extract and (re-)confirm appdir from possibly encoded schedconfig.appdir """
        # e.g. for CERN:
        # processingType = unvalid
        # schedconfig.appdir = /afs/cern.ch/atlas/software/releases|release^/afs/cern.ch/atlas/software/releases|unvalid^/afs/cern.ch/atlas/software/unvalidated/caches
        # -> appdir = /afs/cern.ch/atlas/software/unvalidated/caches
        # if processingType does not match anything, use the default first entry (/afs/cern.ch/atlas/software/releases)
        # NOTE: this function can only be called after a job has been downloaded since processType is unknown until then

        ec = 0

        tolog("Extracting appdir (ATLAS: current value=%s)" % (appdir))

        # override processingType for analysis jobs that use nightlies
        if "rel_" in homePackage:
            tolog("Temporarily modifying processingType from %s to nightlies" % (processingType))
            processingType = "nightlies"

            value = 'VO_ATLAS_NIGHTLIES_DIR'
            if os.environ.has_key(value):
                ec, _appdir = self.getSpecialAppdir(value)
                if ec == 0 and _appdir != "":
                    return ec, _appdir

#        elif "AtlasP1HLT" in homePackage or "AtlasHLT" in homePackage:
#            value = 'VO_ATLAS_RELEASE_DIR'
#            tolog("Encountered HLT homepackage: %s, will look for a set $%s" % (homePackage, value))
#
#            # does a HLT directory exist?
#            if os.environ.has_key(value):
#                ec, _appdir = self.getSpecialAppdir(value)
#                if ec == 0 and _appdir != "":
#                    return ec, _appdir
#            else:
#                tolog('$%s is not set' % (value))

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
                if os.environ.has_key("VO_ATLAS_SW_DIR"):
                    _appdir = os.environ["VO_ATLAS_SW_DIR"]
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

    def getFileSystemRootPath(self):
        """ Return the root path of the local file system """

        # Returns "/cvmfs" or "/(some path)/cvmfs" in case the expected file system root path is not
        # where it usually is (e.g. on an HPC). See example implementation in self.getLocalROOTSetup()

        return os.environ.get('ATLAS_SW_BASE', '/cvmfs')

    def getLocalROOTSetup(self):
        """ Build command to prepend the xrdcp command [xrdcp will in general not be known in a given site] """

        # cmd = 'export ATLAS_LOCAL_ROOT_BASE=%s/atlas.cern.ch/repo/ATLASLocalRootBase; ' % (self.getFileSystemRootPath())
        # cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet; '
        # cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalROOTSetup.sh --rootVersion ${rootVersionVal} --skipConfirm; '
        cmd = 'source %s/atlas.cern.ch/repo/sw/local/xrootdsetup.sh' % (self.getFileSystemRootPath())

        return cmd

    def getLocalEMISetup(self):
        """ Return the path for the local EMI setup """

        cmd = 'export ATLAS_LOCAL_ROOT_BASE=%s/atlas.cern.ch/repo/ATLASLocalRootBase; ' % (self.getFileSystemRootPath())
        cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet; '
        cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalEmiSetup.sh --force'
        #cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/x86_64/emi/current/setup.sh'

        return cmd

    # Required for S3 objectstore
    def getSecurityKey(self, privateKeyName, publicKeyName):
        """ Return the key pair """

        keyName = privateKeyName + "_" + publicKeyName
        if keyName in self.__securityKeys.keys():
            return self.__securityKeys[keyName]
        else:
            try:
                #import environment
                #env = environment.set_environment()

                sslCert = self.getSSLCertificate()
                sslKey = sslCert

                node={}
                node['privateKeyName'] = privateKeyName
                node['publicKeyName'] = publicKeyName
                #host = '%s:%s' % (env['pshttpurl'], str(env['psport'])) # The key pair is not set on other panda server
                host = 'pandaserver.cern.ch:25443'
                path = '/server/panda/getKeyPair'

                conn = httplib.HTTPSConnection(host, key_file=sslKey, cert_file=sslCert, timeout=120)
                conn.request('POST', path, urllib.urlencode(node))

                resp = conn.getresponse()
                data = resp.read()
                conn.close()
                dic = cgi.parse_qs(data)
                if dic["StatusCode"][0] == "0":
                    self.__securityKeys[keyName] = {"publicKey": dic["publicKey"][0], "privateKey": dic["privateKey"][0]}
                    return self.__securityKeys[keyName]
                else:
                   tolog("!!WARNING!!4444!! Failed to get key from PanDA server:")
                   tolog("data = %s" % str(data))
            except:
                _type, value, traceBack = sys.exc_info()
                tolog("!!WARNING!!4445!! Failed to getKeyPair for (%s, %s)" % (privateKeyName, publicKeyName))
                tolog("ERROR: %s %s" % (_type, value))

        return {"publicKey": None, "privateKey": None}

    # Optional
    def shouldExecuteBenchmark(self):
        """ Should the pilot execute a benchmark test before asking server for a job? """

        return False

    # Optional
    def executeBenchmark(self):
        """ Interface method for benchmark test """

        # Use this method to interface with benchmark code
        # The method should return a dictionary containing the results of the test

        dictionary = {}
        cmd = "/afs/cern.ch/user/w/walkerr/public/kflops.sh"
        timeout = 120

        tolog("Executing benchmark test: %s" % (cmd))
        exitcode, output = timedCommand(cmd, timeout=timeout)
        if exitcode != 0:
            tolog("!!WARNING!!3434!! Encountered a problem with benchmark test: %s" % (output))
        else:
            tolog("Benchmark finished")

            filename = "kflops.json"
            if not os.path.exists(filename):
                tolog("!!WARNING!!3435!! Benchmark did not produce expected output file: %s" % (filename))
            else:
                tolog("Parsing benchmark output file: %s" % (filename))
                dictionary = getJSONDictionary(filename)
                if dictionary == {}:
                    tolog("!!WARNING!!3436!! Empty benchmark dictionary - nothing to report")

        return dictionary

if __name__ == "__main__":

    os.environ['PilotHomeDir'] = os.getcwd()

    si = ATLASSiteInformation()
    tolog("Experiment: %s" % (si.getExperiment()))

    cloud = "CERN"
    queuename = si.getTier1Queue(cloud)
    if queuename != "":
        tolog("Cloud %s has Tier-1 queue %s" % (cloud, queuename))
    else:
        tolog("Failed to find a Tier-1 queue name for cloud %s" % (cloud))

    keyPair = si.getSecurityKey('BNL_ObjectStoreKey', 'BNL_ObjectStoreKey.pub')
    print keyPair
