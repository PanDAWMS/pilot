# Class definition:
#   CMSSiteInformation
#   This class is the prototype of a site information class inheriting from SiteInformation
#   Instances are generated with SiteInformationFactory via pUtil::getSiteInformation()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
import os
import re
import pickle
import commands
import shlex
import getopt

from SiteInformation import SiteInformation  # Main site information class
from pUtil import tolog                      # Logging method that sends text to the pilot log
from pUtil import readpar                    # Used to read values from the schedconfig DB (queuedata)
from PilotErrors import PilotErrors          # Error codes

from optparse import (OptionParser,BadOptionError)

class PassThroughOptionParser(OptionParser):
    """
    An unknown option pass-through implementation of OptionParser.

    When unknown arguments are encountered, bundle with largs and try again,
    until rargs is depleted.  

    sys.exit(status) will still be called if a known argument is passed
    incorrectly (e.g. missing arguments or bad argument types, etc.)        
    """
    def _process_args(self, largs, rargs, values):
        while rargs:
            try:
                OptionParser._process_args(self,largs,rargs,values)
            except (BadOptionError, Exception), e:
                #largs.append(e.opt_str)
                continue


class CMSSiteInformation(SiteInformation):

    # private data members
    __experiment = "CMS"
    __instance = None

    # Required methods

    def __init__(self):
        """ Default initialization """
# not needed?

        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(CMSSiteInformation, cls).__new__(cls, *args, **kwargs)

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

    def allowAlternativeStageOut(self):
        """ Is alternative stage-out allowed? """
        # E.g. if stage-out to primary SE (at Tier-2) fails repeatedly, is it allowed to attempt stage-out to secondary SE (at Tier-1)?

        return False

    def belongsTo(self, value, rangeStart, rangeEnd):
        if value >= rangeStart and value <= rangeEnd:
                return True, rangeEnd
        rangeStart=(rangeStart+1000)
        return False, rangeStart
    
 
    def findRange(self, job, filename):
        jobNumber = int(filename.split('_')[-2]) #int(self.extractJobPar(job, '--jobNumber'))
        filesPerJob = len(job.outFiles)

        range = ((filesPerJob)*(jobNumber+1) / 1000 +1)*1000

        if filename.split('.')[-1] != 'root' and filename.split('.')[-2] != 'root' :
            range = '%s/log' % str(range) 

        return str(range)


    def extractJobPar(self, job, par, ptype="string"):

        strpars = job.jobPars
        cmdopt = shlex.split(strpars)
        parser = PassThroughOptionParser()
        parser.add_option(par,\
                          dest='par',\
                          type=ptype)
        (options,args) = parser.parse_args(cmdopt)
        return options.par


    def getProperPaths(self, error, analyJob, token, prodSourceLabel, dsname, filename, sitename, JobData, alt=False):
        """ Called by LocalSiteMover, from put_data method, instead of using SiteMover.getProperPaths 
            needed only if LocalSiteMover is used instead of specific Mover
            serve solo se utilizziamo LocalSiteMover al posto dello specifico Mover 
   
            full lfn format:
            /store/user/<yourHyperNewsusername>/<primarydataset>/<publish_data_name>/<PSETHASH>/<NNNN>/<output_file_name>
        """
        tolog("prodSourceLabel = %s " % prodSourceLabel)
        tolog("dsname = %s " % dsname)
        tolog("filename = %s " % filename)
        tolog("sitename = %s " % sitename)
        tolog(" analyJob = %s " %  analyJob)
        tolog("token = %s " % token)
        job = None
        remoteSE = ''
        try:
            import Job 
            pkl_file = open(JobData, 'rb')
            newJobDef = pickle.load(pkl_file)
            job = newJobDef['job']
            #for attr in dir(newJobDef['job']):
            #    print "obj.%s = %s" % (attr, getattr(newJobDef['job'], attr))
            remoteSE = job.fileDestinationSE
            if remoteSE.find(','):
                remoteSE = remoteSE.split(',')[0]
            tolog("############# getProperPaths - remoteSE: %s - filename: %s" % (remoteSE, filename))
        except Exception, e:
            tolog("############# getProperPaths except!! %s !! remoteSE: %s - filename: %s" % (e, remoteSE, filename)) 
            pass

        ec = 0
        pilotErrorDiag = ""
        tracer_error = ""
        dst_gpfn = ""
        lfcdir = ""
        full_lfn = ""

        if "runGen" in newJobDef['job'].trf:
            """ case runGen 
                dsname example: vmancine/GenericTTbar/AsoTest_130403_094107-v1/USER """
            primarydataset = dsname.split('/')[1]
            publishdataset = dsname.split('/')[2].split('-v1')[0].split('_')[0]
            hnusername = dsname.split('/')[0]
            psethash = 'PSETHASH'

            rndcmd = 'den=(0 1 2 3 4 5 6 7 8 9 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z a b c d e f g h i j k l m n o p q r s t u v w x y z); nd=${#den[*]}; randj=${den[$RANDOM % $nd]}${den[$RANDOM % $nd]}${den[$RANDOM % $nd]}; echo $randj'
            rnd = commands.getoutput(rndcmd)
            if filename.split('.')[1] == 'root':
                #output file 
                newfilename = '%s_%s.%s' % (filename.split('.')[0], rnd, filename.split('.')[1])
            else:
                #log file
                newfilename = '%s%s_%s.%s' % (filename.split('.')[1], filename.split('.')[2], rnd, filename.split('.')[3])

        else:
            """ case CMSRunAnaly 
                dsname example: /RelValProdTTbar/mcinquil-test000001-psethash/USER"""
            hnusername = dsname.split('/')[2].split('-')[0]
            primarydataset = dsname.split('/')[1]
            publishdataset = '-'.join(dsname.split('/')[2].split('-')[1:-1])
            #try:
            #    psethash = dsname.split('/')[2].split('-')[2]
            #except Exception, e:
            #    psethash = 'psethash'
            psethash = dsname.split('/')[2].split('-')[-1] 

            newfilename = filename


        # Calculate value of NNNN folder 

        if job:
            try:
                nnnn = self.findRange(job, newfilename)
            except Exception, e:
                tolog('error = %s' % e)
                nnnn = 'NNNN'
        else:
            nnnn = 'NNNN'

        full_lfn_suffix = '%s/%s/%s/%s/%s/%s' % (hnusername, primarydataset, publishdataset, psethash, nnnn, newfilename)

        #here we should have a check on the async destination and the local site.
        #   if they are the same we should use full_lfn_prefix = '/store/user' otherwise
        #   we should have full_lfn_prefix = '/store/temp/user/'

        full_lfn_prefix = '/store/temp/user/'
        #if remoteSE and sitename:
        #    sitename = sitename.split('ANALY_')[1]
        #    if remoteSE == sitename:
        #        full_lfn_prefix = '/store/user/'
            
        full_lfn = '%s%s'%( full_lfn_prefix, full_lfn_suffix )

        tolog("full_lfn = %s" % full_lfn )
        tolog("dst_gpfn = %s" % (None))
        tolog("lfcdir = %s" % (None))

        return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, full_lfn

    def extractAppdir(self, appdir, processingType, homePackage):
        """ Called by pilot.py, runMain method """
        tolog("CMSExperiment - extractAppdir - nothing to do")

        return 0, ""


if __name__ == "__main__":

    a = CMSSiteInformation()
    tolog("Experiment: %s" % (a.getExperiment()))

