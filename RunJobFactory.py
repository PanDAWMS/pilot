# Class definition:
#   ExperimentFactory
#   This class is used to generate Experiment class objects corresponding to a given "experiment"
#   Based the on Factory Design Pattern
#   Note: not compatible with Singleton Design Pattern due to the subclassing

import traceback
import pUtil


from types import TypeType
from RunJob import RunJob
from RunJobEvent import RunJobEvent
from RunJobHPC import RunJobHPC
try:
    from RunJobTitan import RunJobTitan
except:
    pUtil.tolog("Failed to import RunJobTitan: %s" % traceback.format_exc())

try:
    from RunJobHopper import RunJobHopper
except:
    pUtil.tolog("Failed to import RunJobHopper: %s" % traceback.format_exc())

try:
    from RunJobEdison import RunJobEdison
except:
    pUtil.tolog("Failed to import RunJobEdison: %s" % traceback.format_exc())

try:
    from RunJobAnselm import RunJobAnselm
except:
    pUtil.tolog("Failed to import RunJobAnselm: %s" % traceback.format_exc())

try:
    from RunJobArgo import RunJobArgo
except:
    pUtil.tolog("Failed to import RunJobArgo: %s" % traceback.format_exc())

from RunJobNormal import RunJobNormal
from RunJobHpcEvent import RunJobHpcEvent
from RunJobHpcarcEvent import RunJobHpcarcEvent

class RunJobFactory(object):

    def newRunJob(self, _type="generic"):
        """ Generate a new site information object """



        # get all classes
        runJobClasses = [j for (i,j) in globals().iteritems() if isinstance(j, TypeType) and issubclass(j, RunJob)]
        
        # loop over all subclasses
        for runJobClass in runJobClasses:
            si = runJobClass()

            # return the matching RunJob class
            if si.getRunJob() == _type:
                return runJobClass

        # if no class was found, raise an error
        raise ValueError('RunJobFactory: No such class: "%s"' % (_type))

if __name__ == "__main__":

    factory = RunJobFactory()

    types = ['Normal', 'Normal2', 'RunJobEvent', 'HPC', 'Argo', 'Titan', 'Dummy']

    for t in types:
        print "\nAttempting to get class for type", t
        try:
            runJob = factory.newRunJob(t)
        except Exception, e:
            print e
        else:
            rJ = runJob()
            print 'got runJob:',rJ.getRunJob()
            print 'file name:', rJ.getRunJobFileName()
            del runJob

