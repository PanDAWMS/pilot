# Class definition:
#   ExperimentFactory
#   This class is used to generate Experiment class objects corresponding to a given "experiment"
#   Based the on Factory Design Pattern
#   Note: not compatible with Singleton Design Pattern due to the subclassing

from types import TypeType
from Experiment import Experiment
from ATLASExperiment import ATLASExperiment
from CMSExperiment import CMSExperiment
from OtherExperiment import OtherExperiment
from AMSTaiwanExperiment import AMSTaiwanExperiment
from NordugridATLASExperiment import NordugridATLASExperiment

class ExperimentFactory(object):

    def newExperiment(self, experiment):
        """ Generate a new site information object """

        # get all classes
        experimentClasses = [j for (i,j) in globals().iteritems() if isinstance(j, TypeType) and issubclass(j, Experiment)]

        # loop over all subclasses
        for experimentClass in experimentClasses:
            si = experimentClass()

            # return the matching experiment class
            if si.getExperiment() == experiment:
                return experimentClass

        # if no class was found, raise an error
        raise ValueError('ExperimentFactory: No such class: "%s"' % (experiment))

if __name__ == "__main__":

    factory = ExperimentFactory()

    print "\nAttempting to get ATLAS"
    try:
        experimentClass = factory.newExperiment('ATLAS')
    except Exception, e:
        print e
    else:
        si = experimentClass()
        print 'got experiment:',si.getExperiment()
        del experimentClass
    
    print "\nAttempting to get Nordugrid-ATLAS"
    try:
        experimentClass = factory.newExperiment('Nordugrid-ATLAS')
    except Exception, e:
        print e
    else:
        si = experimentClass()
        print 'got experiment:',si.getExperiment()
        del experimentClass

    print "\nAttempting to get Other"
    try:
        experimentClass = factory.newExperiment('Other')
    except Exception, e:
        print e
    else:
        si = experimentClass()
        print 'got experiment:',si.getExperiment()
        del experimentClass
    
    print "\nAttempting to get AMSTaiwan"
    try:
        experimentClass = factory.newExperiment('AMSTaiwan')
    except Exception, e:
        print e
    else:
        si = experimentClass()
        print 'got experiment:',si.getExperiment()
        del experimentClass
    
    print "\nAttempting to get Dummy"
    try:
        experimentClass = factory.newExperiment('Dummy')
    except Exception, e:
        print e
    else:
        si = experimentClass()
        print 'got experiment:',si.getExperiment()
        del experimentClass
    
