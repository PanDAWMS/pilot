# Class definition:
#   SiteInformationFactory
#   This class is used to generate SiteInformation class objects corresponding to a given "experiment"
#   Based the on Factory Design Pattern
#   Note: not compatible with Singleton Design Pattern due to the subclassing

from types import TypeType
from SiteInformation import SiteInformation
from ATLASSiteInformation import ATLASSiteInformation
from AMSTaiwanSiteInformation import AMSTaiwanSiteInformation
from CMSSiteInformation import CMSSiteInformation
from NordugridATLASSiteInformation import NordugridATLASSiteInformation
from OtherSiteInformation import OtherSiteInformation

class SiteInformationFactory(object):

    def newSiteInformation(self, experiment):
        """ Generate a new site information object """

        # get all classes
        siteInformationClasses = [j for (i,j) in globals().iteritems() if isinstance(j, TypeType) and issubclass(j, SiteInformation)]

        # loop over all subclasses
        for siteInformationClass in siteInformationClasses:
            si = siteInformationClass()

            # return the matching experiment class
            if si.getExperiment() == experiment:
                return siteInformationClass

        # if no class was found, raise an error
        raise ValueError('SiteInformationFactory: No such class: "%s"' % (experiment))

if __name__ == "__main__":

    factory = SiteInformationFactory()

    print "\nAttempting to get ATLAS"
    try:
        siteInformationClass = factory.newSiteInformation('ATLAS')
    except Exception, e:
        print e
    else:
        si = siteInformationClass()
        print 'got experiment:',si.getExperiment()
        del siteInformationClass

    print "\nAttempting to get Nordugrid-ATLAS"
    try:
        from NordugridATLASSiteInformation import NordugridATLASSiteInformation
        siteInformationClass = factory.newSiteInformation('Nordugrid-ATLAS')
    except Exception, e:
        print e
    else:
        si = siteInformationClass()
        print 'got experiment:',si.getExperiment()
        del siteInformationClass

    print "\nAttempting to get CMS"
    try:
        siteInformationClass = factory.newSiteInformation('CMS')
    except Exception, e:
        print e
    else:
        si = siteInformationClass()
        print 'got experiment:',si.getExperiment()
        del siteInformationClass

    print "\nAttempting to get Other"
    try:
        siteInformationClass = factory.newSiteInformation('Other')
    except Exception, e:
        print e
    else:
        si = siteInformationClass()
        print 'got experiment:',si.getExperiment()
        del siteInformationClass
    
    print "\nAttempting to get AMSTaiwan"
    try:
        siteInformationClass = factory.newSiteInformation('AMSTaiwan')
    except Exception, e:
        print e
    else:
        si = siteInformationClass()
        print 'got experiment:',si.getExperiment()
        del siteInformationClass

    print "\nAttempting to get Dummy"
    try:
        siteInformationClass = factory.newSiteInformation('Dummy')
    except Exception, e:
        print e
    else:
        si = siteInformationClass()
        print 'got experiment:',si.getExperiment()
        del siteInformationClass
    
