# Class definition:
#   EventServiceFactory
#   This class is used to generate EventService class objects corresponding to a given "experiment"
#   Based the on Factory Design Pattern
#   Note: not compatible with Singleton Design Pattern due to the subclassing

from types import TypeType
from EventService import EventService
from ATLASEventService import ATLASEventService

class EventServiceFactory(object):

    def newEventService(self, experiment):
        """ Generate a new site information object """

        # get all classes
        eventServiceClasses = [j for (i,j) in globals().iteritems() if isinstance(j, TypeType) and issubclass(j, EventService)]
        print eventServiceClasses
        # loop over all subclasses
        for eventServiceClass in eventServiceClasses:
            si = eventServiceClass()

            # return the matching eventService class
            if si.getEventService() == experiment:
                return eventServiceClass

        # if no class was found, raise an error
        raise ValueError('EventServiceFactory: No such class: "%s"' % (eventServiceClass))

if __name__ == "__main__":

    factory = EventServiceFactory()

    print "\nAttempting to get ATLAS"
    try:
        eventServiceClass = factory.newEventService('ATLAS')
    except Exception, e:
        print e
    else:
        si = eventServiceClass()
        print 'got eventService:',si.getEventService()
        del eventServiceClass
        
    print "\nAttempting to get Dummy"
    try:
        eventServiceClass = factory.newEventService('Dummy')
    except Exception, e:
        print e
    else:
        si = eventServiceClass()
        print 'got eventService:',si.getEventService()
        del eventServiceClass
    
