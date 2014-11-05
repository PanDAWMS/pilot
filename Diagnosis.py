# Class definition:
#   Diagnosis
#
#   Note: not compatible with Singleton Design Pattern due to the subclassing

from PilotErrors import PilotErrors
from pUtil import tolog

class Diagnosis(object):

    # private data members
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    # Additional optional methods
    # ...

if __name__ == "__main__":

    print "Implement test cases here"
