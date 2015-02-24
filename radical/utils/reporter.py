
__author__    = "Radical.Utils Development Team (Andre Merzky, Matteo Turilli)"
__copyright__ = "Copyright 2013, RADICAL@Rutgers"
__license__   = "MIT"


import sys


# ------------------------------------------------------------------------------
#
class Reporter (object) :

    # Define terminal colors for the reporter
    HEADER  = '\033[95m'
    INFO    = '\033[94m'
    OK      = '\033[92m'
    WARN    = '\033[93m'
    ERROR   = '\033[91m'
    ENDC    = '\033[0m'

    DOTTED_LINE = '........................................................................\n'
    SINGLE_LINE = '------------------------------------------------------------------------\n'
    DOUBLE_LINE = '========================================================================\n'
    HASHED_LINE = '########################################################################\n'

    # --------------------------------------------------------------------------
    #
    def __init__ (self, title=None) :

        self._title = title

        if  self._title :
            self._out (self.HEADER, "\n")
            self._out (self.HEADER, self.HASHED_LINE)
            self._out (self.HEADER, "%s\n" % title)
            self._out (self.HEADER, self.HASHED_LINE)
            self._out (self.HEADER, "\n")
    

    # --------------------------------------------------------------------------
    #
    def _out (self, color, msg) :
        sys.stdout.write (color)
        sys.stdout.write (msg)
        sys.stdout.write (self.ENDC)
    

    # --------------------------------------------------------------------------
    #
    def header (self, msg) :
        self._out (self.HEADER, "\n\n%s\n" % msg)
        self._out (self.HEADER, self.DOUBLE_LINE)


    # --------------------------------------------------------------------------
    #
    def info (self, msg) :
        self._out (self.INFO, "\n%s\n" % msg)
        self._out (self.INFO, self.SINGLE_LINE)


    # --------------------------------------------------------------------------
    #
    def ok (self, msg) :
        self._out (self.OK, "%s\n" % msg)


    # --------------------------------------------------------------------------
    #
    def warn (self, msg) :
        self._out (self.WARN, "%s\n" % msg)


    # --------------------------------------------------------------------------
    #
    def error (self, msg) :
        self._out (self.ERROR, "%s\n" % msg)


# ------------------------------------------------------------------------------

if __name__ == "__main__":

    r = Reporter (title='test')

    r.header ('header')
    r.info   ('info  ')
    r.ok     ('ok    ')
    r.warn   ('warn  ')
    r.error  ('error ')


