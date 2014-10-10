from commands import getstatusoutput
from optparse import OptionParser
import os

def argumentParser():
    """ """

    filename = ""
    url = ""

    parser = OptionParser()
    parser.add_option("-t", "--type", dest="type",
                      help="Build type (dev|rc)", metavar="TYPE")
    parser.add_option("-d", "--destination", dest="url",
                      help="Destination URL", metavar="URL")

    # options = {'type': 'dev'}
    (options, args) = parser.parse_args()

    if options.type == "dev" or not options.type:
        print "Building dev pilot"
        filename = "pilotcode-dev.tar.gz"
    elif options.type == "rc":
        print "Building rc pilot"
        filename = "pilotcode-rc.tar.gz"
    else:
        print "Unknown build type: %s" % options.type

    if options.url:
        url = options.url
    else:
        url = "pnilsson@lxplus:www/."

    return filename, url

def executeCommand(cmd, dump=True):
    """ """

    print "Executing command: %s" % (cmd)
    (ec, output) = getstatusoutput(cmd)
    if ec != 0:
        print "ec = %d" % (ec)
    if dump:
        print output

    return ec

(filename, url) = argumentParser()

if filename != "":
    if os.path.exists("PILOT_INITDIR"):
        ec = executeCommand("rm -f PILOT_INITDIR", dump=False)

    ec = executeCommand("rm -f *.tar.gz *.pyc", dump=False)
    if ec == 0:
        ec = executeCommand("tar cvfz %s *" % (filename), dump=False)
        if ec == 0:
            ec = executeCommand("scp %s %s" % (filename, url))
            if ec == 0:
                print "Done"
