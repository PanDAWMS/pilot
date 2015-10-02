#

import os
import commands

from pUtil import tolog, readpar
from FileHandling import readJSON, writeJSON

def updateRedirector(redirector):
    """ Correct the redirector in case the protocol and/or trailing slash are missing """

    if not redirector.startswith("root://"):
        redirector = "root://" + redirector
        tolog("Updated redirector for missing protocol: %s" % (redirector))
    if not redirector.endswith("/"):
        redirector = redirector + "/"
        tolog("Updated redirector for missing trailing /: %s" % (redirector))

    # Protect against triple slashes
    redirector = redirector.replace('///','//')

    return redirector

def _getFAXRedirectors(computingSite, sourceSite, pandaID, url='http://waniotest.appspot.com/SiteToFaxEndpointTranslator'):
    """ Get the FAX redirectors via curl or JSON """

    fax_redirectors_dictionary = {}
    file_name = "fax_redirectors.json"
    if os.path.exists(file_name):
        # Read back the FAX redirectors from file
        fax_redirectors_dictionary = readJSON(file_name)

    if fax_redirectors_dictionary == {}:
        # Attempt to get fax redirectors from Ilija Vukotic's google server
        cmd = "curl --silent --connect-timeout 100 --max-time 120 -X POST --data \'computingsite=%s&sourcesite=%s&pandaID=%s\' %s" % (computingSite, sourceSite, pandaID, url)
        tolog("Trying to get FAX redirectors: %s" % (cmd))
        dictionary_string = commands.getoutput(cmd)
        if dictionary_string != "":
            # try to convert to a python dictionary
            from json import loads
            try:
                fax_redirectors_dictionary = loads(dictionary_string)
            except Exception, e:
                tolog("!!WARNING!!4444!! Failed to parse fax redirector json: %s" % (e))
            else:
                tolog("Backing up dictionary")
                status = writeJSON("fax_redirectors.json", fax_redirectors_dictionary)
                if not status:
                    tolog("Failed to backup the FAX redirectors")

    return fax_redirectors_dictionary

def getFAXRedirectors(computingSite, sourceSite, jobId):
    """ Get the FAX redirectors primarily from the google server, fall back to schedconfig.faxredirector value """

    fax_redirectors_dictionary = {}

    # Is the sourceSite set?
    if sourceSite and sourceSite.lower() != 'null':
        # Get the FAX redirectors (if the method returns an empty dictionary, the keys and values will be set below)
        fax_redirectors_dictionary = _getFAXRedirectors(computingSite, sourceSite, jobId)

        # Verify the dictionary
        if fax_redirectors_dictionary.has_key('computingsite') and fax_redirectors_dictionary['computingsite'] != None:
            if fax_redirectors_dictionary['computingsite'] == "" or fax_redirectors_dictionary['computingsite'].lower() == "null":
                fax_redirectors_dictionary['computingsite'] = readpar('faxredirector')
                tolog("!!WARNING!!5555!! FAX computingsite is unknown, using default AGIS value (%s)" % fax_redirectors_dictionary['computingsite'])
        else:
            fax_redirectors_dictionary['computingsite'] = readpar('faxredirector')
            tolog("!!WARNING!!5556!! FAX computingsite is unknown, using default AGIS value (%s)" % fax_redirectors_dictionary['computingsite'])
        if fax_redirectors_dictionary.has_key('sourcesite') and fax_redirectors_dictionary['sourcesite'] != None:
            if fax_redirectors_dictionary['sourcesite'] == "" or fax_redirectors_dictionary['sourcesite'].lower() == "null":
                fax_redirectors_dictionary['sourcesite'] = readpar('faxredirector')
                tolog("!!WARNING!!5555!! FAX sourcesite is unknown, using default AGIS value (%s)" % fax_redirectors_dictionary['sourcesite'])
        else:
            fax_redirectors_dictionary['sourcesite'] = readpar('faxredirector')
            tolog("!!WARNING!!5556!! FAX aourcesite is unknown, using default AGIS value (%s)" % fax_redirectors_dictionary['sourcesite'])

    else:
        tolog("sourceSite is not set, use faxredirector value from AGIS")

        _faxredirector = readpar('faxredirector')
        _faxredirector = updateRedirector(_faxredirector)
        fax_redirectors_dictionary['computingsite'] = _faxredirector
        fax_redirectors_dictionary['sourcesite'] = _faxredirector

    return fax_redirectors_dictionary
