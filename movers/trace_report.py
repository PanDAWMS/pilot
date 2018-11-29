"""
  Trace report implementation
  :author: Alexey Anisenkov
"""

import os
import time

import hashlib
import commands

import socket

class TraceReport(dict):

    def __init__(self, *args, **kwargs):

        defs = {
                'eventType': '',   # sitemover
                'eventVersion': 'pilot',
                'protocol': None,          # set by specific sitemover
                'clientState': 'INIT_REPORT',
                'localSite': '', # localsite
                'remoteSite': '', # equals remotesite (pilot does not do remote copy?)
                'timeStart': None, # time to start
                'catStart': None,
                'relativeStart': None,
                'transferStart': None,
                'validateStart': None,
                'timeEnd': None,
                'dataset': '',
                'version': None,
                'duid': None,
                'filename': None,
                'guid': None,
                'filesize': None,
                'usr': None,
                'appid': None,
                'hostname': '',
                'ip': '',
                'suspicious': '0',
                'usrdn': '',
                'url': None,
                'stateReason': None,
                'uuid': None,
                'taskid': ''
        }

        super(TraceReport, self).__init__(defs)
        self.update(dict(*args, **kwargs)) # apply extra input

    # sitename, dsname, eventType
    def init(self, job):

        data = {
                'clientState': 'INIT_REPORT',
                'usr': hashlib.md5(job.prodUserID).hexdigest(), # anonymise user and pilot id's
                'appid': job.jobId,
                'usrdn': job.prodUserID,
                'taskid': job.taskID
                }

        self.update(data)

        self['timeStart'] = time.time()

        try:
            self['hostname'] = socket.gethostbyaddr(socket.gethostname())[0]
        except:
            pass
        try:
            self['ip'] = socket.gethostbyname(socket.gethostname())
        except:
            pass

        if job.jobDefinitionID:
            self['uuid'] = hashlib.md5('ppilot_%s' % job.jobDefinitionID).hexdigest() # hash_pilotid
            #tolog("Using job definition id: %s" % job.jobDefinitionID)
        else:
            self['uuid'] = commands.getoutput('uuidgen -t 2> /dev/null').replace('-','') # all LFNs of one request have the same uuid

        #tolog("Tracing report initialised with: %s" % self)
