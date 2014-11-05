#!/usr/bin/env python

import Monitor

import os
import sys
import pUtil
import signal
from CustomEncoder import ConfigurationSerializer
from Configuration import Configuration

import getpass #Eddie to remove

signal.signal(signal.SIGTERM, pUtil.sig2exc)
signal.signal(signal.SIGQUIT, pUtil.sig2exc)
signal.signal(signal.SIGSEGV, pUtil.sig2exc)
signal.signal(signal.SIGXCPU, pUtil.sig2exc)
signal.signal(signal.SIGBUS, pUtil.sig2exc)

CONFIGURATION = ConfigurationSerializer.deserialize_file('./data.json')


# Replace the PilotHomeDir with the glexec'ed dir in the sys.path
if os.environ['PilotHomeDir'] in sys.path:
	sys.path[sys.path.index(os.environ['PilotHomeDir'])] = str(CONFIGURATION['SandBoxPath'])

pUtil.tolog('The sys path is %s ' % sys.path)

pUtil.tolog(CONFIGURATION['thisSite'].workdir) # to correct
pUtil.tolog(CONFIGURATION['inputDir']) # to correct 
pUtil.tolog(CONFIGURATION['outputDir']) # to correct
pUtil.tolog(CONFIGURATION['pilot_initdir']) # to correct

CONFIGURATION['inputDir'] = CONFIGURATION['SandBoxPath']
CONFIGURATION['outputDir'] = CONFIGURATION['SandBoxPath']
CONFIGURATION['pilot_initdir'] = CONFIGURATION['SandBoxPath']
CONFIGURATION['thisSite'].wntmpdir = CONFIGURATION['SandBoxPath'] + '/output'
CONFIGURATION['workdir'] = CONFIGURATION['SandBoxPath'] + '/output'

os.environ['PilotHomeDir'] = CONFIGURATION['SandBoxPath'] # is this needed....? to check!
# Eddie Need to stripout unicode chars
pUtil.tolog('original log file! %s ' % CONFIGURATION['job'].logFile)
CONFIGURATION['job'].logFile = str(CONFIGURATION['job'].logFile)
pUtil.tolog('original out file! %s ' % CONFIGURATION['job'].outFiles)
CONFIGURATION['job'].outFiles = [str(item) for item in CONFIGURATION['job'].outFiles]
pUtil.tolog('new out file! %s ' % CONFIGURATION['job'].outFiles)

pUtil.writeToFile(os.path.join(CONFIGURATION['SandBoxPath'], "CURRENT_SITEWORKDIR"), CONFIGURATION['SandBoxPath'])

pUtil.tolog('starting monitor_job()')
Monitor.Monitor(CONFIGURATION).monitor_job()

pUtil.tolog('finished...')

pUtil.tolog('serialise file')
CONFIGURATION = ConfigurationSerializer.serialize_file(CONFIGURATION,
                                                       './data-output.json')

pUtil.tolog('os pilothome dir is the new or old?.. %s ' % os.environ['PilotHomeDir'])
