#!/usr/bin/env python

"""Module to acccess the my_proxy and glexec services.

The MyProxyInterface class allows creating interfaces for my_proxy. The class
GlexecInterface allows the context switch to run a payload sandboxed.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import Configuration
import pUtil

# Eddie
import re
import stat

from Monitor import Monitor
import environment
from SiteInformation import SiteInformation

try:
    import simplejson as json
except ImportError:
    try:
	import json
    except ImportError:
	json = None

if json is not None:
    import CustomEncoder
else:
    CustomEncoder = None


def execute(program):
    """Run a program on the command line. Return stderr, stdout and status."""
    pUtil.tolog("executable: %s" % program)
    pipe = subprocess.Popen(program, bufsize=-1, shell=True, close_fds=False,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pipe.communicate()
    return stdout, stderr, pipe.wait()


# TODO(rmedrano): These kind of generic exceptions must be refactored.
class GlexecException(Exception):
    """Exception to raise when interactions with glexec failed."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        return self.message


class MyProxyInterface(object):
    """Wrapper around proxy retrieving mechanism from panda-server."""

    def __init__(self, user_proxy):
        self.user_proxy = user_proxy

    def retrieve(self, proxy_path):
        """Retrieve proxy from var and store it."""
	#pUtil.tolog('self.user_proxy is %s ' %self.user_proxy)

	if self.user_proxy == '':
		pUtil.tolog('!!WARNING!! We did NOT get any proxy from panda_server! We will use the one that started the pilot!!!!!!!!!!!!!!!!!!!!!!!')
		shutil.copy(os.environ['X509_USER_PROXY'], proxy_path)
		pUtil.tolog('copied original pilot proxy in %s' %proxy_path)
		os.chmod(proxy_path, 0700)
	else:
		text_file = open(proxy_path, "w")
		text_file.write(self.user_proxy)
		text_file.close()
		pUtil.tolog('wrote retrieved proxy file to %s' %proxy_path)
		os.chmod(proxy_path, 0700)
        #pUtil.tolog('File permissions are.. %s ' % oct(stat.S_IMODE(os.stat(proxy_path).st_mode)))
        os.environ['GLEXEC_SOURCE_PROXY'] = proxy_path
	os.environ['GLEXEC_CLIENT_CERT'] = proxy_path
	#pUtil.tolog('added GLEXEC_SOURCE_PROXY to that file')
        #pUtil.tolog('added GLEXEC_CLIENT_CERT to that file')

	status = 0
	#pUtil.tolog(status)
        return status


class GlexecInterface(object):
    """Wrapper around glexec. Uses glexec and wrap, unwrap and mkgltempdir."""

    def __init__(self, my_proxy_interface, payload='run.sh'):
        """Initialize the wrapper with an optional payload.

        Payload is the final end-user command to be executed.
        It may contain also input options, for example:
        payload='athena --indataset=blah --outdataset=blah ...'
        """
        self.my_proxy_interface = my_proxy_interface
        self.payload = payload
        self.output = None
        self.error = None
        self.status = None
        self.__glexec_path = None
        self.__wrapper_path = None
        self.__unwrapper_path = None
        self.__mkgltempdir_path = None
        self.__target_path = None
        self.__source_path = os.getcwd()
        self.__new_proxy_path = None
        self.__retrieve_proxy()
	self.__actual_workdir = None

    @property
    def sandbox_path(self):
        """Get the path of the glexec sandbox for the job."""
        return self.__target_path

    def setup_and_run(self):
        """Prepare the environment and execute glexec."""
        self.__set_glexec_paths()
        self.__extend_pythonpath()
        self.__mk_gl_temp_dir()
        self.__set_glexec_env_vars()
        self.__ship_queue_data()
        self.__ship_job_definition()
        self.__dump_current_configuration('data-orig.json')
        self.__run_glexec()
        self.__clean()

    def __mk_gl_temp_dir(self):
        """Make the directory tree for glexec.

        See usage example in:
        http://wiki.nikhef.nl/grid/GLExec_TransientPilotJobs
        """
        #pUtil.tolog('sys path is %s' % sys.path)
        #pUtil.tolog('os environ is %s' % os.environ)

        pUtil.tolog("folder is : %s" % self.__mkgltempdir_path)
	cmd = '%s -t 777' % self.__mkgltempdir_path
	
        stdout, stderr, status = execute(cmd)
	pUtil.tolog('cmd: %s' % cmd)
        pUtil.tolog('output: %s' % stdout)
        pUtil.tolog('error: %s' % stderr)
        pUtil.tolog('status: %s' % status)
        if not (status or stderr):
            self.__target_path = stdout.rstrip('\n')
            os.environ['GLEXEC_TARGET_DIR'] = self.__target_path
            os.environ['GLEXEC_TARGET_PROXY'] = os.path.join(self.__target_path, 'user_proxy')
            pUtil.tolog("gltmpdir created and added to env: %s" % self.__target_path)
        else:
	    pUtil.tolog('error! gltmpdir has failed')
	    pUtil.tolog('sys path is %s' % sys.path)
	    pUtil.tolog('os environ is %s' % os.environ)
	    raise GlexecException("mkgltempdir failed: %s" % stderr)

    def __set_glexec_paths(self):
        """Sets the path with the glexec executable

         - In gLite sites it is  $GLITE_LOCATION/sbin/glexec
         - In OSG sites it is $OSG_GLEXEC_LOCATION/glexec
        """
        if os.environ.has_key('OSG_GLEXEC_LOCATION'):
            self.__glexec_path = os.environ['OSG_GLEXEC_LOCATION']
        elif os.environ.has_key('GLITE_LOCATION'):
            self.__glexec_path = os.path.join(os.environ['GLITE_LOCATION'],
                                             'sbin/glexec')
        elif os.environ.has_key('GLEXEC_LOCATION'):
            self.__glexec_path = os.path.join(os.environ['GLEXEC_LOCATION'],
                                             'sbin/glexec')

        self.__wrapper_path = os.path.join(os.path.dirname(self.__glexec_path),
                                          'glexec_wrapenv.pl')
        self.__unwrapper_path = os.path.join(os.path.dirname(self.__glexec_path),
                                            'glexec_unwrapenv.pl')
        # TODO(rmedrano): hack to run our version of mkgltempdir.
        self.__mkgltempdir_path = os.path.join(os.path.dirname(self.__glexec_path),
                                               'mkgltempdir')
        #self.__mkgltempdir_path = os.path.join(os.path.dirname('/home/ekaravak/'),
        #                                      'mkgltempdir')

    def __extend_pythonpath(self):
        current_path = os.getcwd()
	pUtil.tolog('extending curring path to %s ' % current_path)
        sys.path.append(current_path)
	pUtil.tolog('sys path is ... %s ' % sys.path)

    def __retrieve_proxy(self):
        """Download the proxy from myproxy.

        The retrieved credentials will be placed in a temporary directory
        as /tmp/<random_dir>/X509. We retrieve the proxy once per job and
        don't renew it.
        """
        new_proxy_dir = tempfile.mkdtemp(dir=os.getcwd())
        self.__new_proxy_path = os.path.join(new_proxy_dir, 'user_proxy')
	pUtil.tolog('new proxy path is .. %s ' % self.__new_proxy_path)
        self.my_proxy_interface.retrieve(self.__new_proxy_path)
        os.environ['GLEXEC_CLIENT_CERT'] = self.__new_proxy_path
        os.environ['GLEXEC_SOURCE_PROXY'] = self.__new_proxy_path

    def __set_glexec_env_vars(self):
        """Set the glexec environment with the downloaded proxy."""
        # TODO(rmedrano): refactor env setting here. Check dependencies.


    def __correct_env_vars(self):
        """Set the glexec environment with the downloaded proxy."""
        # TODO(rmedrano): refactor env setting here. Check dependencies.


    def __dump_current_configuration(self, file_name='data.json'):
        """Dump the current configuration in JSON format into the sandbox"""
	pUtil.tolog('dumping config file...')
	if json is None:
	    raise RuntimeError('json is not available')
        Configuration.Configuration()['SandBoxPath'] = self.sandbox_path
        configuration_path = os.path.join(self.sandbox_path, file_name)
        CustomEncoder.ConfigurationSerializer.serialize_file(Configuration.Configuration(), configuration_path)
        os.chmod(configuration_path, 0644)
	pUtil.tolog('dumped config file at %s' %configuration_path)

    def __ship_queue_data(self):
        """Ships the queue data to the sandbox environment."""
	# Will now try to find if we have the json or dat extension and then copy it to the sandbox
	si = SiteInformation()
	queuedatafile = si.getQueuedataFileName()
	if '.dat' in queuedatafile:
		shutil.copy2(queuedatafile, os.path.join(self.sandbox_path, 'queuedata.dat'))
        	os.chmod(os.path.join(self.sandbox_path, 'queuedata.dat'), 0666)
	else:
		shutil.copy2(queuedatafile, os.path.join(self.sandbox_path, 'queuedata.json'))
                os.chmod(os.path.join(self.sandbox_path, 'queuedata.json'), 0666)

	# Eddie
	shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'glexec_aux.py'),
                     os.path.join(self.sandbox_path, 'glexec_aux.py'))
        os.chmod(os.path.join(self.sandbox_path, 'glexec_aux.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'Monitor.py'),
                     os.path.join(self.sandbox_path, 'Monitor.py'))
        os.chmod(os.path.join(self.sandbox_path, 'Monitor.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'pUtil.py'),
                     os.path.join(self.sandbox_path, 'pUtil.py'))
        os.chmod(os.path.join(self.sandbox_path, 'pUtil.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'environment.py'),
                     os.path.join(self.sandbox_path, 'environment.py'))
        os.chmod(os.path.join(self.sandbox_path, 'environment.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'Configuration.py'),
                     os.path.join(self.sandbox_path, 'Configuration.py'))
        os.chmod(os.path.join(self.sandbox_path, 'Configuration.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'JobRecovery.py'),
                     os.path.join(self.sandbox_path, 'JobRecovery.py'))
        os.chmod(os.path.join(self.sandbox_path, 'JobRecovery.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'PilotErrors.py'),
                     os.path.join(self.sandbox_path, 'PilotErrors.py'))
        os.chmod(os.path.join(self.sandbox_path, 'PilotErrors.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'JobState.py'),
                     os.path.join(self.sandbox_path, 'JobState.py'))
        os.chmod(os.path.join(self.sandbox_path, 'JobState.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'FileState.py'),
                     os.path.join(self.sandbox_path, 'FileState.py'))
        os.chmod(os.path.join(self.sandbox_path, 'FileState.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'processes.py'),
                     os.path.join(self.sandbox_path, 'processes.py'))
        os.chmod(os.path.join(self.sandbox_path, 'processes.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'FileStateClient.py'),
                     os.path.join(self.sandbox_path, 'FileStateClient.py'))
        os.chmod(os.path.join(self.sandbox_path, 'FileStateClient.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'WatchDog.py'),
                     os.path.join(self.sandbox_path, 'WatchDog.py'))
        os.chmod(os.path.join(self.sandbox_path, 'WatchDog.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'PilotTCPServer.py'),
                     os.path.join(self.sandbox_path, 'PilotTCPServer.py'))
        os.chmod(os.path.join(self.sandbox_path, 'PilotTCPServer.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'UpdateHandler.py'),
                     os.path.join(self.sandbox_path, 'UpdateHandler.py'))
        os.chmod(os.path.join(self.sandbox_path, 'UpdateHandler.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'CustomEncoder.py'),
                     os.path.join(self.sandbox_path, 'CustomEncoder.py'))
        os.chmod(os.path.join(self.sandbox_path, 'CustomEncoder.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'ATLASSiteInformation.py'),
                     os.path.join(self.sandbox_path, 'ATLASSiteInformation.py'))
        os.chmod(os.path.join(self.sandbox_path, 'ATLASSiteInformation.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'SiteMover.py'),
                     os.path.join(self.sandbox_path, 'SiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'SiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'futil.py'),
                     os.path.join(self.sandbox_path, 'futil.py'))
        os.chmod(os.path.join(self.sandbox_path, 'futil.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'timed_command.py'),
                     os.path.join(self.sandbox_path, 'timed_command.py'))
        os.chmod(os.path.join(self.sandbox_path, 'timed_command.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'config.py'),
                     os.path.join(self.sandbox_path, 'config.py'))
        os.chmod(os.path.join(self.sandbox_path, 'config.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'SiteInformation.py'),
                     os.path.join(self.sandbox_path, 'SiteInformation.py'))
        os.chmod(os.path.join(self.sandbox_path, 'SiteInformation.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'Job.py'),
                     os.path.join(self.sandbox_path, 'Job.py'))
        os.chmod(os.path.join(self.sandbox_path, 'Job.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'Node.py'),
                     os.path.join(self.sandbox_path, 'Node.py'))
        os.chmod(os.path.join(self.sandbox_path, 'Node.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'Site.py'),
                     os.path.join(self.sandbox_path, 'Site.py'))
        os.chmod(os.path.join(self.sandbox_path, 'Site.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'ExperimentFactory.py'),
                     os.path.join(self.sandbox_path, 'ExperimentFactory.py'))
        os.chmod(os.path.join(self.sandbox_path, 'ExperimentFactory.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'Experiment.py'),
                     os.path.join(self.sandbox_path, 'Experiment.py'))
        os.chmod(os.path.join(self.sandbox_path, 'Experiment.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'ATLASExperiment.py'),
                     os.path.join(self.sandbox_path, 'ATLASExperiment.py'))
        os.chmod(os.path.join(self.sandbox_path, 'ATLASExperiment.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'RunJobUtilities.py'),
                     os.path.join(self.sandbox_path, 'RunJobUtilities.py'))
        os.chmod(os.path.join(self.sandbox_path, 'RunJobUtilities.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'OtherExperiment.py'),
                     os.path.join(self.sandbox_path, 'OtherExperiment.py'))
        os.chmod(os.path.join(self.sandbox_path, 'OtherExperiment.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'NordugridATLASExperiment.py'),
                     os.path.join(self.sandbox_path, 'NordugridATLASExperiment.py'))
        os.chmod(os.path.join(self.sandbox_path, 'NordugridATLASExperiment.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'PandaServerClient.py'),
                     os.path.join(self.sandbox_path, 'PandaServerClient.py'))
        os.chmod(os.path.join(self.sandbox_path, 'PandaServerClient.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'JobLog.py'),
                     os.path.join(self.sandbox_path, 'JobLog.py'))
        os.chmod(os.path.join(self.sandbox_path, 'JobLog.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'Mover.py'),
                     os.path.join(self.sandbox_path, 'Mover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'Mover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'SiteMoverFarm.py'),
                     os.path.join(self.sandbox_path, 'SiteMoverFarm.py'))
        os.chmod(os.path.join(self.sandbox_path, 'SiteMoverFarm.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'dCacheSiteMover.py'),
                     os.path.join(self.sandbox_path, 'dCacheSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'dCacheSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'BNLdCacheSiteMover.py'),
                     os.path.join(self.sandbox_path, 'BNLdCacheSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'BNLdCacheSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'xrootdSiteMover.py'),
                     os.path.join(self.sandbox_path, 'xrootdSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'xrootdSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'xrdcpSiteMover.py'),
                     os.path.join(self.sandbox_path, 'xrdcpSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'xrdcpSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'CastorSiteMover.py'),
                     os.path.join(self.sandbox_path, 'CastorSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'CastorSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'dCacheLFCSiteMover.py'),
                     os.path.join(self.sandbox_path, 'dCacheLFCSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'dCacheLFCSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'lcgcpSiteMover.py'),
                     os.path.join(self.sandbox_path, 'lcgcpSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'lcgcpSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'lcgcp2SiteMover.py'),
                     os.path.join(self.sandbox_path, 'lcgcp2SiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'lcgcp2SiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'stormSiteMover.py'),
                     os.path.join(self.sandbox_path, 'stormSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'stormSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'mvSiteMover.py'),
                     os.path.join(self.sandbox_path, 'mvSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'mvSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'HUSiteMover.py'),
                     os.path.join(self.sandbox_path, 'HUSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'HUSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'rfcpLFCSiteMover.py'),
                     os.path.join(self.sandbox_path, 'rfcpLFCSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'rfcpLFCSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'castorSvcClassSiteMover.py'),
                     os.path.join(self.sandbox_path, 'castorSvcClassSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'castorSvcClassSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'LocalSiteMover.py'),
                     os.path.join(self.sandbox_path, 'LocalSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'LocalSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'ChirpSiteMover.py'),
                     os.path.join(self.sandbox_path, 'ChirpSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'ChirpSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'curlSiteMover.py'),
                     os.path.join(self.sandbox_path, 'curlSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'curlSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'FAXSiteMover.py'),
                     os.path.join(self.sandbox_path, 'FAXSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'FAXSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'aria2cSiteMover.py'),
                     os.path.join(self.sandbox_path, 'aria2cSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'aria2cSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'SiteInformationFactory.py'),
                     os.path.join(self.sandbox_path, 'SiteInformationFactory.py'))
        os.chmod(os.path.join(self.sandbox_path, 'SiteInformationFactory.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'CMSSiteInformation.py'),
                     os.path.join(self.sandbox_path, 'CMSSiteInformation.py'))
        os.chmod(os.path.join(self.sandbox_path, 'CMSSiteInformation.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'NordugridATLASSiteInformation.py'),
                     os.path.join(self.sandbox_path, 'NordugridATLASSiteInformation.py'))
        os.chmod(os.path.join(self.sandbox_path, 'NordugridATLASSiteInformation.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'OtherSiteInformation.py'),
                     os.path.join(self.sandbox_path, 'OtherSiteInformation.py'))
        os.chmod(os.path.join(self.sandbox_path, 'OtherSiteInformation.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'RunJob.py'),
                     os.path.join(self.sandbox_path, 'RunJob.py'))
        os.chmod(os.path.join(self.sandbox_path, 'RunJob.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'RunJobEvent.py'),
                     os.path.join(self.sandbox_path, 'RunJobEvent.py'))
        os.chmod(os.path.join(self.sandbox_path, 'RunJobEvent.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'RunJobHPC.py'),
                     os.path.join(self.sandbox_path, 'RunJobHPC.py'))
        os.chmod(os.path.join(self.sandbox_path, 'RunJobHPC.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'RunJobTitan.py'),
                     os.path.join(self.sandbox_path, 'RunJobTitan.py'))
        os.chmod(os.path.join(self.sandbox_path, 'RunJobTitan.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'RunJobMira.py'),
                     os.path.join(self.sandbox_path, 'RunJobMira.py'))
        os.chmod(os.path.join(self.sandbox_path, 'RunJobMira.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'ErrorDiagnosis.py'),
                     os.path.join(self.sandbox_path, 'ErrorDiagnosis.py'))
        os.chmod(os.path.join(self.sandbox_path, 'ErrorDiagnosis.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'Diagnosis.py'),
                     os.path.join(self.sandbox_path, 'Diagnosis.py'))
        os.chmod(os.path.join(self.sandbox_path, 'Diagnosis.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'ProxyGuard.py'),
                     os.path.join(self.sandbox_path, 'ProxyGuard.py'))
        os.chmod(os.path.join(self.sandbox_path, 'ProxyGuard.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'DBReleaseHandler.py'),
                     os.path.join(self.sandbox_path, 'DBReleaseHandler.py'))
        os.chmod(os.path.join(self.sandbox_path, 'DBReleaseHandler.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'VmPeak.py'),
                     os.path.join(self.sandbox_path, 'VmPeak.py'))
        os.chmod(os.path.join(self.sandbox_path, 'VmPeak.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'JobInfoXML.py'),
                     os.path.join(self.sandbox_path, 'JobInfoXML.py'))
        os.chmod(os.path.join(self.sandbox_path, 'JobInfoXML.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'Cleaner.py'),
                     os.path.join(self.sandbox_path, 'Cleaner.py'))
        os.chmod(os.path.join(self.sandbox_path, 'Cleaner.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'CMSExperiment.py'),
                     os.path.join(self.sandbox_path, 'CMSExperiment.py'))
        os.chmod(os.path.join(self.sandbox_path, 'CMSExperiment.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'dataPilot.py'),
                     os.path.join(self.sandbox_path, 'dataPilot.py'))
        os.chmod(os.path.join(self.sandbox_path, 'dataPilot.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'OtherSiteMover.py'),
                     os.path.join(self.sandbox_path, 'OtherSiteMover.py'))
        os.chmod(os.path.join(self.sandbox_path, 'OtherSiteMover.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'trivialPilot.py'),
                     os.path.join(self.sandbox_path, 'trivialPilot.py'))
        os.chmod(os.path.join(self.sandbox_path, 'trivialPilot.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'globusPilot.py'),
                     os.path.join(self.sandbox_path, 'globusPilot.py'))
        os.chmod(os.path.join(self.sandbox_path, 'globusPilot.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], '__init__.py'),
                     os.path.join(self.sandbox_path, '__init__.py'))
        os.chmod(os.path.join(self.sandbox_path, '__init__.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'JEMstub.py'),
                     os.path.join(self.sandbox_path, 'JEMstub.py'))
        os.chmod(os.path.join(self.sandbox_path, 'JEMstub.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'glexec_utils.py'),
                     os.path.join(self.sandbox_path, 'glexec_utils.py'))
        os.chmod(os.path.join(self.sandbox_path, 'glexec_utils.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'myproxyUtils.py'),
                     os.path.join(self.sandbox_path, 'myproxyUtils.py'))
        os.chmod(os.path.join(self.sandbox_path, 'myproxyUtils.py'), 0666)
        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'PilotUtils.py'),
                     os.path.join(self.sandbox_path, 'PilotUtils.py'))
        os.chmod(os.path.join(self.sandbox_path, 'PilotUtils.py'), 0666)

        shutil.copy2(os.path.join(os.environ['PilotHomeDir'], 'PILOTVERSION'),
                     os.path.join(self.sandbox_path, 'PILOTVERSION'))
        os.chmod(os.path.join(self.sandbox_path, 'PILOTVERSION'), 0666)



    def __ship_job_definition(self):
        """Ships the job definition to the sandbox."""
        execute('cp ./Job_*.py %s' % self.sandbox_path)

    def __run_glexec(self):
        """Start the sandboxed process for the job.

        See usage examples in:
        http://wiki.nikhef.nl/grid/GLExec_Environment_Wrap_and_Unwrap_scripts
        """
	pUtil.tolog('correcting env vars before running glexec!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
	# Store actual workdir in order to restore it later on
        env = Configuration.Configuration()
	self.__actual_workdir = env['workdir']
	env['workdir'] = self.sandbox_path + '/output'
        if not os.path.exists(env['workdir']):
                os.makedirs(env['workdir'])
                os.chmod(env['workdir'],0777)
	env['thisSite'].wntmpdir = env['workdir']
	env['PilotHomeDir'] = self.sandbox_path
        env['inputDir'] = self.sandbox_path
        env['outputDir'] = self.sandbox_path
        env['pilot_initdir'] = self.sandbox_path
	pUtil.tolog('Pilot home dir is %s '%env['PilotHomeDir'])
        self.__site_workdir = re.split('Panda_Pilot',env['thisSite'].workdir)
        env['thisSite'].workdir = env['workdir'] + '/Panda_Pilot' + self.__site_workdir[1]
	env['job'].datadir = env['thisSite'].workdir + '/PandaJob_' + str(env['job'].jobId) + '_data'
        if not os.path.exists(env['thisSite'].workdir):
        	os.makedirs(env['thisSite'].workdir)
		os.chmod(env['thisSite'].workdir,0777)
	self.__dump_current_configuration()
        
	pUtil.tolog("cding and running GLEXEC!")
        cmd = "export GLEXEC_ENV=`%s`; \
               %s %s -- 'cd %s; \
               %s 2>1;'" % (self.__wrapper_path,
                             self.__glexec_path,
                             self.__unwrapper_path,
                             self.__target_path,
                             self.payload)
        self.output, self.error, self.status = execute(cmd)
        pUtil.tolog(self.error)
        pUtil.tolog(self.output)

    def __clean(self):
        """Remove the previously created proxy and reload configuration."""
	if json is None:
	    raise RuntimeError('json is not available')
        try:
            # Reloads configuration from the subprocess.
            configuration_path = os.path.join(self.sandbox_path, 'data-orig.json')
            CustomEncoder.ConfigurationSerializer.deserialize_file(configuration_path)
	    pUtil.tolog('glexec client cert is %s ' % os.environ['GLEXEC_CLIENT_CERT'])
            pUtil.tolog('glexec source proxy is %s ' % os.environ['GLEXEC_SOURCE_PROXY'])
            pUtil.tolog('glexec target dir is %s ' % os.environ['GLEXEC_TARGET_DIR'])
            pUtil.tolog('glexec target proxy is %s ' % os.environ['GLEXEC_TARGET_PROXY'])

	    pUtil.tolog('Removing GLEXEC env vars')
	    del os.environ['GLEXEC_CLIENT_CERT']
            del os.environ['GLEXEC_SOURCE_PROXY']
            del os.environ['GLEXEC_TARGET_DIR']
            del os.environ['GLEXEC_TARGET_PROXY']
            #pUtil.tolog('Removing user_proxy')
            #os.remove(self.__new_proxy_path)
            #pUtil.tolog('Proxy removed!')
	    pUtil.tolog('sys path is ... %s ' % sys.path)
	    pUtil.tolog('Will now check current dir and remove it from sys.path %s ' % os.getcwd())
	    sys.path.remove(os.getcwd())	
            pUtil.tolog('sys path now is ... %s ' % sys.path)
	    #pUtil.tolog('x509_user_proxy is %s' % os.environ['X509_USER_PROXY'])
	except:
	    pUtil.tolog('Failed to reload previous configuration.')
