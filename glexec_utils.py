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
import glob

# Eddie
import re
import stat
import time

from Monitor import Monitor
import environment
from SiteInformation import SiteInformation

environment.set_environment()


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
        status = self.__mk_gl_temp_dir()
	if status == 1:
		pUtil.tolog('Creating a temporary gLExec dir failed. Exiting...')
		return
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
        pUtil.tolog('sys path is %s' % sys.path)

        pUtil.tolog("folder is : %s" % self.__mkgltempdir_path)
	cmd = '%s -t 777 `pwd`' % self.__mkgltempdir_path

	attempts = 0
	while attempts < 3:
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
			pUtil.tolog("now adding sandbox to sys.path")
			sys.path.append(self.__target_path)
			pUtil.tolog("New sys.path is %s " %sys.path)
			return 0
	        else:
			pUtil.tolog('error! gltmpdir has failed')
		        attempts += 1
			#raise GlexecException("mkgltempdir failed: %s" % stderr)
			pUtil.tolog("mkgltempdir failed: %s" % stderr)
			if attempts == 3:
	                        pUtil.tolog('sys path is %s' % sys.path)
        	                pUtil.tolog('os environ is %s' % os.environ)
				ec = 1226
				env = Configuration.Configuration()

	                        pUtil.tolog("Updating PanDA server for the failed job (error code %d)" % (ec))
	                        env['job'].result[0] = 'failed'
				env['job'].currentState = env['job'].result[0]
                	        env['job'].result[2] = ec
	                        env['pilotErrorDiag'] = "gLExec related failure - %s" %stderr
				env['job'].pilotErrorDiag = env['pilotErrorDiag']

				from pilot import getProperNodeName

				if 'https://' not in env['pshttpurl']:
					env['pshttpurl'] = 'https://' + env['pshttpurl']

				import Node#, Site
			        env['workerNode'] = Node.Node()
			        env['workerNode'].setNodeName(getProperNodeName(os.uname()[1]))
				
				env['job'].workdir = os.getcwd()
				env['thisSite'].workdir = os.getcwd()

				from PandaServerClient import PandaServerClient				

				strXML = pUtil.getMetadata(env['thisSite'].workdir, env['job'].jobId)

				client = PandaServerClient(pilot_version = env['version'], pilot_version_tag = env['pilot_version_tag'],
	                               pilot_initdir = env['pilot_initdir'], jobSchedulerId = env['jobSchedulerId'],
        	                       pilotId = env['pilotId'], updateServer = env['updateServerFlag'],
                	               jobrec = env['jobrec'], pshttpurl = env['pshttpurl'])

				client.updatePandaServer(env['job'], env['thisSite'], env['workerNode'], env['psport'],
					log = env['pilotErrorDiag'], useCoPilot = env['useCoPilot'], xmlstr = strXML)

        	                return 1

			else:
				pUtil.tolog('[Trial %s] Sleeping for 10 secs and retrying' % attempts)
				time.sleep(10)

    def __set_glexec_paths(self):
        """Sets the path with the glexec executable

         - In gLite sites it is  $GLITE_LOCATION/sbin/glexec
         - In OSG sites it is $OSG_GLEXEC_LOCATION/glexec
        """

        if os.environ.has_key('OSG_GLEXEC_LOCATION'):
		if os.environ['OSG_GLEXEC_LOCATION'] != '':
                	self.__glexec_path = os.environ['OSG_GLEXEC_LOCATION']
                else:
                        pUtil.tolog('OSG_GLEXEC_LOCATION env var was set to an empty string, will force it to /usr/sbin/glexec')
                        self.__glexec_path = '/usr/sbin/glexec'
                        os.environ['OSG_GLEXEC_LOCATION'] = '/usr/sbin/glexec'
	elif os.environ.has_key('GLEXEC_LOCATION'):
        	if os.environ['GLEXEC_LOCATION'] != '':
                	self.__glexec_path = os.path.join(os.environ['GLEXEC_LOCATION'],'sbin/glexec')
                else:
                        pUtil.tolog('GLEXEC_LOCATION env var was set to an empty string, will force it to /usr/sbin/glexec')
                        self.__glexec_path = '/usr/sbin/glexec'
                        os.environ['GLEXEC_LOCATION'] = '/usr'
	elif os.path.exists('/usr/sbin/glexec'):
                pUtil.tolog('glexec is installed in the standard location. Adding the missing GLEXEC_LOCATION env var')
                self.__glexec_path = '/usr/sbin/glexec'
                os.environ['GLEXEC_LOCATION'] = '/usr'
        elif os.environ.has_key('GLITE_LOCATION'):
                self.__glexec_path = os.path.join(os.environ['GLITE_LOCATION'], 'sbin/glexec')
        else:
                pUtil.tolog("!!WARNING!! gLExec is probably not installed at the WN!")
                self.__glexec_path = '/usr/sbin/glexec'

        self.__wrapper_path = os.path.join(os.path.dirname(self.__glexec_path),
                                          'glexec_wrapenv.pl')
        self.__unwrapper_path = os.path.join(os.path.dirname(self.__glexec_path),
                                            'glexec_unwrapenv.pl')
        self.__mkgltempdir_path = os.path.join(os.path.dirname(self.__glexec_path),
                                               'mkgltempdir')

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
        os.chmod(configuration_path, 0777)
	pUtil.tolog('dumped config file at %s' %configuration_path)

    def __ship_queue_data(self):
        """Ships the queue data to the sandbox environment."""

	for file_name in os.listdir(os.environ['PilotHomeDir']):
		full_file_name = os.path.join(os.environ['PilotHomeDir'], file_name)
		if (os.path.isfile(full_file_name)) and '.pyc' not in file_name:
			try:
				os.chmod(os.path.join(os.environ['PilotHomeDir'], file_name), 0777)
				shutil.copy(full_file_name, self.sandbox_path)
			except:
				pUtil.tolog('cannot chmod and copy %s ' %file_name)

        dirs = [d for d in os.listdir(os.environ['PilotHomeDir']) if os.path.isdir(os.path.join(os.environ['PilotHomeDir'], d)) and 'gltmpdir' not in d and 'Panda_Pilot' not in d]
	
	for i in dirs:
                if os.path.exists(os.path.join(self.sandbox_path, i)):
                        shutil.rmtree(os.path.join(self.sandbox_path, i))
                shutil.copytree(os.path.join(os.environ['PilotHomeDir'], i), os.path.join(self.sandbox_path, i))
		os.chmod(os.path.join(self.sandbox_path,i), 0777)
		for root, directories, files in os.walk(os.path.join(self.sandbox_path, i)):  
  			for dir in directories:  
			    os.chmod(os.path.join(root, dir), 0777)
		  	for file in files:
			    os.chmod(os.path.join(root, file), 0755)


    def __ship_job_definition(self):
        """Ships the job definition to the sandbox."""
        execute('cp ./Job_*.py %s; chmod 777 %s/Job_*.py' % (self.sandbox_path, self.sandbox_path))

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
	env['job'].datadir = env['thisSite'].workdir + '/PandaJob_' + env['job'].jobId + '_data'
        if not os.path.exists(env['thisSite'].workdir):
        	os.makedirs(env['thisSite'].workdir)
		os.chmod(env['thisSite'].workdir,0777)
	self.__dump_current_configuration()

	pUtil.tolog('dumping debug info')
	debug_cmd = "ls -ld `pwd`; ls -ld %s ; ls -la %s/glexec*; ls -ld %s" %(self.__target_path, self.__target_path, env['thisSite'].workdir)

	self.output, self.error, self.status = execute(debug_cmd)
	pUtil.tolog(self.output) 

	pUtil.tolog("cding and running GLEXEC!")
        cmd = "export GLEXEC_ENV=`%s`; \
               %s %s -- 'cd %s; \
               %s 2>&1;'" % (self.__wrapper_path,
                             self.__glexec_path,
                             self.__unwrapper_path,
                             self.__target_path,
                             self.payload)
        self.output, self.error, self.status = execute(cmd)
        pUtil.tolog(self.error)
        pUtil.tolog(self.output)

    def __clean(self):
        """Remove the previously created proxy and reload configuration."""

        pUtil.tolog('Actual workdir is %s ' % self.__actual_workdir)
        pUtil.tolog('Will now check current dir and remove it from sys.path %s ' % os.getcwd())
        sys.path.remove(os.getcwd())
        os.chdir(self.__actual_workdir)
        env = Configuration.Configuration()
        env['workdir'] = self.__actual_workdir
	env['thisSite'].workdir = self.__actual_workdir
	env['PilotHomeDir'] = self.__actual_workdir

	if json is None:
	    raise RuntimeError('json is not available')
        try:
            # Reloads configuration from the subprocess.
            configuration_path = os.path.join(self.sandbox_path, 'data-orig.json')
            CustomEncoder.ConfigurationSerializer.deserialize_file(configuration_path)

	except:
            pUtil.tolog('Failed to reload previous configuration. Will manually correct the variables')
            Configuration.Configuration()['inputDir'] = self.__actual_workdir
            Configuration.Configuration()['outputDir'] = self.__actual_workdir
            Configuration.Configuration()['pilot_initdir'] = self.__actual_workdir
	    Configuration.Configuration()['thisSite'].wntmpdir = self.__actual_workdir
            Configuration.Configuration()['workdir'] = self.__actual_workdir

	pUtil.tolog('glexec client cert is %s ' % os.environ['GLEXEC_CLIENT_CERT'])
        pUtil.tolog('glexec source proxy is %s ' % os.environ['GLEXEC_SOURCE_PROXY'])
        pUtil.tolog('glexec target dir is %s ' % os.environ['GLEXEC_TARGET_DIR'])
        pUtil.tolog('glexec target proxy is %s ' % os.environ['GLEXEC_TARGET_PROXY'])

        pUtil.tolog('Will now delete completely the glexec temporary dir')
        cmd = '%s -r %s -f' % (self.__mkgltempdir_path, self.__target_path)
        self.output, self.error, self.status = execute(cmd)
        pUtil.tolog(self.error)
        pUtil.tolog(self.output)

	pUtil.tolog('Removing GLEXEC env vars')
	del os.environ['GLEXEC_CLIENT_CERT']
        del os.environ['GLEXEC_SOURCE_PROXY']
        del os.environ['GLEXEC_TARGET_DIR']
        del os.environ['GLEXEC_TARGET_PROXY']
        pUtil.tolog('Removing user_proxy at %s' % self.__new_proxy_path)
        os.remove(self.__new_proxy_path)
        pUtil.tolog('Proxy removed!')
	sys.path.remove(self.__target_path)	
        pUtil.tolog('sys path now is ... %s ' % sys.path)
