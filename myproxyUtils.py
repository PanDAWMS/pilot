#!/bin/env python

import commands
import os
import subprocess
import tempfile

class CommandLine(object):

    def __init__(self, program):

        self.program = program
        self.output    = None    # the std output after execution
        self.error     = None    # the std error after execution
        self.status    = None    # the return code after execution

    def execute(self):

        p = subprocess.Popen(self.program, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        self.output = p.stdout.read()
        self.error = p.stderr.read()
        self.status = p.wait()

        
class MyProxyInterface(object):

    def __init__(self, userdn, credname, servername='myproxy.cern.ch'):

        self.servername = servername  # name of the myproxy server
        self.userDN = userdn          # DN of the user
        self.credname = credname      # key (acting as pseudo-password) 

    self.__fixuserdn()

    def retrieve(self, proxypath):
        
        cmd = 'myproxy-logon'
        cmd += ' -s %s' % self.servername     # name of the myproxy server 
        cmd += ' --no_passphrase'             # do not prompt for password
        cmd += ' --out %s' % proxypath        # retrieved proxy location
        cmd += ' -l %s'% self.userDN          # user DN used as username
        cmd += ' -k %s'% self.credname        # the key
        cmd += ' -t 0'                        # allows maximum lifetime 
                                              # for retrieved proxies
                                              # this timelife is then specified
                                              # at the delegation time
        CommandLine(cmd).execute()

    def __fixuserdn(self):
        """delegated proxies do not include strings like "/CN=proxy" or 
        "/CN=12345678" in the DN. Extra strings have to be removed
        Those strings have to be deleted in order
        to match properly the name of the proxy delegated 
        in a myproxy server.
        
        The parentheses are scaped because the MyProxy server
        understand them as regexp syntax otherwise
        
        Inserting the dn between quoation marks to prevent problems
        with the shell and special characters like white spaces, pipes, etc
        """

        if self.userDN.count('/CN=') > 1:
            first_index = self.userDN.find('/CN=')
            second_index = self.userDN.find('/CN=', first_index+1)
            self.userDN = self.userDN[0:second_index]

        self.userDN = self.userDN.replace('(', '\(')
        self.userDN = self.userDN.replace(')', '\)')

        self.userDN = '\'%s\'' % self.userDN


class executeGlexec(object):

    def __init__(self, myproxyinterface, payload='run.sh'):
        '''
        myproxyinterface is an instance of class MyProxyInterface

        payload is the final end-user command to be executed.
        It may contain also input options, for example:
            payload='athena --indataset=blah --outdataset=blah ...'
        Default value is run.sh for historical reasons.
        '''

        self.myproxyinterface = myproxyinterface
        self.payload = payload 

        # environment variables that must not be recreated
        # by glexec, so they must not go to the wrapper
        self.excluded_env_vars = ['X509_USER_PROXY', 'HOME', 'LD_PRELOAD', 'LOGNAME', 'USER', '_'] 

        self.output = None
        self.status = None

    def execute(self):

        #   change permissions
        #   setup environment variable GLEXEC_CLIENT_CERT

        self.__changepermissions()
        self.__setglexecpath()
        self.__retrieveproxy()
        self.__createwrapper()
        self.__setglexecenvvars()
        self.__createcmd()
        self.__runglexec()
        self.__clean()

    def __changepermissions(self):

        commands.getoutput('chmod -R go+rwx .')

    def __setglexecpath(self):
        """ sets the path with the glexec executable
        In gLite sites is  $GLITE_LOCATION/sbin/glexec  
        In OSG sites is (not sure) $OSG_GLEXEC_LOCATION/glexec
        """
        if os.environ.has_key('OSG_GLEXEC_LOCATION'):
                self.__glexecpath = os.environ['OSG_GLEXEC_LOCATION']
        elif os.environ.has_key('GLITE_LOCATION'):
                self.__glexecpath = os.path.join(os.environ['GLITE_LOCATION'], 'sbin/glexec')
        elif os.environ.has_key('GLEXEC_LOCATION'):
                self.__glexecpath = os.path.join(os.environ['GLEXEC_LOCATION'], 'sbin/glexec')

    def __retrieveproxy(self):
        """
        New proxy path:
               the retrieved credentials will be placed in a temporary directory underneath /tmp/
               The name of the proxy will be then like /tmp/<random_dir>/X509

        For the time being, we are going to assume we retrieve the proxy
        once per job. No renewal.
        """

        self.__newproxydir = tempfile.mkdtemp(dir='/tmp/')
        self.__newproxypath = os.path.join(self.__newproxydir, 'X509')
        self.myproxyinterface.retrieve(self.__newproxypath)
        # regenerating voms attributes
        #self.__regenerate()

    def __regenerate(self):
        """ method to regenerate voms attributes
        The option -noregen of voms-proxy-init is used.
        """

        cmd = 'voms-proxy-init'
        cmd += ' -key %s' %self.__newproxypath
        cmd += ' -cert %s' %self.__newproxypath
        cmd += ' -noregen'
        cmd += ' -limited'
        cmd += ' -out %s' %self.__newproxypath
        # FIXME ?
        #cmd += ' -vomses %s' %self.vomses
        #cmd += ' -voms %s' %attributes

        # FIXME. 
    CommandLine(cmd).execute()

    # --------------------------------------------------------------------------- 
    # wrapper creation: begin
    # --------------------------------------------------------------------------- 

    def __createwrapper(self):
        """Method to create an intermediate wrapper to be run under gLExec.
        In this wrapper the whole environment is reconstructed, 
        and the payload is called.

        The default value for the payload is 'run.sh'

        The environment has two components:
              - the os environment, listed in os.environ()
              - the GLEXEC specific variables
        """

        self.__workingdir = os.getcwd()
        self.__wrapperpath = os.path.join(self.__workingdir, 'glexecwrapper.sh')
        self.__wrapper = open(self.__wrapperpath, "w")

        self.__wrapper_init()
        self.__wrapper_environment()
        self.__wrapper_eggdir()
        self.__wrapper_payload()
        self.__wrapper.close()
        self.__wrapper_executable()
        self.__wrapper_readable()

    def __wrapper_init(self):
        """adding the shebang
        """
        print >> self.__wrapper, '#!/bin/bash \n'
        print >> self.__wrapper, '\n'

    def __wrapper_environment(self):
        """recreation of the environment
        """
        for var, value in os.environ.iteritems():
                if var not in self.excluded_env_vars:
                        self.__wrapper_os_envvar(var, value)

    def __wrapper_os_envvar(self, var, value):
        """adding a single environment variable  to wrapper
        """
        value = value.replace('"',r'\"')
        if var != '' and value != '':
                print >> self.__wrapper, 'export %s="%s"' %(var, value)

    def __wrapper_eggdir(self):
        '''
        adding the creation of the eggdir 
        at a random place
        Not sure still needed.
        '''
        print >> self.__wrapper, 'eggdir=`uuidgen`'
        print >> self.__wrapper, 'mkdir /tmp/$eggdir'
        print >> self.__wrapper, 'export PYTHON_EGG_CACHE=/tmp/$eggdir'
        print >> self.__wrapper, 'umask u=rwx,g=rwx,o=rwx'

    def __wrapper_payload(self):
        """adding lines to the wrapper pointing to the paylaod 
        """
        # NOTE:
        #  maybe it is needed here to escape all possible quotes, slashes, etc.
        # that the payload variable may contain.

        print >> self.__wrapper, 'cd %s\n' % self.__workingdir
        print >> self.__wrapper, '%s' %  self.payload

    def __wrapper_executable(self):
        """giving the wrapper execution permissions
        """
        CommandLine('chmod +x %s' % self.__wrapperpath).execute()

    def __wrapper_readable(self):
        """makes wrapper file readable by anyone,
        not just the owner (the pilot)
        """
        CommandLine('chmod go+r %s' % self.__wrapperpath).execute()


    def __setglexecenvvars(self):
        '''
        sets up the two GLEXEC_ env var needed to handle the proxy credentials
        '''
        os.environ['GLEXEC_CLIENT_CERT'] = self.__newproxypath
        os.environ['GLEXEC_SOURCE_PROXY'] = self.__newproxypath

    # --------------------------------------------------------------------------- 
    # wrapper creation: end
    # --------------------------------------------------------------------------- 

    def __createcmd(self):

        self.__cmd = 'cd %s; %s /bin/bash %s' % (
            self.__workingdir,
            self.__glexecpath,
            self.__wrapperpath,
            )

    def __runglexec(self):

        self.__glexec_cmd = CommandLine(self.__cmd)
        self.__glexec_cmd.execute()
        self.output = self.__glexec_cmd.output
        self.error = self.__glexec_cmd.error
        self.status = self.__glexec_cmd.status 

    def __clean(self):

        os.remove(self.__newproxypath)   # the retrieved proxy is removed
        os.remove(self.__wrapperpath)    # the intermediate wrapper is 
                                         # not needed any longer 
