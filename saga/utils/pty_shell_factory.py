
<<<<<<< HEAD
__author__    = "Andre Merzky"
=======
__author__    = "Andre Merzky, Ole Weidner"
>>>>>>> origin/titan
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


<<<<<<< HEAD
import getpass
import os
import pwd
import string
import sys

import saga
import saga.exceptions         as se
import saga.utils.logger       as sul
import saga.utils.misc         as sumisc
import saga.utils.pty_process  as supp
import saga.utils.singleton    as sus
import saga.utils.threads      as sut
import saga.utils.which        as suw
=======
import os
import sys
import pwd
import string
import getpass

import radical.utils           as ru
import radical.utils.logger    as rul

import saga
import saga.exceptions         as se
import saga.utils.misc         as sumisc
import saga.utils.pty_process  as supp

import pty_exceptions               as ptye
>>>>>>> origin/titan


# ------------------------------------------------------------------------------
#
# ssh options:
#   -e none         : no escape character
#   -M              : master mode for connection sharing
#   -S control_path : slave mode for connection sharing
#   -t              : force pty allocation
#   -x              : disable x11 forwarding
#   
#   ServerAliveInterval
#   CheckHostIP no
#   ConnectTimeout
#   ControlMaster  yes | no | no ...
#   ControlPath    $BASE/ssh_control_%n_%p.$$.sock
#                  %r (remote id)? would need inspection
#   ControlPersist 100  : close master after 100 seconds idle
#   EscapeChar     none : transparent for binary data
#   TCPKeepAlive   yes  : detect connection failure
#
#   LoginGraceTime seconds : disconnect if no login after n seconds
#
# ------------------------------------------------------------------------------
<<<<<<< HEAD
# these arrays help to map requested client schemas to master schemas
_SCHEMAS_SH  = ['sh', 'fork', 'local', 'file']
_SCHEMAS_SSH = ['ssh', 'scp', 'sftp']
_SCHEMAS_GSI = ['gsissh', 'gsiscp', 'gsisftp', 'gsiftp']
=======

# these arrays help to map requested client schemas to master schemas
_SCHEMAS_SH  = ['sh', 'fork', 'local', 'file']
_SCHEMAS_SSH = ['ssh', 'scp', 'sftp']
_SCHEMAS_GSI = ['gsissh', 'gsiscp', 'gsisftp']
>>>>>>> origin/titan

_SCHEMAS = _SCHEMAS_SH + _SCHEMAS_SSH + _SCHEMAS_GSI

# FIXME: '-o ControlPersist' is only supported for newer ssh versions.  We
# should add detection, and enable that if available -- for now, just diable it.
#
# FIXME: we should use '%n' instead of '%h', but that is not supported by older
# ssh versions...

# ssh master/slave flag magic # FIXME: make timeouts configurable
<<<<<<< HEAD
_SSH_FLAGS_MASTER   = "-o ControlMaster=yes -o ControlPath=%(ctrl)s"
_SSH_FLAGS_SLAVE    = "-o ControlMaster=no  -o ControlPath=%(ctrl)s"
=======
# _SSH_FLAGS_MASTER = "-o ControlMaster=yes  -o ControlPath=%(ctrl)s -o TCPKeepAlive=yes -o ServerAliveInterval=10 -o ServerAliveCountMax=20 -2 "
# _SSH_FLAGS_SLAVE  = "-o ControlMaster=no   -o ControlPath=%(ctrl)s -o TCPKeepAlive=yes -o ServerAliveInterval=10 -o ServerAliveCountMax=20 -2 "
_SSH_FLAGS_MASTER   = "-o ControlMaster=auto -o ControlPath=%(ctrl)s -o TCPKeepAlive=no  -o ServerAliveInterval=10 -o ServerAliveCountMax=20"
_SSH_FLAGS_SLAVE    = "-o ControlMaster=auto -o ControlPath=%(ctrl)s -o TCPKeepAlive=no  -o ServerAliveInterval=10 -o ServerAliveCountMax=20"
_SCP_FLAGS          = ""
_SFTP_FLAGS         = ""
>>>>>>> origin/titan

# FIXME: right now, we create a shell connection as master --
# but a master does not actually need a shell, as it is never really
# used to run commands...
_SCRIPTS = {
    'ssh' : { 
<<<<<<< HEAD
        'master'        : "%(ssh_env)s %(ssh_exe)s   %(ssh_args)s  %(m_flags)s  %(host_str)s",
        'shell'         : "%(ssh_env)s %(ssh_exe)s   %(ssh_args)s  %(s_flags)s  %(host_str)s",
      # 'copy_to'       : "%(scp_env)s %(scp_exe)s   %(scp_args)s  %(s_flags)s  %(src)s %(root)s/%(tgt)s",
      # 'copy_from'     : "%(scp_env)s %(scp_exe)s   %(scp_args)s  %(s_flags)s  %(root)s/%(src)s %(tgt)s",
        'copy_to'       : "%(sftp_env)s %(sftp_exe)s %(sftp_args)s %(s_flags)s  %(host_str)s",
        'copy_from'     : "%(sftp_env)s %(sftp_exe)s %(sftp_args)s %(s_flags)s  %(host_str)s",
        'copy_to_in'    : "progress \n put %(cp_flags)s %(src)s %(tgt)s \n exit \n",            
        'copy_from_in'  : "progress \n get %(cp_flags)s %(src)s %(tgt)s \n exit \n",
    },
    'sh' : { 
        'master'        : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'shell'         : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'copy_to'       : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'copy_from'     : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'copy_to_in'    : "cd ~ && exec %(cp_exe)s %(cp_flags)s %(src)s %(tgt)s",
        'copy_from_in'  : "cd ~ && exec %(cp_exe)s %(cp_flags)s %(src)s %(tgt)s",
=======
        'master'       : '%(ssh_env)s "%(ssh_exe)s" %(ssh_args)s %(m_flags)s %(host_str)s',
        'shell'        : '%(ssh_env)s "%(ssh_exe)s" %(ssh_args)s %(s_flags)s %(host_str)s'
    },
    'scp' : {
        'copy_to'      : '%(scp_env)s "%(scp_exe)s" %(scp_args)s %(s_flags)s %(cp_flags)s "%(src)s" "%(scp_root)s%(tgt)s"',
        'copy_from'    : '%(scp_env)s "%(scp_exe)s" %(scp_args)s %(s_flags)s %(cp_flags)s "%(scp_root)s%(src)s" "%(tgt)s"',
        'copy_to_in'   : '',
        'copy_from_in' : ''
    },
    'sftp' : {

        'copy_to'      : '%(sftp_env)s "%(sftp_exe)s" %(sftp_args)s %(s_flags)s %(host_str)s',
        'copy_from'    : '%(sftp_env)s "%(sftp_exe)s" %(sftp_args)s %(s_flags)s %(host_str)s',
        'copy_to_in'   : 'mput %(cp_flags)s "%(src)s" "%(tgt)s" \n',
        'copy_from_in' : 'mget %(cp_flags)s "%(src)s" "%(tgt)s" \n'
    },
    'sh' : {
        'master'       : '%(sh_env)s "%(sh_exe)s"  %(sh_args)s',
        'shell'        : '%(sh_env)s "%(sh_exe)s"  %(sh_args)s',
        'copy_to'      : '%(sh_env)s "%(sh_exe)s"  %(sh_args)s',
        'copy_from'    : '%(sh_env)s "%(sh_exe)s"  %(sh_args)s',
        'copy_to_in'   : 'cd ~ && "%(cp_exe)s" -v %(cp_flags)s "%(src)s" "%(tgt)s"',
        'copy_from_in' : 'cd ~ && "%(cp_exe)s" -v %(cp_flags)s "%(src)s" "%(tgt)s"',
>>>>>>> origin/titan
    }
}

# ------------------------------------------------------------------------------
#
class PTYShellFactory (object) :
    """
    This is the place where all master and all client shell connections get
    created.  But also, this factory maintains a registry of master connections,
    to quickly spawn slave connections for any customer w/o repeated
    authorization overhead.  Masters are unique per
    a host/user/port/context/shell_type hash.

    Any ssh master connection in this registry can idle, and may thus shut down
    after ``ControlPersist`` seconds (see options).

    data model::


      self.registry
        |
        +-- "host[:port]"
        |   |
        |   +-- str(context)
        |   |   |
        |   |   +-- "shell_type" (ssh)
        |   |   |   |
        |   |   |   +-- pty_process (gc_timeout)
        |   |   |   +-- shell_initialize()
        |   |   |   +-- shell_finalize()
        |   |   |   +-- shell_alive()
        |   |   |
        |   |   +-- ...
        |   |
        |   +-- ...
        |
        +-- ...

    When Slave connections are requested, a suitable master connection is looked
    for and used.  'Suitable' means: ssh master for scp and sftp slaves; gsissh
    for gsiscp and gsisftp slaves; and sh master for file slaves

    """

<<<<<<< HEAD
    __metaclass__ = sus.Singleton
=======
    __metaclass__ = ru.Singleton
>>>>>>> origin/titan


    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

<<<<<<< HEAD
        self.logger   = sul.getLogger ('PTYShellFactory')
        self.registry = {}
        self.rlock    = sut.RLock ('pty shell factory')
=======
        self.logger     = rul.getLogger ('saga', 'PTYShellFactory')
        self.registry   = {}
        self.rlock      = ru.RLock ('pty shell factory')
>>>>>>> origin/titan


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    def initialize (self, url, session=None, logger=None) :
=======
    def initialize (self, url, session=None, prompt=None, logger=None, posix=True) :
>>>>>>> origin/titan

        with self.rlock :

            # make sure we have a valid url type
            url = saga.Url (url)

<<<<<<< HEAD
            if  not logger :
                logger = sul.getLogger ('PTYShellFactory')

            # collect all information we have/need about the requested master
            # connection
            info = self._create_master_entry (url, session, logger)

            # we got master info - register the master, and create the instance!
            type_s = str(info['type'])
=======
            if  not prompt :
                prompt = "^(.*[\$#%>\]])\s*$"

            if  not logger :
                logger = rul.getLogger ('saga', 'PTYShellFactory')

            # collect all information we have/need about the requested master
            # connection
            info = self._create_master_entry (url, session, prompt, logger)

            # we got master info - register the master, and create the instance!
            type_s = str(info['shell_type'])
>>>>>>> origin/titan
            user_s = str(info['user'])
            host_s = str(info['host_str'])

            # Now, if we don't have that master, yet, we need to instantiate it
            if not host_s in self.registry                 : self.registry[host_s] = {}
            if not user_s in self.registry[host_s]         : self.registry[host_s][user_s] = {}
            if not type_s in self.registry[host_s][user_s] :

                # new master: create an instance, and register it
<<<<<<< HEAD
                m_cmd = _SCRIPTS[info['type']]['master'] % info
=======
                m_cmd = info['scripts'][info['shell_type']]['master'] % info
>>>>>>> origin/titan

                logger.debug ("open master pty for [%s] [%s] %s: %s'" \
                                % (type_s, host_s, user_s, m_cmd))

                info['pty'] = supp.PTYProcess (m_cmd, logger=logger)
                if not info['pty'].alive () :
                    raise se.NoSuccess._log (logger, \
<<<<<<< HEAD
                	  "Shell not connected to %s" % info['host_str'])

                # authorization, prompt setup, etc
                self._initialize_pty (info['pty'], info, is_shell=True)
=======
                          "Shell not connected to %s" % info['host_str'])

                # authorization, prompt setup, etc.  Initialize as shell if not
                # explicitly marked as non-posix shell
                self._initialize_pty (info['pty'], info, is_shell=posix)
>>>>>>> origin/titan

                # master was created - register it
                self.registry[host_s][user_s][type_s] = info


            else :
                # we already have a master: make sure it is alive, and restart as
                # needed
                info = self.registry[host_s][user_s][type_s]

                if  not info['pty'].alive (recover=True) :
                    raise se.IncorrectState._log (logger, \
<<<<<<< HEAD
                	  "Lost shell connection to %s" % info['host_str'])
=======
                          "Lost shell connection to %s" % info['host_str'])
>>>>>>> origin/titan

            return info


    # --------------------------------------------------------------------------
    #
    def _initialize_pty (self, pty_shell, info, is_shell=False) :

        # is_shell: only for shells we use prompt triggers.  sftp for example
        # does not deal well with triggers (no printf).

        with self.rlock :

            shell_pass = info['pass']
            key_pass   = info['key_pass']
<<<<<<< HEAD
=======
            prompt     = info['prompt']
>>>>>>> origin/titan
            logger     = info['logger']
            latency    = info['latency']

            pty_shell.latency = latency

            # if we did not see a decent prompt within 'delay' time, something
            # went wrong.  Try to prompt a prompt (duh!)  Delay should be
            # minimum 0.1 second (to avoid flooding of local shells), and at
            # maximum 1 second (to keep startup time reasonable)
            # most one second.  We try to get within that range with 100*latency.
            delay = min (1.0, max (0.1, 50 * latency))

            try :
                prompt_patterns = ["[Pp]assword:\s*$",             # password   prompt
                                   "Enter passphrase for .*:\s*$", # passphrase prompt
<<<<<<< HEAD
                                   "want to continue connecting",  # hostkey confirmation
                                   ".*HELLO_\\d+_SAGA$",           # prompt detection helper
                                   "^(.*[\$#%>])\s*$"]             # greedy native shell prompt 

                # find a prompt
=======
                                   "Token_Response.*:\s*$",        # passtoken  prompt
                                   "want to continue connecting",  # hostkey confirmation
                                   ".*HELLO_\\d+_SAGA$",           # prompt detection helper
                                   prompt]                         # greedy native shell prompt 

                # find a prompt
                # use a very aggressive, but portable prompt setting scheme.
                # Error messages may appear for tcsh and others
                pty_shell.write (" export PS1='$' ; set prompt='$'\n")
>>>>>>> origin/titan
                n, match = pty_shell.find (prompt_patterns, delay)

                # this loop will run until we finally find the shell prompt, or
                # if we think we have tried enough and give up.  On success
                # we'll try to set a different prompt, and when we found that,
                # too, we exit the loop and are be ready to running shell
<<<<<<< HEAD
=======

>>>>>>> origin/titan
                # commands.
                retries       = 0
                retry_trigger = True
                used_trigger  = False
                found_trigger = ""

                while True :

                    # --------------------------------------------------------------
                    if n == None :

                        # we found none of the prompts, yet, and need to try
                        # again.  But to avoid hanging on invalid prompts, we
                        # print 'HELLO_x_SAGA', and search for that one, too.
                        # We actually do 'printf HELLO_%d_SAGA x' so that the
                        # pattern only appears in the result, not in the
                        # command... 

                        if  retries > 100 :
                            raise se.NoSuccess ("Could not detect shell prompt (timeout)")

<<<<<<< HEAD
=======
                        # make sure we retry a finite time...
                        retries += 1

>>>>>>> origin/titan
                        if  not retry_trigger : 
                            # just waiting for the *right* trigger or prompt, 
                            # don't need new ones...
                            continue

<<<<<<< HEAD
                        retries += 1

                        if  is_shell :
                            pty_shell.write ("printf 'HELLO_%%d_SAGA\\n' %d\n" % retries)
=======
                        if  is_shell :
                            # use a very aggressive, but portable prompt setting scheme
                            pty_shell.write (" export PS1='$' > /dev/null 2>&1 || set prompt='$'\n")
                            pty_shell.write (" printf 'HELLO_%%d_SAGA\\n' %d\n" % retries)
>>>>>>> origin/titan
                            used_trigger = True

                        # FIXME:  consider better timeout
                        n, match = pty_shell.find (prompt_patterns, delay)


                    # --------------------------------------------------------------
                    elif n == 0 :
                        logger.info ("got password prompt")
                        if  not shell_pass :
                            raise se.AuthenticationFailed ("prompted for unknown password (%s)" \
                                                          % match)

<<<<<<< HEAD
                        pty_shell.write ("%s\n" % shell_pass)
=======
                        pty_shell.write ("%s\n" % shell_pass, nolog=True)
>>>>>>> origin/titan
                        n, match = pty_shell.find (prompt_patterns, delay)


                    # --------------------------------------------------------------
                    elif n == 1 :
                        logger.info ("got passphrase prompt : %s" % match)

                        start = string.find (match, "'", 0)
                        end   = string.find (match, "'", start+1)

                        if start == -1 or end == -1 :
                            raise se.AuthenticationFailed ("could not extract key name (%s)" % match)

                        key = match[start+1:end]

                        if  not key in key_pass    :
                            raise se.AuthenticationFailed ("prompted for unknown key password (%s)" \
                                                          % key)

<<<<<<< HEAD
                        pty_shell.write ("%s\n" % key_pass[key])
=======
                        pty_shell.write ("%s\n" % key_pass[key], nolog=True)
>>>>>>> origin/titan
                        n, match = pty_shell.find (prompt_patterns, delay)


                    # --------------------------------------------------------------
                    elif n == 2 :
<<<<<<< HEAD
=======
                        logger.info ("got token prompt")
                        import getpass
                        token = getpass.getpass ("enter token: ")
                        pty_shell.write ("%s\n" % token.strip(), nolog=True)
                        n, match = pty_shell.find (prompt_patterns, delay)


                    # --------------------------------------------------------------
                    elif n == 3 :
>>>>>>> origin/titan
                        logger.info ("got hostkey prompt")
                        pty_shell.write ("yes\n")
                        n, match = pty_shell.find (prompt_patterns, delay)


                    # --------------------------------------------------------------
<<<<<<< HEAD
                    elif n == 3 :
=======
                    elif n == 4 :
>>>>>>> origin/titan

                        # one of the trigger commands got through -- we can now
                        # hope to find the prompt (or the next trigger...)
                        logger.debug ("got shell prompt trigger (%s) (%s)" %  (n, match))

                        found_trigger = match
                        retry_trigger = False
                        n, match = pty_shell.find (prompt_patterns, delay)
                        continue


                    # --------------------------------------------------------------
<<<<<<< HEAD
                    elif n == 4 :
=======
                    elif n == 5 :
>>>>>>> origin/titan

                        logger.debug ("got initial shell prompt (%s) (%s)" %  (n, match))

                        if  retries :
                            if  used_trigger :
                                # we already sent triggers -- so this match is only
                                # useful if saw the *correct* shell prompt trigger
                                # first
                                trigger = "HELLO_%d_SAGA" % retries

                                if  not trigger in found_trigger :
<<<<<<< HEAD
=======

>>>>>>> origin/titan
                                    logger.debug ("waiting for prompt trigger %s: (%s) (%s)" \
                                               % (trigger, n, match))
                                    # but more retries won't help...
                                    retry_trigger = False
<<<<<<< HEAD
                                    n = None
                                    while not n :
                                        n, match = pty_shell.find (prompt_patterns, delay)
                                    continue


                        logger.info ("got initial shell prompt (%s) (%s)" \
                                   % (n, match))

                        # we are done waiting for a prompt
                        break
                
                
            except Exception as e :
                raise self._translate_exception (e)


    # --------------------------------------------------------------------------
    #
    def run_shell (self, info) :
        """ 
        This initiates a master connection.  If there is a suitable master
        connection in the registry, it is re-used, and no new master connection
        is created.  If needed, the existing master connection is revived.  
        """

      # if True :
        with self.rlock :

            s_cmd = _SCRIPTS[info['type']]['shell'] % info

            # at this point, we do have a valid, living master
            sh_slave = supp.PTYProcess (s_cmd, info['logger'])

            # authorization, prompt setup, etc
            self._initialize_pty (sh_slave, info, is_shell=True)

            return sh_slave


    # --------------------------------------------------------------------------
    #
    def run_copy_to (self, info, src, tgt, cp_flags="") :
        """ 
        This initiates a slave copy connection.   Src is interpreted as local
        path, tgt as path on the remote host.
        """

      # if True :
        with self.rlock :

            repl = dict ({'src'      : src, 
                          'tgt'      : tgt, 
                          'cp_flags' : cp_flags}.items ()+ info.items ())

            # at this point, we do have a valid, living master
            s_cmd = _SCRIPTS[info['type']]['copy_to']    % repl
            s_in  = _SCRIPTS[info['type']]['copy_to_in'] % repl

            cp_slave = supp.PTYProcess (s_cmd, info['logger'])

            self._initialize_pty (cp_slave, info)

            cp_slave.write ("%s\n" % s_in)
            cp_slave.wait  ()

            if  cp_slave.exit_code != 0 :
                raise se.NoSuccess._log (info['logger'], "file copy failed: %s" % cp_slave.cache[-256:])

            info['logger'].debug ("copy done")


    # --------------------------------------------------------------------------
    #
    def run_copy_from (self, info, src, tgt, cp_flags="") :
        """ 
        This initiates a slave copy connection.   Src is interpreted as path on
        the remote host, tgt as local path.
=======
                                    attempts      = 0
                                    n             = None

                                    while not n :

                                        attempts += 1
                                        n, match  = pty_shell.find (prompt_patterns, delay)

                                        if  not n :
                                            if  attempts == 1 :
                                                if  is_shell :
                                                    pty_shell.write (" printf 'HELLO_%%d_SAGA\\n' %d\n" % retries)

                                            if  attempts > 100 :
                                                raise se.NoSuccess ("Could not detect shell prompt (timeout)")

                                    continue


                        logger.debug ("Got initial shell prompt (%s) (%s)" \
                                   % (n, match))
                        # we are done waiting for a prompt
                        break
                
            except Exception as e :
                raise ptye.translate_exception (e)
                

    # --------------------------------------------------------------------------
    #
    def get_cp_slave (self, s_cmd, info) :

        with self.rlock :

          # print 'new cp  shell to %s' % s_cmd

            cp_slave = supp.PTYProcess (s_cmd, info['logger'])
            self._initialize_pty (cp_slave, info)

            return cp_slave

    # --------------------------------------------------------------------------
    #
    def run_shell (self, info) :
        """ 
        This initiates a master connection.  If there is a suitable master
        connection in the registry, it is re-used, and no new master connection
        is created.  If needed, the existing master connection is revived.  
>>>>>>> origin/titan
        """

      # if True :
        with self.rlock :

<<<<<<< HEAD
            repl = dict ({'src'      : src, 
                          'tgt'      : tgt, 
                          'cp_flags' : cp_flags}.items ()+ info.items ())

            # at this point, we do have a valid, living master
            s_cmd = _SCRIPTS[info['type']]['copy_from']    % repl
            s_in  = _SCRIPTS[info['type']]['copy_from_in'] % repl

            cp_slave = supp.PTYProcess (s_cmd, info['logger'])

            self._initialize_pty (cp_slave, info)

            cp_slave.write ("%s\n" % s_in)
            cp_slave.wait  ()

            if  cp_slave.exit_code != 0 :
                raise se.NoSuccess._log (info['logger'], "file copy failed: %s" % cp_slave.cache[-256:])

            info['logger'].debug ("copy done")
=======
            s_cmd = info['scripts'][info['shell_type']]['shell'] % info

            # at this point, we do have a valid, living master
            sh_slave = supp.PTYProcess (s_cmd, info['logger'])

            # authorization, prompt setup, etc
            self._initialize_pty (sh_slave, info, is_shell=True)

            return sh_slave
>>>>>>> origin/titan


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    def _create_master_entry (self, url, session, logger) :
=======
    def _create_master_entry (self, url, session, prompt, logger) :
>>>>>>> origin/titan
        # FIXME: cache 'which' results, etc
        # FIXME: check 'which' results

        with self.rlock :
<<<<<<< HEAD
      # if True :

            info = {}

            info['schema']    = url.schema.lower ()
            info['host_str']  = url.host
            info['logger']    = logger
            info['pass']      = ""
            info['key_pass']  = {}

            # find out what type of shell we have to deal with
            if  info['schema']   in _SCHEMAS_SSH :
                info['type']     = "ssh"
                info['ssh_exe']  = suw.which ("ssh")
                info['scp_exe']  = suw.which ("scp")
                info['sftp_exe'] = suw.which ("sftp")

            elif info['schema']  in _SCHEMAS_GSI :
                info['type']     = "ssh"
                info['ssh_exe']  = suw.which ("gsissh")
                info['scp_exe']  = suw.which ("gsiscp")
                info['sftp_exe'] = suw.which ("gsisftp")

            elif info['schema']  in _SCHEMAS_SH :
                info['type']     = "sh"
                info['sh_args']  = "-i"
                info['sh_env']   = "/usr/bin/env TERM=vt100"
                info['cp_env']   = "/usr/bin/env TERM=vt100"
                info['fs_root']  = "/"

                if  "SHELL" in os.environ :
                    info['sh_exe'] =  suw.which (os.environ["SHELL"])
                    info['cp_exe'] =  suw.which ("cp")
                else :
                    info['sh_exe'] =  suw.which ("sh")
                    info['cp_exe'] =  suw.which ("cp")

            else :
                raise se.BadParameter._log (self.logger, \
                	  "cannot handle schema '%s://'" % url.schema)
=======


            info = {}

            # get and evaluate session config
            if  not session :
                session = saga.Session (default=True)

            session_cfg = session.get_config ('saga.utils.pty')
            info['ssh_copy_mode'] = session_cfg['ssh_copy_mode'].get_value ()


            # fill the info dict with details for this master channel, and all
            # related future slave channels
            info['schema']    = url.schema.lower ()
            info['host_str']  = url.host
            info['prompt']    = prompt
            info['logger']    = logger
            info['url']       = url
            info['pass']      = ""
            info['key_pass']  = {}
            info['scripts']   = _SCRIPTS

            if  not info['schema'] :
                info['schema'] = 'local'
                    

            # find out what type of shell we have to deal with
            if  info['schema'] in _SCHEMAS_SSH :
                info['shell_type'] = "ssh"
                info['copy_type']  = info['ssh_copy_mode']
                info['ssh_exe']    = ru.which ("ssh")
                info['scp_exe']    = ru.which ("scp")
                info['sftp_exe']   = ru.which ("sftp")

            elif info['schema'] in _SCHEMAS_GSI :
                info['shell_type'] = "ssh"
                info['copy_type']  = info['ssh_copy_mode']
                info['ssh_exe']    = ru.which ("gsissh")
                info['scp_exe']    = ru.which ("gsiscp")
                info['sftp_exe']   = ru.which ("gsisftp")

            elif info['schema'] in _SCHEMAS_SH :
                info['shell_type'] = "sh"
                info['copy_type']  = "sh"
                info['sh_args']    = "-i"
                info['sh_env']     = "/usr/bin/env TERM=vt100 PS1='PROMPT-$?->'"
                info['cp_env']     = "/usr/bin/env TERM=vt100 PS1='PROMPT-$?->'"
                info['scp_root']   = "/"

                if  "SHELL" in os.environ :
                    info['sh_exe'] =  ru.which (os.environ["SHELL"])
                    info['cp_exe'] =  ru.which ("cp")
                else :
                    info['sh_exe'] =  ru.which ("sh")
                    info['cp_exe'] =  ru.which ("cp")

            else :
                raise se.BadParameter._log (self.logger, \
                          "cannot handle schema '%s://'" % url.schema)
>>>>>>> origin/titan


            # depending on type, create command line (args, env etc)
            #
            # We always set term=vt100 to avoid ansi-escape sequences in the prompt
            # and elsewhere.  Also, we have to make sure that the shell is an
            # interactive login shell, so that it interprets the users startup
            # files, and reacts on commands.

            try :
                info['latency'] = sumisc.get_host_latency (url)

                # FIXME: note that get_host_latency is considered broken (see
                # saga/utils/misc.py line 73), and will return a constant 250ms.

            except Exception  as e :
                info['latency'] = 1.0  # generic value assuming slow link
                info['logger'].warning ("Could not contact host '%s': %s" % (url, e))
                
<<<<<<< HEAD
            if  info['type'] == "sh" :
=======
            if  info['shell_type'] == "sh" :

                info['sh_env'] = "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
>>>>>>> origin/titan

                if not sumisc.host_is_local (url.host) :
                    raise se.BadParameter._log (self.logger, \
                            "expect local host for '%s://', not '%s'" % (url.schema, url.host))

                if  'user' in info and info['user'] :
                    pass
                else :
                    info['user'] = getpass.getuser ()

            else :
                info['ssh_env']   =  "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
                info['scp_env']   =  "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
                info['sftp_env']  =  "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
                info['ssh_args']  =  "-t "                       # force pty
<<<<<<< HEAD
                info['scp_args']  =  ""
                info['sftp_args'] =  ""
=======
                info['scp_args']  =  _SCP_FLAGS
                info['sftp_args'] =  _SFTP_FLAGS
>>>>>>> origin/titan

                if  session :

                    for context in session.contexts :

                        # ssh can also handle UserPass contexts, and ssh type contexts.
                        # gsissh can handle the same, but also X509 contexts.

                        if  context.type.lower () == "ssh" :
                            if  info['schema'] in _SCHEMAS_SSH + _SCHEMAS_GSI :

                                if  context.attribute_exists ("user_id") and context.user_id :
                                    info['user']  = context.user_id

<<<<<<< HEAD
                                if  context.attribute_exists ("user_cert")  and  context.user_cert  :
                                    info['ssh_args']  += "-o IdentityFile=%s " % context.user_cert 
                                    info['scp_args']  += "-o IdentityFile=%s " % context.user_cert 
                                    info['sftp_args'] += "-o IdentityFile=%s " % context.user_cert 

                                    if  context.attribute_exists ("user_pass") and context.user_pass :
                                        info['key_pass'][context.user_cert] = context.user_pass
=======
                                if  context.attribute_exists ("user_key")  and  context.user_key  :
                                    info['ssh_args']  += "-o IdentityFile=%s " % context.user_key 
                                    info['scp_args']  += "-o IdentityFile=%s " % context.user_key 
                                    info['sftp_args'] += "-o IdentityFile=%s " % context.user_key 

                                    if  context.attribute_exists ("user_pass") and context.user_pass :
                                        info['key_pass'][context.user_key] = context.user_pass
>>>>>>> origin/titan

                        if  context.type.lower () == "userpass" :
                            if  info['schema'] in _SCHEMAS_SSH + _SCHEMAS_GSI :
                                if  context.attribute_exists ("user_id") and context.user_id :
                                    info['user']       = context.user_id
                                if  context.attribute_exists ("user_pass") and context.user_pass :
                                    info['pass']       = context.user_pass

                        if  context.type.lower () == "x509" :
                            if  info['schema'] in _SCHEMAS_GSI :

                                if  context.attribute_exists ("user_proxy")  and   context.user_proxy :
                                    info['ssh_env']   += "X509_USER_PROXY='%s' " % context.user_proxy
                                    info['scp_env']   += "X509_USER_PROXY='%s' " % context.user_proxy
                                    info['sftp_env']  += "X509_USER_PROXY='%s' " % context.user_proxy
                       
                                if  context.attribute_exists ("user_cert")   and  context.user_cert :
                                    info['ssh_env']   += "X509_USER_CERT='%s' " % context.user_cert
                                    info['scp_env']   += "X509_USER_CERT='%s' " % context.user_cert
                                    info['sftp_env']  += "X509_USER_CERT='%s' " % context.user_cert
                       
                                if  context.attribute_exists ("user_key")    and  context.user_key :
                                    info['ssh_env']   += "X509_USER_key='%s' "  % context.user_key
                                    info['scp_env']   += "X509_USER_key='%s' "  % context.user_key
                                    info['sftp_env']  += "X509_USER_key='%s' "  % context.user_key
                       
                                if  context.attribute_exists ("cert_repository") and context.cert_repository :
                                    info['ssh_env']   += "X509_CERT_DIR='%s' "  % context.cert_repository
                                    info['scp_env']   += "X509_CERT_DIR='%s' "  % context.cert_repository
                                    info['sftp_env']  += "X509_CERT_DIR='%s' "  % context.cert_repository

                if url.port and url.port != -1 :
                    info['ssh_args']  += "-p %d " % int(url.port)
                    info['scp_args']  += "-p %d " % int(url.port)
                    info['sftp_args'] += "-P %d " % int(url.port)


                # all ssh based shells allow for user_id and user_pass from contexts
                # -- but the data given in the URL take precedence

                if url.username   :  info['user'] = url.username
                if url.password   :  info['pass'] = url.password

                ctrl_user = pwd.getpwuid (os.getuid ()).pw_name
                ctrl_base = "/tmp/saga_ssh_%s" % ctrl_user


                if  'user' in info and info['user'] :
                    info['host_str'] = "%s@%s"  % (info['user'], info['host_str'])
<<<<<<< HEAD
                    info['ctrl'] = "%s_%%h_%%p.%s.%s.ctrl" % (ctrl_base, os.getpid (), info['user'])
                else :
                    info['user'] = getpass.getuser ()
                    info['ctrl'] = "%s_%%h_%%p.%s.ctrl" % (ctrl_base, os.getpid ())

                info['m_flags']  = _SSH_FLAGS_MASTER % ({'ctrl' : info['ctrl']})
                info['s_flags']  = _SSH_FLAGS_SLAVE  % ({'ctrl' : info['ctrl']})
                info['fs_root']  = url

                info['fs_root'].path = "/"
=======
                    info['ctrl'] = "%s_%%h_%%p.%s.ctrl" % (ctrl_base, info['user'])
                else :
                    info['user'] = getpass.getuser ()
                    info['ctrl'] = "%s_%%h_%%p.ctrl" % (ctrl_base)

                info['m_flags']  = _SSH_FLAGS_MASTER % ({'ctrl' : info['ctrl']})
                info['s_flags']  = _SSH_FLAGS_SLAVE  % ({'ctrl' : info['ctrl']})

                # we want the userauth and hostname parts of the URL, to get the
                # scp-scope fs root.  
                info['scp_root']  = ""
                has_auth          = False
                if  url.username : 
                    info['scp_root'] += url.username
                    has_auth          = True
                if  url.password : 
                    info['scp_root'] += ":"
                    info['scp_root'] += url.password
                    has_auth          = True
                if  has_auth :
                    info['scp_root'] += "@"
                info['scp_root']     += "%s:" % url.host

                # FIXME: port needs to be handled as parameter
              # if  url.port : 
              #     info['scp_root'] += ":%d" % url.port
>>>>>>> origin/titan


            # keep all collected info in the master dict, and return it for
            # registration
            return info

<<<<<<< HEAD

    # ----------------------------------------------------------------
    #
    def _translate_exception (self, e) :
        """
        In many cases, we should be able to roughly infer the exception cause
        from the error message -- this is centrally done in this method.  If
        possible, it will return a new exception with a more concise error
        message and appropriate exception type.
        """

        if  not issubclass (e.__class__, se.SagaException) :
            # we do not touch non-saga exceptions
            return e

        if  not issubclass (e.__class__, se.NoSuccess) :
            # this seems to have a specific cause already, leave it alone
            return e

        cmsg = e._plain_message
        lmsg = cmsg.lower ()

        if 'auth' in lmsg :
            e = se.AuthorizationFailed (cmsg)

        elif 'pass' in lmsg :
            e = se.AuthenticationFailed (cmsg)

        elif 'ssh_exchange_identification' in lmsg :
            e = se.AuthenticationFailed ("too frequent login attempts, or sshd misconfiguration: %s" % cmsg)

        elif 'denied' in lmsg :
            e = se.PermissionDenied (cmsg)

        elif 'shared connection' in lmsg :
            e = se.NoSuccess ("Insufficient system resources: %s" % cmsg)

        elif 'pty allocation' in lmsg :
            e = se.NoSuccess ("Insufficient system resources: %s" % cmsg)

        elif 'Connection to master closed' in lmsg :
            e = se.NoSuccess ("Connection failed (insufficient system resources?): %s" % cmsg)

        return e


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

=======
>>>>>>> origin/titan
