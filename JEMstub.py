#!/usr/bin/env python
'''
JEM - the Job Execution Monitor - library head module for PanDA integration.

part of the Job Execution Monitor (JEM) - https://svn.grid.uni-wuppertal.de/trac/JEM
(c) 2010 Bergische Universitaet Wuppertal

Written by Tim dos Santos (dos.santos@physik.uni-wuppertal.de)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
'''
import sys, os, re, types
from copy import copy


BLOCKSIZE = 512
JEMSTUB_VER = "gamma-16b"


class NullLogger(object):
    def __init__(self, name = ""): pass
    def trace(self, msg): pass
    def debug(self, msg): pass
    def info(self, msg): pass
    def warn(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass
    def fatal(self, msg): pass
    def critical(self, msg): pass


class ConsoleLogger(object):
    def __init__(self, name):
        self.name = name
    def _out(self, severity, msg):
        print "%-16s %-16s %s" % (self.name[:16], severity[:16], msg)
    def trace(self, msg):
        self._out("TRACE", msg)
    def debug(self, msg):
        self._out("DEBUG", msg)
    def info(self, msg):
        self._out("INFO", msg)
    def warn(self, msg):
        self._out("WARN", msg)
    def warning(self, msg):
        self._out("WARN", msg)
    def error(self, msg):
        self._out("ERROR", msg)
    def fatal(self, msg):
        self._out("FATAL", msg)
    def critical(self, msg):
        self._out("FATAL", msg)


class FuncLogger(object):
    def __init__(self, f = None):
        self._out = f
    def trace(self, msg):
        self._out("trace | " + msg)
    def debug(self, msg):
        self._out("debug | " + msg)
    def info(self, msg):
        self._out("info  | " + msg)
    def warn(self, msg):
        self._out("warn  | " + msg)
    def warning(self, msg):
        self._out("warn  | " + msg)
    def error(self, msg):
        self._out("ERROR | " + msg)
    def fatal(self, msg):
        self._out("FATAL | " + msg)
    def critical(self, msg):
        self._out("FATAL | " + msg)


class JEMstub(object):
    """
    The JEMstub class represents JEMs library and, if it is not present, tries to load it from the
    given URL (usually from a web server).
    """
    def __init__(self, logger = NullLogger(), actSvcResult = {}):
        """
        Construct a JEMstub instance.
        
        @param logger: Logger instance (if any) to use. Must have the usual .debug, .info... methods.
        @param activationSvcResult: Result dict received from the activation server  
        """
        self.repo = actSvcResult['repository']
        self.version = actSvcResult['version']
        self.actRes = actSvcResult

        try:
            if self.repo[-1] != '/': self.repo += '/'
            os.environ["JEM_PluginLoader_repositoryBaseUrl"] = self.repo + self.version
        except:
            pass

        self.dest_dir = "JEM"
        self.lib_tar = "JEM.library.tgz"
        self.launcher_tar = "JEM.launcher.tgz"
        if logger is not None and type(logger) == types.FunctionType:
            self.logger = FuncLogger(logger)
        else:
            self.logger = logger


    def __exc_handler(self):
        ei = sys.exc_info()
        self.logger.error("JEMstub: %s - %s" % (str(ei[0]), str(ei[1])))


    def __download(self, repoUrl, wanted_jem_ver, filename, dest):

        if not repoUrl[-1] == "/": repoUrl += "/"
        fullUrl = repoUrl + wanted_jem_ver
        if not fullUrl[-1] == "/": fullUrl += "/"

        self.logger.debug("trying to download '%s' from %s ..." % (filename, fullUrl))

        try:
            import urllib2
            c = urllib2.urlopen(fullUrl + filename)
        except:
            self.__exc_handler()
            return False

        try:
            if not os.path.exists(dest): os.makedirs(dest)
            if not dest[-1] == os.sep: dest += os.sep
            fd = open(dest + filename, "wb")
        except:
            self.__exc_handler()
            return False

        try:
            if c.headers.has_key("content-length"):
                _len = int(c.headers["content-length"])
                self.logger.debug("downloading %d bytes" % _len)
            else:
                _len = None

            _read = None
            _sum = 0
            while _read != 0:
                try:
                    _data = c.read(BLOCKSIZE)
                    _read = len(_data)
                    _sum += _read
                    fd.write(_data)
                except:
                    self.__exc_handler()
            self.logger.debug("%d bytes loaded." % _sum)

            return True
        except:
            self.__exc_handler()

        fd.close()


    def __extract(self, file, keep = False):  # @ReservedAssignment
        try:
            self.logger.debug("extracting %s ..." % file)

            path = self.dest_dir + os.sep + file
            if not os.path.exists(path) or not os.path.isfile(path):
                return False

            _cwd = os.getcwd()
            os.chdir(self.dest_dir)

            import tarfile
            tf = tarfile.open(file, "r:gz")

            for f in tf:
                tf.extract(f)

            tf.close()

            if not keep: os.unlink(file)

            os.system("touch __init__.py")
            os.chdir(_cwd)
            return True
        except:
            self.__exc_handler()

        return False


    def __import(self):
        try:
            sys.path.append(os.path.abspath(self.dest_dir))

            from Common.Info import VERSION, REV  # @UnresolvedImport
            self.logger.info("JEM %s (rev %s)" % (VERSION, REV))

            return True
        except:
            self.__exc_handler()
        return False


    def __getFromCvmfsOrLocal(self):
        """
        Try to initialize JEM from CVMFS (if available) or try to use local installed JEM version
        
        @return success
        """
        try:
            ver_dir = "/cvmfs/atlas.cern.ch/repo/sw/JEM-WN/%s" % self.version
            if os.path.exists(ver_dir):
                try:
                    import commands
                    r = commands.getoutput("sh -c 'source %s/setup.sh && echo $JEM_PACKAGEPATH'" % ver_dir)
                    if os.path.exists(r + "/JEM.py"):
                        os.environ["JEM_PACKAGEPATH"] = r
                        self.logger.debug('loaded JEM from CVMFS: ' + ver_dir)
                except:
                    pass
        except:
            pass

        # check if JEM may already just be there! :)
        if "JEM_PACKAGEPATH" in os.environ:
            path = os.environ["JEM_PACKAGEPATH"]
            if os.path.exists(path + os.sep + "JEM.py"):
                self.dest_dir = path
                self.logger.info("using JEM from %s" % path)
                return True
        else:
            return False


    def __getFromJEMpage(self):
        """
        Try to download and extract the latest JEM version from a central web server.
        If successful, insert it into the python path.
        
        @return success
        """

        if not self.__download(self.repo, self.version, self.lib_tar, self.dest_dir): return False
        if not self.__extract(self.lib_tar): return False

        if not self.__download(self.repo, self.version, self.launcher_tar, self.dest_dir): return False
        if not self.__extract(self.launcher_tar): return False

        self.logger.info("successfully downloaded and extracted JEM ver %s from repo %s" % (self.version, self.repo))

        if os.path.exists(self.dest_dir + "/JEM.py"):
            os.environ["JEM_PACKAGEPATH"] = self.dest_dir


        return True


    def __getAndInitializeJEM(self):
        """
        Try to get JEM either from CVMFS or from a local installation or from the JEM web page

        @return success
        """

        if self.__getFromCvmfsOrLocal():
            self.logger.debug('loaded JEM from cvmfs')
            return self.__import()
        else:
            self.logger.debug('failed to laod JEM from cvmfs, trying jem page')

        if self.__getFromJEMpage():
            self.logger.debug('loaded JEM from JEM page')
            return self.__import()
        else:
            self.logger.debug("couldn't load JEM")
            return False


    def wrapJEMaroundCommand(self, args = [], intoShellScript = None, withJobId = None,
                             withUserDN = None, config = None):
        """
        Wrap a to-be-executed command into a JEM WN instance. This is a bit more
        tricky as I originally thought, because there may be arguments to the command
        that contain spaces or that are enclosed in quotes; this has to be detected
        and correctly preserved by JEM.
        
        @param args: Argument list (execv-style or plain command string)
        @param intoShellScript: None or filename (then: create temp script for cmd)
        @param withJobId: None or job-id (string)
        @param withUserDN: None or user-DN (string)
        @param config: None or dict (string => string, JEM config vars in env-format)
        @return modified args
        """
        if not self.__getAndInitializeJEM():
            self.logger.error("Failed to get/initialize JEM - will run unmonitored")
            return args
        self.logger.info("wrapping command: %s" % str(args))

        # custom JEM-configuration
        if config is not None:
            try:
                from Common.Utils.CoreUtils import process_config_string  # @UnresolvedImport
                process_config_string(config, logger = self.logger, to_env = True)
            except:
                ei = sys.exc_info()
                self.logger.warn("failed to evaluate custom JEM configuration - continuing with (minimal) defaults")
                self.logger.warn("the underlying error was: %s - %s" % (str(ei[0]), str(ei[1])))

        jemargs = ["--mode", "WN"]

        # pass job ID to JEM (if set)
        if withJobId is not None:
            jemargs += ["--job-id", str(withJobId)]

        # pass real user DN to JEM (if set)
        if withUserDN is not None:
            os.environ["PanDA_user_DN"] = str(withUserDN)

        # use passed config file, if present
        jemargs += ["--config", "./JEM.conf"]
        isstr = (type(args) == str)
        sargs = copy(args)

        # let JEM find free port on startup.
        jemargs += ["--logging-port 0"]

        # create outter shell script (for cases where just a list of multiple shell commands is given to us)
        if type(intoShellScript) == str:
            try:
                fd = open(intoShellScript, "w")
                fd.write("#!/bin/sh\n")
            except:
                self.logger.info("failed to write wrapscript. running as-is.")
                return sargs

            try:
                if not isstr: args = " ".join(args)
            except:
                self.logger.info("command not wrapable. running as-is.")
                return sargs

            fd.write(args + "\n")

            # # let the job say "bye" to ActSvc.
            # fd.write('JOB_RESULT=$?\n')
            # fd.write("curl -d jobId=%s -d finished=True -d exitcode=$JOB_RESULT http://jem.physik.uni-wuppertal.de/activation >/dev/null 2>&1\n" % str(withJobId))
            # fd.write("exit $JOB_RESULT\n")

            os.fsync(fd)
            fd.close()

            try:
                import stat
                os.chmod(intoShellScript, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            except:
                self.logger.warn("failed to set payload script permissions - continuing anyway.")

            if isstr:
                return "%s %s %s --script '%s'" % \
                    (sys.executable, self.dest_dir + os.sep + "JEM.py", " ".join(jemargs), intoShellScript)
            else:
                return [sys.executable, self.dest_dir + os.sep + "JEM.py"] + jemargs + ["--script", intoShellScript]
        else:
            try:
                if type(args) == str:
                    from Common.Utils.CoreUtils import split_command_string  # @UnresolvedImport
                    args = split_command_string(args)
                    isstr = True
                else:
                    for i, a in enumerate(args):
                        if " " in a and not a[0] in ("'", '"'): args[i] = '"%s"' % a
            except:
                self.logger.info("command not wrapable. running as-is.")
                return sargs

            if args[0] == sys.executable:
                cmdargs = " ".join(args[1:])
                pyi = sys.executable
            elif re.match(r'python([2-9](\.[0-9])?)?', args[0]):
                cmdargs = " ".join(args[1:])
                pyi = args[0]
            else:
                cmdargs = " ".join(args)
                pyi = sys.executable

            args = [pyi, self.dest_dir + os.sep + "JEM.py"] + jemargs
            if isstr:
                cmd = """%s --script '%s'""" % (" ".join(args), cmdargs)
                self.logger.info("wrapped cmd is: " + cmd)
                return cmd
            else:
                args += ["--script", cmdargs]
                self.logger.info("wrapped cmd is: " + str(args))
                return args


def updateRunCommand4JEM(cmd, job, jobSite, tolog, metaOut = None, actSvcUrl = "http://jem.physik.uni-wuppertal.de/activation"):
    """
    add the JEM wrapper around the run command if necessary
    
    @param cmd Job execution command (string). This is just the command to-be-executed with its arguments!
    @param job Job information object; contains data like job ID, user, type, ...
    @param jobSite Grid site information object (of the site the job will run on)
    @param tolog Logger instance. Some object that has logger methods
    @param metaOut Dictionary that will be filled with JEM-activation-metadata (if not = None)
    @param actSvcUrl 
    @return modified (or unmodified) cmd
    """
    global JEMSTUB_VER

    tolog("updating run cmd for JEM (stub ver: %s)" % JEMSTUB_VER)

    def queryActivationSvc(job, jobSite, userRequestedJEM, tolog, actSvcUrl):
        """
        JEM features an activation-webservice that takes a dictionary of job meta data
        and responds with a YES/NO/DONTCARE answer. It possibly feeds config overrides
        to the local JEM instance, as well, and it can override the URL JEM should be
        fetched from (plus its version: "latest", "dev", or explicit version no.)
        """
        try:
            import urllib
            import socket

            postDict = {}

            # fill in the data that may be relevant for JEM activation decision
            try:
                postDict['user'] = job.prodUserID
            except: pass

            try:
                postDict['jobId'] = job.jobId
            except: pass

            try:
                postDict['jobDefinitionID'] = job.jobDefinitionID
            except: pass

            try:
                postDict['taskID'] = job.taskID
            except: pass

            try:
                postDict['prodSourceLabel'] = job.prodSourceLabel
            except: pass

            try:
                postDict['cloud'] = job.cloud
            except: pass

            try:
                postDict['payloadType'] = job.payload
            except: pass

            try:
                postDict['atlasRelease'] = job.release
            except: pass

            try:
                postDict['siteName'] = jobSite.sitename
            except: pass

            try:
                postDict['CE'] = jobSite.computingElement
            except: pass

            postDict['userRequestedJEM'] = str(userRequestedJEM)

            params = urllib.urlencode(postDict)

            old_timeout_value = socket.getdefaulttimeout()
            socket.setdefaulttimeout(1)
            try:
                def _remove(dict, key):  # @ReservedAssignment
                    try:
                        del dict[key]
                    except: pass

                tolog('querying: %s' % actSvcUrl)
                response = urllib.urlopen(actSvcUrl, params)
                result = response.headers

                # lose standard headers we're not interested in
                _remove(result, "Server")
                _remove(result, "Content-Length")
                _remove(result, "Connection")
                _remove(result, "Content-Type")
            except:
                tolog("connection to activation service timed out, or failed to connect - disabling monitoring")
                result = {}
            socket.setdefaulttimeout(old_timeout_value)

            if not "activate" in result:
                result["activate"] = "NO"
            if not "MQ" in result:
                result["MQ"] = "mq.pleiades.uni-wuppertal.de"
            if not "config" in result:
                result["config"] = ""
            if not "reason" in result:
                result["reason"] = "n/a"
            return result
        except:
            ei = sys.exc_info()
            tolog("control server didn't respond or failed: %s - %s" % (str(ei[0]), str(ei[1])))
            tolog("""falling back to "NO".""")
            return {"activate": "NO", "MQ": "mq.pleiades.uni-wuppertal.de", "config": "", "reason": "AvtSvc failure"}

    def remove_enclosing_quotes(_string):
        try:
            _string = _string.strip()
            if _string[0] == _string[-1] and _string[0] in ('"', "'"): _string = _string[1:-1]
        except: pass
        return _string

    def split_quoted_string(_string, _sep = (';',), _quot = ('"', "'",), _esc = ('\\',)):
        results, quote, prev = [[]], None, ""
        for c in _string:
            if c in _quot:
                if c == quote:
                    quote = None
                elif quote == None:
                    quote = c
            elif c in _sep:
                if prev in _esc:
                    prev = ""
                elif quote == None:
                    results.append([])
                    continue
            elif c in _esc:
                if not prev in _esc:
                    prev = c
                    continue
                prev = ""
            elif prev in _esc:
                results[-1].append(prev)
                prev = ""
            results[-1].append(c)
            prev = c
        return [''.join(x) for x in results]

    # try to query the activation-service, to check whether it wants JEM added or not (or doesn't care)
    tolog("attempting to query JEM activation service")
    result = queryActivationSvc(job, jobSite, "--enable-jem" in cmd, tolog, actSvcUrl)
    tolog("activation service responded:\n%s" % str(result).strip())
    _config = ""

    if metaOut is not None:
        metaOut["actSvcResult"] = result["activate"]
        metaOut["actSvcReason"] = result["reason"]

    # make sure version and repository are set to defaults
    if "version" in result:
        # check if not empty
        if result['version'] == '':
            result["version"] = 'latest'
    else:
        # no version given, use default
        result["version"] = 'latest'
    if "repository" in result:
        # check if not empty
        if result['repository'] == '':
            result["repository"] = "http://jem.physik.uni-wuppertal.de/repository/"
    else:
        # no version given, use default
        result["repository"] = "http://jem.physik.uni-wuppertal.de/repository/"

    # prepare metadata
    try:
        userDN = job.prodUserID
    except:
        userDN = None

    try:
        jobId = job.jobId
        if jobId is not None:
            jobId = "PanDA." + str(jobId)
    except:
        jobId = None

    force_offline = False

    # check if user requested JEM
    if "--enable-jem" in cmd:
        # remove the JEM parameter (if present)
        cmd = cmd.replace('--enable-jem', '')

        if metaOut is not None: metaOut["userRequestedJEM"] = "YES"

        # if the ActSvc denies JEM activation, but the user requested it, don't disable JEM
        # for this job - just disable the StompValve!
        if result["activate"] == "NO":
            force_offline = True

        result["activate"] = "YES_BY_USER"

        # try to parse jem-config, if it is specified
        try:
            if "--jem-config" in cmd:
                # we need to identify the --jem-config args to remove them.
                try:
                    tokens = split_quoted_string(cmd, (' ',))
                    for i, t in enumerate(tokens):
                        if t == "--jem-config":
                            _config = tokens[i + 1]
                            cmd = cmd.replace(_config, '')
                            _config = remove_enclosing_quotes(_config)
                            break
                except:
                    tolog("invalid JEM-config was specified")
                cmd = cmd.replace('--jem-config', '')

            # HACK: check passed config whether JEM repo is to be overridden...
            if "+ver=" in _config:
                try:
                    tokens = split_quoted_string(_config)
                    for t in tokens:
                        if "+ver=" in t:
                            k, v = t.split("=")  # @UnusedVariable
                            result['version'] = v
                            tolog("using JEM version '%s'" % v)
                            break
                    else:
                        raise ValueError()
                except:
                    tolog("invalid JEM-repository was specified")

            if "+repo=" in _config:
                try:
                    tokens = split_quoted_string(_config)
                    for t in tokens:
                        if "+repo=" in t:
                            k, v = t.split("=")  # @UnusedVariable
                            tolog("using JEM-repository '%s'" % v)
                            result["repository"] = v
                            if not result["repository"][-1] == "/":
                                result["repository"] += "/"
                            break
                    else:
                        raise ValueError()
                except:
                    tolog("invalid JEM-repository was specified")
        except:
            pass
    else:
        if metaOut is not None: metaOut["userRequestedJEM"] = "NO"

    # --- here we finally replace the run command with JEM, if it's enabled (by user or by act svc). ------------------
    if result["activate"] in ("YES", "YES_BY_USER"):
        # process config passed via control-server response. appending
        # effectively overrides stuff passed by the user in cmd.
        try:
            if result["config"] != "": _config += ";" + remove_enclosing_quotes(result["config"])
            while ";;" in _config: _config = _config.replace(";;", ";")
        except: pass

        # if live-JEM was denied, remove all Stomp-related options.
        if force_offline:
            # also catch multiple occurrences... ugh. but it works.
            while "+live" in _config: _config = _config.replace("+live", "")
            while "WN_Valves=StompValve" in _config: _config = _config.replace("WN_Valves=StompValve", "")
            while "WN_Valves=PyStompValve" in _config: _config = _config.replace("WN_Valves=PyStompValve", "")
            while "+StompValve" in _config: _config = _config.replace("+StompValve", "")
            while "+PyStompValve" in _config: _config = _config.replace("+PyStompValve", "")

        # put some more infos in the job's env which JEM may need / want to know...
        try:
            os.environ["PanDA_job_def_id"] = job.jobDefinitionID
        except: pass

        try:
            os.environ["PanDA_task_id"] = job.taskID
        except: pass

        try:
            os.environ["PanDA_site_name"] = jobSite.sitename
        except: pass

        try:
            os.environ["PanDA_CE_name"] = jobSite.computingElement
        except: pass

        try:
            os.environ['PanDA_cloud_name'] = job.cloud
        except: pass

        try:
            os.environ['PanDA_atlas_release'] = job.release
        except: pass

        try:
            os.environ['PanDA_payload_type'] = job.payload
        except: pass

        try:
            os.environ['PanDA_prod_source_label'] = job.prodSourceLabel
        except: pass


        if result["activate"] == "YES_BY_USER":
            tolog("Trying to enable JEM (requested by user)")
        else:
            tolog("Trying to enable JEM (requested by activation svc)")

        try:
            jem = JEMstub(logger = tolog, actSvcResult = result)

            tolog("using JEM-config '%s'" % _config)

            # hack: have unique payload shell script for multijobs
            import time
            shellScriptName = '_payload_%f.sh' % time.time()

            # modify the cmd to run to be wrapped into a JEM instance.
            cmd = jem.wrapJEMaroundCommand(cmd, intoShellScript = shellScriptName, withJobId = jobId,
                                                withUserDN = userDN, config = _config)

            if metaOut is not None:
                metaOut["JEMactive"] = "YES"
                if result["activate"] == "YES_BY_USER":
                    metaOut["JEMactiveReason"] = "USERREQ"
                else:
                    metaOut["JEMactiveReason"] = "FORCED"

            # done, delete the JEM object
            del jem
        except:
            ei = sys.exc_info()
            tolog("Enabling JEM failed (error message: %s - %s)" % (str(ei[0]), str(ei[1])))
            if metaOut is not None:
                metaOut["JEMactive"] = "NO"
                metaOut["JEMactiveReason"] = "ERROR"
    elif result["activate"] == "NO":
        if "reason" in result:
            reason = result["reason"]
        else:
            reason = "n/a"
        tolog("JEM *not* enabled (reason: %s)" % reason)
        if metaOut is not None:
            metaOut["JEMactive"] = "NO"
            metaOut["JEMactiveReason"] = "DENIED"
    else:
        tolog("JEM not requested")
        if metaOut is not None:
            metaOut["JEMactive"] = "NO"
            metaOut["JEMactiveReason"] = "NOREQ"

    return cmd


def notifyJobEnd2JEM(job, tolog):
    try:
        import urllib
        import socket

        postDict = {"finished": "True"}

        try:
            postDict['jobId'] = job.jobId
        except: pass

        try:
            postDict['exitcode'] = job.exeErrorCode
        except: pass

        # in the current impl., we repeat all header fields for the bye-message;
        # this is to compare with the activation-message (mainly control reasons).
        # this could in principle be dropped (jobId should suffice!). Hmmm.
        try:
            postDict['user'] = job.prodUserID
        except: pass

        try:
            postDict['jobDefinitionID'] = job.jobDefinitionID
        except: pass

        try:
            postDict['taskID'] = job.taskID
        except: pass

        try:
            postDict['prodSourceLabel'] = job.prodSourceLabel
        except: pass

        try:
            postDict['cloud'] = job.cloud
        except: pass

        try:
            postDict['payloadType'] = job.payload
        except: pass

        try:
            postDict['atlasRelease'] = job.release
        except: pass

        params = urllib.urlencode(postDict)

        old_timeout_value = socket.getdefaulttimeout()
        socket.setdefaulttimeout(10)  # hmm. enough? too much?
        try:
            urllib.urlopen("http://jem.physik.uni-wuppertal.de/activation", params)
        except:
            pass
        socket.setdefaulttimeout(old_timeout_value)
    except:
        pass  # ignore


if __name__ == "__main__":
    """
    By launching the stub from the command line like this:

    ./JEMstub.py <command> <args...>
    
    the command is wrapped by a freshly downloaded and extracted JEM (if that succeeded). If
    download or extraction fails, the command is just run as-is. This is mainly possible for
    debug reasons, most probably we'll want to directy use the JEMstub class in the pilot.
    """
    j = JEMstub(logger = ConsoleLogger("main"))

    if len(sys.argv) > 1:
        args = j.wrapJEMaroundCommand(sys.argv[1:])
        print args
    else:
        args = j.wrapJEMaroundCommand('echo "Hello, World!"')
        print args
