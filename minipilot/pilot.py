#!/usr/bin/python -u

import sys
import os
import logging
import logging.config
import logging.handlers
import argparse
import pycurl
from StringIO import StringIO
import json
import cpuinfo
import urllib
import psutil
import socket
import platform
import pip
import time
import traceback
import pipes
from job_description_fixer import description_fixer

logging.basicConfig()
log = logging.getLogger()


try:
    h = logging.NullHandler()
    h = None
except AttributeError:

    # 2.6 workaround

    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

    logging.NullHandler = NullHandler
    pass


class Pilot:
    """
    Main class.

    This class holds all the stuff and does all the things.
    """

    user_agent = 'Pilot/2.0'
    sslCert = ""
    sslPath = ""
    sslCertOrPath = ""
    args = None
    argv = None
    executable = __file__
    queuedata = None

    def __init__(self):
        """
        Initialization. Mostly setting up argparse, but also a few lines of resolving some early variables.
        :return:
        """
        self.dir = os.path.dirname(os.path.realpath(__file__))

        self.argParser = argparse.ArgumentParser(description="This is simplepilot. It will start your task... maybe..."
                                                             " in some distant future... on a specific environment..."
                                                             " with the help of some magic...")
        self.argParser.add_argument("--logconf", type=logging.config.fileConfig, default=os.path.join(self.dir,
                                                                                                      "loggers.ini"),
                                    help="specify logger parameters file", metavar="path/to/loggers.ini")
        testuserproxy = ""
        try:
            testuserproxy = '/tmp/x509up_u%s' % str(os.getuid())
        except AttributeError:
            # Wow, not UNIX. Nevermind, skip.
            pass

        self.argParser.add_argument("--cacert", default=os.environ.get('X509_USER_PROXY', testuserproxy),
                                    help="specify CA certificate or path for your transactions to server",
                                    metavar="path/to/your/certificate")
        self.argParser.add_argument("--capath", default=os.environ.get('X509_CERT_DIR',
                                                                       '/etc/grid-security/certificates'),
                                    help="specify CA path for certificates",
                                    metavar="path/to/certificates/")
        self.argParser.add_argument("--pandaserver", default="pandaserver.cern.ch",
                                    help="Panda server web address.",
                                    metavar="panda.example.com")
        self.argParser.add_argument("--jobserver", default="aipanda007.cern.ch",
                                    help="Panda job server web address.",
                                    metavar="pandajob.example.com")
        self.argParser.add_argument("--pandaserver_port", default=25085,
                                    type=int,
                                    help="Panda server port.",
                                    metavar="PORT")
        self.argParser.add_argument("--jobserver_port", default=25443,
                                    type=int,
                                    help="Panda job server port.",
                                    metavar="PORT")

        testqueuedata = "queuedata.json" if os.path.isfile("queuedata.json") else None
        self.argParser.add_argument("--queuedata", default=testqueuedata,
                                    type=lambda x: x if os.path.isfile(x) else testqueuedata,
                                    help="Preset queuedata file.",
                                    metavar="path/to/queuedata.json")
        self.argParser.add_argument("--queue", default='',
                                    help="Queue name",
                                    metavar="QUEUE_NAME")
        self.argParser.add_argument("--job_tag", default='prod',
                                    help="Job type tag. Eg. test, user, prod, etc...",
                                    metavar="tag")
        self.argParser.add_argument("--job_description", default=None,
                                    type=lambda x: x if os.path.isfile(x) else None,
                                    help="Job description file, preloaded from server. The contents must be JSON.",
                                    metavar="tag")
        self.argParser.add_argument("--no_job_update", action='store_true',
                                    help="Disable job server updates")
        self.argParser.add_argument("--simulate_rucio", action='store_true',
                                    help="Disable rucio, just simulate")
        self.argParser.add_argument("--harvester", action='store_true', default=False,
                                    help="Use Pilot with Harvester, disable rucio,job_update") 
        self.argParser.add_argument("--harvester_datadir", default=None,
                                    type=lambda x: x if os.path.exists(x) else None,
                                    help="Base directory from Harvester to store files",
                                    metavar="dir")
        self.argParser.add_argument("--harvester_workdir", default=None,
                                    type=lambda x: x if os.path.exists(x) else None,
                                    help="Harvester working directory",
                                    metavar="dir")
        self.argParser.add_argument("--harvester_eventStatusDumpJsonFile", default=None,
                                    help="Json file for harvester containing status of the processing")
        self.argParser.add_argument("--harvester_workerAttributesFile",default=None,
                                    help="Json file for harvester containing status of the panda job")


        self.logger = logging.getLogger("pilot")
        self.user_agent += " (Python %s; %s %s; rv:alpha) minipilot/daniel" % \
                           (sys.version.split(" ")[0],
                            platform.system(), platform.machine())

        self.node_name = socket.gethostbyaddr(socket.gethostname())[0]
        if "_CONDOR_SLOT" in os.environ:
            self.node_name = os.environ.get("_CONDOR_SLOT", '') + "@" + self.node_name

        self.pilot_id = self.node_name + (":%d" % os.getpid())

                
    def init_after_arguments(self):
        """
        Second step of initialization. After arguments received, pilot needs to set up some other variables.
        :return:
        """
        global log

        if os.path.exists(self.args.cacert):
            self.sslCert = self.args.cacert
        if os.path.exists(self.args.capath):
            self.sslPath = self.args.capath

        self.sslCertOrPath = self.sslCert if self.sslCert != "" else self.sslPath

        self.logger = logging.getLogger("pilot")
        log = self.logger

        if self.args.harvester is True :
            self.args.simulate_rucio = True
            self.args.no_job_updates = True  # should decide how to do it with harvester files
            
        
    def print_initial_information(self):
        """
        Pilot is initialized somehow, this initialization needs to be print out for information.
        :return:
        """
        if self.args is not None:
            log.info("Pilot is running.")
            log.info("Started with: %s" % " ".join(pipes.quote(x) for x in self.argv))
        log.info("User-Agent: " + self.user_agent)
        log.info("Node name: " + self.node_name)
        log.info("Pilot ID: " + self.pilot_id)

        log.info("Pilot is started from %s" % self.dir)
        log.info("Current working directory is %s" % os.getcwd())

        if self.args.harvester is True :
            log.info("Pilot is running in Harvester mode")
            if self.args.harvester_datadir is None : 
                # log it and throw error
                log.error("Harvester datadir (--harvester_datadir) not defined")
                raise NameError("Harvester datadir (--harvester_datadir) not defined")
            else:
                log.info("Harvester datadir: %s" % self.args.harvester_datadir)
            if self.args.harvester_workdir is None : 
                # log it and throw error
                log.error("Harvester workdir (--harvester_workdir) not defined")
                raise NameError("Harvester workdir (--harvester_workdir) not defined")
            else:
                log.info("Harvester workdir: %s" % self.args.harvester_workdir)
            if self.args.harvester_eventStatusDumpJsonFile is None :
               # log it and throw error
                log.error("Harvester eventStatusDumpJsonFile (--harvester_eventStatusDumpJsonFile) not defined")
                raise NameError("Harvester eventStatusDumpJsonFile (--harvester_eventStatusDumpJsonFile) not defined")
            else:
                log.info("Harvester eventStatusDumpJsonFile : %s" % self.args.harvester_eventStatusDumpJsonFile)
            if self.args.harvester_workerAttributesFile is None :
               # log it and throw error
                log.error("Harvester workerAttributesFile (--harvester_workerAttributesFile) not defined")
                raise NameError("Harvester workerAttributesFile (--harvester_workerAttributesFile) not defined")
            else:
                log.info("Harvester workerAttributesFile : %s" % self.args.harvester_workerAttributesFile)
        else:
            log.info("Printing requirements versions...")
        
            try:
                requirements = pip.req.parse_requirements(os.path.join(self.dir,"requirements.txt"),session=False)
                for req in requirements:
                    log.info("%s (%s)" % (req.name, req.installed_version))
            except TypeError:
                log.warn("Outdated version of PIP? Have you set up your environment properly? Skipping module info test...")
                log.warn("Pilot may crash at any time, be aware. And I can't provide you with module information, probably"
                     " the crash is caused by some outdated module.")
       

    def run(self, argv):
        """
        Main execution entrance point.
        :param argv: command line arguments
        :return:
        """
        self.executable = argv[0]
        self.argv = argv
        self.args = self.argParser.parse_args(argv[1:])
        print self.args
        print type(self.args)
        self.init_after_arguments()

        log.info("This pilot version is developed only for testing purposes, do not use it in production."
                 " You were warned.")

        self.print_initial_information()

        # noinspection PyBroadException
        try:
            self.get_queuedata()
            job = self.get_job()
            #log.info('job environment: %s' % str(os.environ))
            job.run()
        except Exception:
            log.error("During the run encountered uncaught exception.")
            log.error(traceback.format_exc())
            #dpbpass
            raise

    @staticmethod
    def time_iso8601(t=time.localtime(), timezone=time.timezone):
        """
        :param time(t): time to format down. Default to now.
        :param timezone: timezone of requested time. Default to local timezone.
        :return str: ISO-8601 compliant date/time string, timezone included
        """

        if timezone > 0:
            tz_sign = '-'
        else:
            tz_sign = '+'
        timezone_hours = int(timezone / 3600)

        return str("%s%s%02d%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", t), tz_sign, timezone_hours,
                                     int(timezone / 60 - timezone_hours * 60)))

    def curl_query(self, url, body=None, **kwargs):
        """
        Send query to server using cURL library. For simpleness does not test anything.

        :param url: URL of the resource
        :param body: string to be sent, if any.
        ... params to be passed to create_curl

        :return str: server response.
        """
        buf = StringIO()
        c = self.create_curl(**kwargs)
        c.setopt(c.URL, url)
        c.setopt(c.WRITEFUNCTION, buf.write)
        if body is not None:
            c.setopt(c.POSTFIELDS, body)
        c.perform()
        c.close()
        _str = str(buf.getvalue())
        buf.close()
        return _str

    def create_curl(self, ssl=False):
        """
        Creates cURL interface instance with required options and headers.
        :param Boolean(ssl): whether to set up SSL params or not. Default False.

        :return pycurl.Curl: cURL interface class
        """
        c = pycurl.Curl()
        if self.sslCertOrPath != "":
            c.setopt(c.CAPATH, self.sslCertOrPath)
        c.setopt(c.CONNECTTIMEOUT, 20)
        c.setopt(c.TIMEOUT, 120)
        c.setopt(c.HTTPHEADER, ['Accept: application/json;q=0.9,'
                                'text/html,application/xhtml+xml,application/xml;q=0.7,*/*;q=0.5',
                                'User-Agent: ' + self.user_agent])
        if ssl:
            if self.sslCert != "":
                c.setopt(c.SSLCERT, self.sslCert)
                c.setopt(c.SSLKEY, self.sslCert)
            if self.sslPath != "":
                c.setopt(c.CAPATH, self.sslPath)
            c.setopt(c.SSL_VERIFYPEER, False)
        return c

    def try_get_json_file(self, file_name):
        """
        Tries to read a file and parse it as JSON. All exceptions converted to warnings.

        :param file_name:

        :return: parsed JSON object or None on failure.
        """
        if isinstance(file_name, basestring) and file_name != "" and os.path.isfile(file_name):
            log.info("Trying to fetch JSON local file %s." % file_name)
            try:
                with open(file_name) as f:
                    j = json.load(f)
                    log.info("Successfully loaded file and parsed.")
                    return j
            except Exception as e:
                log.warning(str(e))
                log.warning("File loading and parsing failed.")
                pass
        return None

    def get_queuedata(self):
        """
        Retrieve queuedata from file or from server and store it into Pilot.queuedata.
        """
        log.info("Trying to get queuedata.")
        self.queuedata = self.try_get_json_file(self.args.queuedata)
        # if self.queuedata is None:
        #     self.queuedata = self.try_get_json_file("/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json")
        if self.queuedata is None and self.args.harvester is not True :
            log.info("Queuedata is not saved locally. Asking server.")

            # _str = self.curl_query("http://%s:%d/cache/schedconfig/%s.all.json" % (self.args.pandaserver,
            #                                                                        self.args.pandaserver_port,
            #                                                                        self.args.queue))

            _str = self.curl_query("http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?"
                                   "json&preset=schedconf.all&panda_queue=%s" % self.args.queue)

            confs = json.loads(_str)
            if self.args.queue in confs:
                self.queuedata = confs[self.args.queue]
        if self.queuedata is not None :
            log.info("Queuedata obtained.")
            log.debug("queuedata: " + json.dumps(self.queuedata, indent=4, sort_keys=True))
        else :
            log.error("Queuedata not found.")
            raise
        
    def get_job(self):
        """
        Gets job description from a file or from server.

        :return: job description.
        """
        log.info("Trying to get job description.")
        job_desc = self.try_get_json_file(self.args.job_description)
        if job_desc is None:
            if self.args.harvester is True :
                log.error(" pilot in Harvester mode - job json file not found ")
                raise NameError(" pilot in Harvester mode - job json file not found ")
            else:
                log.info("Job description is not saved locally. Asking server.")
                cpu_info = cpuinfo.get_cpu_info()
                mem_info = psutil.virtual_memory()
                disk_space = float(psutil.disk_usage(".").total) / 1024. / 1024.
                # diskSpace = min(diskSpace, 14336)  # I doubt this is necessary, so RM

                data = {
                    'cpu': float(cpu_info['hz_actual_raw'][0]) / 1000000.,
                    'mem': float(mem_info.total) / 1024. / 1024.,
                    'node': self.node_name,
                    'diskSpace': disk_space,
                    'getProxyKey': False,  # do we need it?
                    'computingElement': self.args.queue,
                    'siteName': self.args.queue,
                    'workingGroup': '',  # do we need it?
                    'prodSourceLabel': self.args.job_tag
                }

                _str = self.curl_query("https://%s:%d/server/panda/updateJob" % (self.args.jobserver,
                                                                             self.args.jobserver_port),
                                   ssl=True, body=urllib.urlencode(data))
                log.debug("Got from server: "+_str)
                try:
                    job_desc = json.loads(_str)
                except ValueError:
                    log.error("JSON parser failed.")
                    log.error("Got from server: " + _str)
                    raise

        log.info("Got job description.")
        from job import Job
        job = Job(self, description_fixer(job_desc))
        return job


# main
if __name__ == "__main__":
    """
    Main workflow is to create Pilot instance and run it with the command arguments.
    """
    pilot = Pilot()
    try:
        pilot.run(sys.argv)
    except:
        log.error("pilot returned error")
        raise
