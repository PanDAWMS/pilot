import urllib
import os
import json
import shlex
import pipes
import logging
import copy
from utility import Utility, touch

# TODO: Switch from external Rucio calls to internal ones. (Should consult with Mario)
# Before: fix platform dependencies in Rucio

# TODO: Rework queuedata overriding. Current version is a complete garbage.


class LoggingContext(object):
    """
    Class to override logging level for specified handler.
    Used for output header and footer of log file regardless the level.
    Automatically resets level on exit.

    Usage:

        with LoggingContext(handler, new_level):
            log.something

    """
    def __init__(self, handler, level=None):
        self.level = level
        self.handler = handler

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.handler.level
            self.handler.setLevel(self.level)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.handler.setLevel(self.old_level)


class Job(Utility):
    """
    This class holds a job and helps with it.
    Class presents also an interface to job description. Each field in it is mirrored to this class if there is no other
    specific variable, that shadows it.
    For example, job_id will be mirrored, but log_file would not, because there is specific class instance property,
    shadowing it.
    Every shadowing property should have exactly the same meaning and a great reason to shadow the one from description.
    Every such property must be documented.

    Attributes:
        id                      Alias to job_id
        state                   Job last state
        pilot                   Link to Pilot class instance
        description             Job description
        error_code              Job payload exit code
        no_update               Flag, specifying whether we will update server
        log_file                Job dedicated log file, into which the logs _are_ written. Shadowing log_file from
                                description, because that file is not a log file, but an archive containing it.
                                Moreover, log_file from description may contain not only log file.
                                :Shadowing property:
        log_archive             Detected archive extension. Mostly ".tgz"
        log                     Logger, used by class members.
        log_handler             File handler of real log file for logging. Added to root logger to catch outer calls.
        log_level               Filter used by handler to filter out unnecessary logging data.
                                Acquired from ''pilot.jobmanager'' logger configuration.
                                :Static:
        log_formatter           Formatter used by log handlers.
                                Acquired from ''pilot.jobmanager'' logger configuration.
                                :Static:
    """
    pilot = None
    description = None
    error_code = None
    no_update = False
    log_file = 'stub.job.log'
    log_archive = '.tgz'
    log = logging.getLogger()
    log_handler = None
    log_level = None
    log_formatter = None

    __state = "sent"
    __description_aliases = {
        'id': 'job_id'
    }
    __acceptable_log_wrappers = ["tar", "tgz", "gz", "gzip", "tbz2", "bz2", "bzip2"]

    def __init__(self, _pilot, _desc):
        """
        Initializer. Parses description.
        :param _pilot: Pilot class instance.
        :param _desc: Description object.
        :return:
        """
        Utility.__init__(self)
        self.log = logging.getLogger('pilot.jobmanager')
        self.pilot = _pilot
        if _pilot.args.no_job_update:
            self.no_update = True
        self.description = _desc
        _pilot.logger.debug(json.dumps(self.description, indent=4, sort_keys=True))
        self.parse_description()

    def __getattr__(self, item):
        """
        Reflection of description values into Job instance properties if they are not shadowed.
        If there is no own property with corresponding name, the value of Description is used.
        Params and return described in __getattr__ interface.
        """
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            if self.description is not None:
                if item in self.__description_aliases:
                    return self.description[self.__description_aliases[item]]
                if item in self.description:
                    return self.description[item]
            raise

    def __setattr__(self, key, value):
        """
        Reflection of description values into Job instance properties if they are not shadowed.
        If there is no own property with corresponding name, the value of Description is set.
        Params and return described in __setattr__ interface.
        """
        try:
            object.__getattribute__(self, key)
            object.__setattr__(self, key, value)
        except AttributeError:
            if self.description is not None:
                if key in self.__description_aliases:
                    self.description[self.__description_aliases[key]] = value
                elif self.description is not None and key in self.description:
                    self.description[key] = value
                return
            object.__setattr__(self, key, value)

    def get_key_value_for_queuedata(self, parameter):
        m = parameter.split('=', 1)
        key = m[0]
        value = None
        if len(m) > 1:
            try:
                value = json.loads(m[1])
            except ValueError:
                value = m[1]

        return key, value

    def prepare_command_params(self):
        """
        Splits command parameters and extracts queuedata modifications if present.

        Queuedata modification principles:
            Extraction is done from one of the parameter strings:
              1) ... --overwriteQueuedata key1=val1[ key2=val2[ ...]] -- ...
              2) ... --overwriteQueuedata key1=val1[ key2=val2[ ...]] -...
              3) ... --overwriteQueuedata key1=val1[ key2=val2[ ...]]

            Extraction starts from --overwriteQueuedata, then goes number of key=value pairs.
            Each value in pairs is either valid JSON or simple string.

            Example:
                'parameter list --overwriteQueuedata k1 k2=val k3=\'{"a":"b"}\' -- will lead to'
                modification in queuedata:
                {
                    k1: None
                    k2: "val"
                    k3: {a: "b"}
                }
                and parameter list:
                'parameter list will lead to'

            The end of the list is marked with:
              1) "--" (two dashes exactly), which is also stripped from parameter list;
              2) some parameter starting with "-" and is not just two dashes;
              3) EOL.

            If the next parameter (case 2) is --overwriteQueuedata, it is parsed all the same.
        """
        if isinstance(self.command_parameters, list):
            return
        params = shlex.split(str(self.command_parameters), True, True)
        overwriting = False
        new_params = []
        for param in params:
            if overwriting:
                if param.startswith('-'):
                    overwriting = False
                    if param == '--':
                        continue  # variant to end the parameter list
                else:
                    key, value = self.get_key_value_for_queuedata(param)
                    self.log.debug("Overwriting queuedata parameter \"%s\" to %s" % (key, json.dumps(value)))
                    self.pilot.queuedata[key] = value

            if not overwriting:
                if param == '--overwriteQueuedata':
                    self.log.debug("overwriteQueuedata found")
                    overwriting = True
                else:
                    new_params.append(param)

        self.log.debug("Prepared parameters: %s" % " ".join(pipes.quote(x) for x in new_params))
        self.command_parameters = new_params

    def init_logging(self):
        """
        Sets up logger handler for specified job log file. Beforehand it extracts job log file's real name and it's
        archive extension.
        """
        log_basename = self.description["log_file"]

        log_file = ''
        log_archive = ''

        for ext in self.__acceptable_log_wrappers:
            if log_file != '':
                break
            log_file, dot_ext, rest = log_basename.rpartition("." + ext)
            log_archive = dot_ext + rest

        if log_file == '':
            log_file = log_basename
            log_archive = ''

        h = logging.FileHandler(log_file, "w")
        if Job.log_level is None:
            Job.log_formatter = self.log.handlers.pop().formatter
            lvl = self.log.getEffectiveLevel()
            Job.log_level = lvl
        else:
            lvl = Job.log_level

        h.formatter = Job.log_formatter

        if lvl > logging.NOTSET:
            h.setLevel(lvl)

        self.log.setLevel(logging.NOTSET)  # save debug and others to higher levels.

        root_log = logging.getLogger()
        root_log.handlers.append(h)

        self.log_archive = log_archive
        self.log_file = log_file
        self.log_handler = h

        with LoggingContext(h, logging.NOTSET):
            self.log.info("Using job log file " + self.log_file)
            self.pilot.print_initial_information()
            self.log.info("Using effective log level " + logging.getLevelName(lvl))

    def parse_description(self):
        """
        Initializes description induced configurations: log handlers, queuedata modifications, etc.
        """
        self.init_logging()
        self.prepare_command_params()

    @property
    def state(self):
        """
        :return: Last job state
        """
        return self.__state

    def send_state(self):
        """
        Sends job state to the dedicated panda server.
        """
        if not self.no_update:
            self.log.info("Updating server job status...")
            data = {
                'node': self.pilot.node_name,
                'state': self.state,
                'jobId': self.id,
                # 'pilotID': self.pilot_id,
                'timestamp': self.pilot.time_iso8601(),
                'workdir': os.getcwd()
            }

            if self.error_code is not None:
                data["exeErrorCode"] = self.error_code

            _str = self.pilot.curl_query("https://%s:%d/server/panda/updateJob" % (self.pilot.args.jobserver,
                                                                                   self.pilot.args.jobserver_port),
                                         ssl=True, body=urllib.urlencode(data))
            self.log.debug("Got from server: " + _str)
            # jobDesc = json.loads(_str)
            # self.logger.info("Got from server: " % json.dumps(jobDesc, indent=4))

    @state.setter
    def state(self, value):
        """
        Sets new state and updates server.

        :param value: new job state.
        """
        if value != self.__state:
            self.log.info("Setting job state of job %s to %s" % (self.id, value))
            self.__state = value
            self.send_state()

    def prepare_log(self, include_files=None):
        """
        Prepares log file for stage out.
         May be called several times. The prime log file is not removed, so it will append new information (may be
         useful on log stage out failures to append the info).
         Automatically detects tarball and zipping based on previously extracted log archive extension.

        :param include_files: array of files to be included if tarball is used to aggregate log.
        """
        with LoggingContext(self.log_handler, logging.NOTSET):
            import shutil
            full_log_name = self.log_file + self.log_archive

            self.log.info("Preparing log file to send.")

            if os.path.isfile(full_log_name) and self.log_file != full_log_name:
                os.remove(full_log_name)

            mode = "w"
            if self.log_archive.find("g") >= 0:
                self.log.info("Detected compression gzip.")
                mode += ":gz"
                from gzip import open as compressor
            elif self.log_archive.find("2") >= 0:
                self.log.info("Detected compression bzip2.")
                mode += ":bz2"
                from bz2 import BZ2File as compressor  # NOQA: N813

            if self.log_archive.find("t") >= 0:
                self.log.info("Detected log archive: tar.")
                import tarfile

                with tarfile.open(full_log_name, mode) as tar:
                    if include_files is not None:
                        for f in include_files:
                            if os.path.exists(f):
                                self.log.info("Adding file %s" % f)
                                tar.add(f)
                    self.log.info("Adding log file... (must be end of log)")
                    tar.add(self.log_file)

                self.log.info("Finalizing log file.")
                tar.close()

            elif mode != "w":  # compressor
                self.log.info("Compressing log file... (must be end of log)")
                with open(self.log_file, 'rb') as f_in, compressor(full_log_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            elif self.log_file != full_log_name:
                self.log.warn("Compression is not known, assuming no compression.")
                self.log.info("Copying log file... (must be end of log)")

                shutil.copyfile(self.log_file, full_log_name)

        self.log.info("Log file prepared for stageout.")

    def rucio_info(self):
        """
        Logs basic Rucio information (basically whoami response)
        """
        if self.pilot.args.simulate_rucio:
            c, o, e = (0, "simulated", "")
        else:
            c, o, e = self.call(['rucio', 'whoami'])
        self.log.info("Rucio whoami responce: \n" + o)
        if e != '':
            self.log.warn("Rucio returned error(s): \n" + e)

    def stage_in(self):
        """
        Stages in files using Rucio.
        """
        self.state = 'stagein'
        self.rucio_info()
        for f in self.input_files:
            if self.pilot.args.simulate_rucio:
                touch(f)
                self.log.info("Simulated downloading " + f + " from " + self.input_files[f]['scope'])
            else:
                c, o, e = self.call(['rucio', 'download', '--no-subdir', self.input_files[f]['scope'] + ":" + f])

    def stage_out(self):
        """
        Stages out files using Rucio.
        """
        self.state = 'stageout'
        self.rucio_info()
        for f in self.output_files:
            if os.path.isfile(f) and self.description['log_file'] != f:
                if self.pilot.args.simulate_rucio:
                    self.log.info("Simulated uploading " + f + " to scope " + self.output_files[f]['scope'] +
                                  " and SE " + self.output_files[f]['storage_element'])
                else:
                    c, o, e = self.call(['rucio', 'upload', '--rse', self.output_files[f]['storage_element'], '--scope',
                                         self.output_files[f]['scope'], f])
            else:
                self.log.warn("Can not upload " + f + ", file does not exist.")
        self.prepare_log()
        with self.description['log_file'] as f:
            if os.path.isfile(f):
                if self.pilot.args.simulate_rucio:
                    self.log.info("Simulated uploading " + f + " to scope " + self.output_files[f]['scope'] +
                                  " and SE " + self.output_files[f]['storage_element'])
                else:
                    c, o, e = self.call(['rucio', 'upload', '--rse', self.output_files[f]['storage_element'], '--scope',
                                         self.output_files[f]['scope'], f])
            else:
                self.log.warn("Can not upload " + f + ", file does not exist.")

    def payload_run(self):
        """
        Runs payload.
        """
        self.state = 'running'
        args = copy.deepcopy(self.command_parameters)
        args.insert(0, self.command)

        self.log.info("Starting job cmd: %s" % " ".join(pipes.quote(x) for x in args))

        c, o, e = self.call(args)

        self.log.info("Job ended with status: %s" % c)
        self.log.info("Job stdout:\n%s" % o)
        self.log.info("Job stderr:\n%s" % e)
        self.error_code = c

        self.state = "holding"

    def run(self):
        """
        Main code of job manager.

        Stages in, executes and stages out the job.
        """
        self.state = 'starting'

        self.stage_in()
        self.payload_run()
        self.stage_out()

        self.state = 'finished'
