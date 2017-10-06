import urllib
import os
import os.path
import json
import shlex
import pipes
import logging
import copy
import shutil
import glob
import time
from utility import Utility, touch, construct_file_path, calc_adler32,timeStamp

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
        error_msg               Job payload error message
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
    error_msg = None
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
        self.log.info('in send_state')
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
        
        if self.pilot.args.harvester is True:
            self.log.debug('dumping worker attributes')
            self.dump_worker_attributes()
            self.log.debug('done dumping worker attributes')

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
            self.log.info("log_file - %s" %(self.log_file))
            self.log.info("log_archive - %s" %(self.log_archive))
            self.log.info("full_log_name - %s" %(full_log_name))

            if os.path.isfile(full_log_name) and self.log_file != full_log_name:
                self.log.info("delete full_log_name - %s" %(full_log_name))
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

    def is_log_file(self,filename):
        return '.log.' in filename

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

    def check_input_file(self,source_file,filename):
        # in a loop with a variable sleep statement look for input file and stat it
        found_file = False
        have_symlink = False
        continue_loop = True
        # sleep step ( sleep time is iloop*sleep_step)
        sleep_step = 10 
        iloop = 1
        while continue_loop :
            try:
                if (os.path.isfile(source_file)) :
                    try:
                        statinfo = os.stat(source_file)                         
                        found_file = True
                        #self.log.info('found file: %s' % str(source_file))
                    except:
                        self.log.error("can not stat file : %s " %str(source_file))
                        pass
                else:
                    self.log.error("Can not find input file : %s " %str(source_file))
                # now test for sym link
                if found_file :
                    try:
                        if (os.path.islink(filename)) :
                            have_symlink = True
                            #self.log.info('have simlink')
                        else :
                            os.symlink(source_file,filename)
                            self.log.info("Created sym link between %s and link - %s" %(source_file,filename))
                            have_symlink = True
                    except :
                        self.log.error("Can not created sym link between %s and link - %s" %(source_file,filename))
                        pass
            except:
                self.log.error("os.path.isfile exception for file : %s " %str(source_file))
                pass
            if found_file and have_symlink :
                continue_loop = False
            else:
                iloop+=1
                if iloop > 3:
                    continue_loop = False
                else :
                    # sleep 
                    time.sleep(iloop*sleep_step)
        # exit function
        return (found_file and have_symlink)


    def stage_in(self):
        """
        Stages in files using Rucio.
        """
        self.state = 'stagein'

        self.log.debug("input_files = %s" % str(self.input_files))
        
        if self.pilot.args.harvester is not True:
            self.rucio_info()

        have_symlink = False
        
        for f in self.input_files:
            self.log.debug('f = %s'%str(f))
            if self.pilot.args.harvester :
                # create a sym link between input files in harvester_datadir and lfn in pilot work area file. 
                try:
                    source_file = construct_file_path(self.pilot.args.harvester_datadir, self.input_files[f]['scope'], f)
                    new_pars = []
                    for x in self.command_parameters:
                        if f in x:
                           new_pars.append(x.replace(f,source_file))
                        else:
                           new_pars.append(x)
                    self.command_parameters = new_pars
                    self.log.debug('command_parameters = %s'%str(self.command_parameters))
                    # test if file exists
                    if (self.check_input_file(source_file,f)) :
                        #self.log.error("Can not find input file : %s " %str(source_file))
                        have_symlink = True
                        # replace input file in job?
                    else:
                        self.log.error('Cannot find input file: %s %s' % (str(source_file),f))
                except Exception,e:
                    self.log.warn("Warning could NOT created sym link between %s and link - %s: exception: %s" %(source_file,f,e))
                    
                    pass    
            else:
                if self.pilot.args.simulate_rucio:
                    touch(f)
                    self.log.info("Simulated downloading " + f + " from " + self.input_files[f]['scope'])
                else:
                    c, o, e = self.call(['rucio', 'download', '--no-subdir', self.input_files[f]['scope'] + ":" + f])
        if ( not have_symlink) :
            self.log.error("No symlink(s) exists between working directory and real input file(s)")
            raise ValueError('No symlink(s) exists between working directory and real input file(s)')
    
    def stage_out(self):
        """
        Stages out files using Rucio.  or with the harvester moves files to the harvester_datadir
        """
        self.state = 'stageout'
        
        filesDict = dict()
        outfileDict = dict()
        outFiles = []


        if self.pilot.args.harvester is False:            
            self.rucio_info()
        else:
            # check if job report json file exists
            readJsonPath=os.path.join(os.getcwd(),"jobReport.json")
            self.log.info('parsing %s' % readJsonPath)
            if os.path.exists(readJsonPath):
                # load json                                                                                                                                       
                try:
                    with open(readJsonPath) as jsonFile:
                        loadDict = json.load(jsonFile)

                    filesDict = loadDict['files']
                    outFiles = filesDict['output']
                    for outFile in outFiles:
                        for subFile in outFile['subFiles']:
                            outfileDict[subFile['name']] = subFile['file_guid']
                    # remove long athena log messages from jobReport
                    for e in loadDict['executor']:
                        e['logfileReport'] = {}
                    json.dump(loadDict,open(readJsonPath,'w'))
                except:
                    pass
            else:
                self.log.error('%s does not exist' % readJsonPath)


            
        #self.log.info("type self.output files %s" %(type(self.output_files)))
        self.log.info("%s",self.output_files)

        data = {}
        data_values=[]
        for f in self.output_files:
            self.log.info("processing file - %s" % f)
            # process log files
            if self.is_log_file(f):
                athena_logs=['transform.stdout','transform.stderr','%s.log' % self.id]
                athenamp_worker_logs = glob.glob('athenaMP-workers-*/*/AthenaMP.log')
                if len(athenamp_worker_logs) > 0:
                    athena_logs+= athenamp_worker_logs
                self.prepare_log(athena_logs)

            if os.path.isfile(f):
                if self.is_log_file(f):
                    file_type = 'log'
                else:
                    file_type = 'output'
                if self.pilot.args.harvester is True:
                    # move files to harvester data area
                    dest_file = construct_file_path(self.pilot.args.harvester_datadir, self.output_files[f]['scope'], f)
                    try:
                        if not os.path.exists(os.path.dirname(dest_file)):
                            os.makedirs(os.path.dirname(dest_file))
                        
                        self.log.info("move file from: %s to: %s" %(f,dest_file))
                        shutil.move(f,dest_file)
                        # need to calculate adler32 check sum of file
                        checksum =  calc_adler32(dest_file)
                        if f in outfileDict :
                            file_guid = outfileDict[f]
                            data_values.append({'eventStatus' : self.error_code,
                                                'path'        : dest_file,
                                                'type'        : file_type,
                                                'chksum'      : checksum,
                                                'guid'        : file_guid
                                            })
                        else:
                            data_values.append({'eventStatus' : self.error_code,
                                                'path'        : dest_file,
                                                'type'        : file_type,
                                                'chksum'      : checksum
                                            })
                    except:
                        self.log.error("Could not move %s to %s" %(f,dest_file))
                        raise
                else:
                    if self.pilot.args.simulate_rucio:
                        self.log.info("Simulated uploading " + f + " to scope " + self.output_files[f]['scope'] +
                                      " and SE " + self.output_files[f]['storage_element'])
                    else:
                        c, o, e = self.call(['rucio', 'upload', '--rse', self.output_files[f]['storage_element'], '--scope',
                                             self.output_files[f]['scope'], f])
            else:
                self.log.warn("Can not upload " + f + ", file does not exist.")
        self.log.debug('data_values: %s' % data_values)        
        with open(self.pilot.args.harvester_eventStatusDumpJsonFile,'w') as outfile:
            data[self.id]=data_values
            json.dump(data,outfile)
            self.log.info("write Event status to in json format to file %s" % self.pilot.args.harvester_eventStatusDumpJsonFile)
            self.log.info("save event status in json format to file %s:  %s" % (self.pilot.args.harvester_eventStatusDumpJsonFile,data[self.id]))

        '''
        # ignore this section of code
        # create a sym link between file jobReport.json and one level up
        if self.pilot.args.harvester :
            try:
                
                source_file = os.path.join(os.getcwd(),"jobReport.json")
                symlink = os.path.join(self.pilot.args.harvester_workdir,"jobReport.json")
                # test if file exists
                if (not os.path.isfile(source_file)) :
                    self.log.warn("Can not file input file : %s \n" %str(f))
                else :
                    # check if sym link exists if so remove it
                    if (os.path.exists(symlink)) :
                        os.unlink(symlink)
                    os.symlink(source_file,symlink)
                    self.log.info("Created sym link between %s and link - %s" %(source_file,symlink))
                    have_symlink = True
            except:
                self.log.warn("Warning could NOT created sym link between %s and link - %s" %(source_file,symlink))
                pass    
        '''

    def payload_run(self):
        """
        Runs payload.
        """
        self.state = 'running'
        args = copy.deepcopy(self.command_parameters)
        args.insert(0, self.command)

        self.log.info("Starting job cmd: %s" % " ".join(pipes.quote(x) for x in args))

        c,o,e = self.call(args,stdout_file='transform.stdout',stderr_file='transform.stderr')

        self.log.info("Job ended with status: %s" % c)
        self.log.info(o)
        self.log.info(e)
        #self.log.info("Job stdout:\n%s" % o)
        #self.log.info("Job stderr:\n%s" % e)
        self.error_code = c
        if self.error_code != 0:
            # extract error message
            c,o,e = self.call('tail -n 7 transform.stdout | grep CRITICAL',shell=True)

            self.error_msg = o 


        self.state = "holding"

    def dump_worker_attributes(self):
        self.log.debug('in dump_worker_attributes')
        # Harvester only expects the attributes files for certain states.
        if self.__state in ['finished','failed','running']:
            with open(self.pilot.args.harvester_workerAttributesFile,'w') as outputfile:
                workAttributes = {'jobStatus':self.state}
                workAttributes['workdir'] = os.getcwd()
                workAttributes['messageLevel'] = logging.getLevelName(self.log.getEffectiveLevel())
                workAttributes['timestamp'] = timeStamp()
                workAttributes['cpuConversionFactor'] = 1.0
                
                coreCount = None
                nEvents = None
                dbTime = None
                dbData = None
                workDirSize = None

                if 'ATHENA_PROC_NUMBER' in os.environ:
                     workAttributes['coreCount'] = os.environ['ATHENA_PROC_NUMBER']
                     coreCount = os.environ['ATHENA_PROC_NUMBER']

                # check if job report json file exists
                jobReport = None
                readJsonPath=os.path.join(os.getcwd(),"jobReport.json")
                self.log.debug('parsing %s' % readJsonPath)
                if os.path.exists(readJsonPath):
                    # load json                                                                                                                                       
                    with open(readJsonPath) as jsonFile:
                        jobReport = json.load(jsonFile)
                if jobReport is not None:
                    if 'resource' in jobReport:
                        if 'transform' in jobReport['resource']:
                            if 'processedEvents' in jobReport['resource']['transform']:
                                workAttributes['nEvents'] = jobReport['resource']['transform']['processedEvents']
                                nEvents = jobReport['resource']['transform']['processedEvents']
                            if 'cpuTimeTotal' in jobReport['resource']['transform']:
                                workAttributes['cpuConsumptionTime'] = jobReport['resource']['transform']['cpuTimeTotal']
                            
                        if 'machine' in jobReport['resource']:
                            if 'node' in jobReport['resource']['machine']:
                                workAttributes['node'] = jobReport['resource']['machine']['node']
                            if 'model_name' in jobReport['resource']['machine']:
                                workAttributes['cpuConsumptionUnit'] = jobReport['resource']['machine']['model_name']
                        
                        if 'dbTimeTotal' in jobReport['resource']:
                            dbTime = jobReport['resource']['dbTimeTotal']
                        if 'dbDataTotal' in jobReport['resource']:
                            dbData = jobReport['resource']['dbDataTotal']

                        if 'executor' in jobReport['resource']:
                            if 'memory' in jobReport['resource']['executor']:
                                for transform_name,attributes in jobReport['resource']['executor'].iteritems():
                                    if 'Avg' in attributes['memory']:
                                        for name,value in attributes['memory']['Avg'].iteritems():
                                            try:
                                                workAttributes[name] += value
                                            except:
                                                workAttributes[name] = value
                                    if 'Max' in attributes['memory']:
                                        for name,value in attributes['memory']['Max'].iteritems():
                                            try:
                                                workAttributes[name] += value
                                            except:
                                                workAttributes[name] = value
                            
                    if 'exitCode' in jobReport: 
                        workAttributes['transExitCode'] = jobReport['exitCode']
                        workAttributes['exeErrorCode'] = jobReport['exitCode']
                    if 'exitMsg'  in jobReport: 
                        workAttributes['exeErrorDiag'] = jobReport['exitMsg']
                    if 'files' in jobReport:
                        if 'input' in jobReport['files']:
                            if 'subfiles' in jobReport['files']['input']:
                                workAttributes['nInputFiles'] = len(jobReport['files']['input']['subfiles'])

                    if coreCount and nEvents and dbTime and dbData:
                        c,o,e = self.call('du -s',shell=True)
                        workAttributes['jobMetrics'] = 'coreCount=%s nEvents=%s dbTime=%s dbData=%s workDirSize=%s' % (
                              coreCount,nEvents,dbTime,dbData,o.split()[0] )

                else:
                    self.log.debug('no jobReport object')
                self.log.info('output worker attributes for Harvester: %s' % workAttributes)
                json.dump(workAttributes,outputfile)
        else:
            self.log.debug(' %s is not a good state' % self.state)
        self.log.debug('exit dump worker attributes')

    def run(self):
        """
        Main code of job manager.

        Stages in, executes and stages out the job.
        """
        self.state = 'starting'
         
        try:
            self.stage_in()
            self.payload_run()
            self.stage_out()
        except:
            self.state = 'failed'
            
            raise
        
        if self.error_code != 0:
            self.state = 'failed'
            self.log.error('failing job')
        else:
            self.state = 'finished'
            self.log.info('job finished')
        

