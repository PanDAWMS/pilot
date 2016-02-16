import commands
import os
import shutil
import sys
import time
import traceback
import json
import pickle

from Logger import Logger

import logging
logging.basicConfig(level=logging.DEBUG)

class HPCManager:
    def __init__(self, logFileName=None):
        self.__globalWorkingDir = None
        self.__localWorkingDir = None

        self.__jobStateFile = 'HPCManagerState.json'
        self.__logFileName = logFileName
        self.__log= Logger(logFileName)
        self.__isFinished = False

        # HPC resource information
        self.__queue = None
        self.__backfill_queue = None
        self.__nodes = None
        self.__cpuPerNode = None
        self.__ATHENA_PROC_NUMBER = None
        self.__repo = None
        self.__mppwidth = None
        self.__mppnppn = None
        self.__walltime = None
        self.__walltime_m = 0
        # Number of AthenaMP workers per rank
        self.__ATHENA_PROC_NUMBER = 2
        self.__eventsPerWorker = 3
        self.__failedPollTimes = 0
        self.__lastState = None
        self.__lastTime = time.time()

        self.__copyInputFiles = copyInputFiles

        self.__mode = None
        self.__jobs = {}
        self.__jobsFile = None
        self.__eventRanges = None
        self.__eventRangesFile = None
        self.__jobid = None
        self.__stageout_threads = 1
        self.__pandaJobStateFile = None

        self.__pluginName = 'pbs'
        self.__plugin = None
        self.__localSetup = None

        self.__firstJobWorkDir = None

    def __init__(self, globalWorkingDir=None, localWorkingDir=None, logFileName=None, copyInputFiles=False):
        self.__globalWorkingDir = globalWorkingDir
        # self.__globalYodaDir = os.path.join(globalWorkingDir, 'Yoda')
        self.__globalYodaDir = self.__globalWorkingDir
        if not os.path.exists(self.__globalYodaDir):
            os.makedirs (self.__globalYodaDir)
        self.__localWorkingDir = localWorkingDir
        if self.__localWorkingDir is None:
            self.__localWorkingDir = self.__globalYodaDir

        self.__jobStateFile = os.path.join(self.__globalYodaDir, 'HPCManagerState.json')
        self.__logFileName = logFileName
        self.__log= Logger(logFileName)
        self.__isFinished = False

        # HPC resource information
        self.__queue = None
        self.__backfill_queue = None
        self.__nodes = None
        self.__cpuPerNode = None
        self.__repo = None
        self.__mppwidth = None
        self.__mppnppn = None
        self.__walltime = None
        self.__walltime_m = 0
        # Number of AthenaMP workers per rank
        self.__ATHENA_PROC_NUMBER = 2
        self.__eventsPerWorker = 3
        self.__failedPollTimes = 0
        self.__lastState = None
        self.__lastTime = time.time()

        self.__copyInputFiles = copyInputFiles

        self.__mode = None
        self.__jobs = {}
        self.__jobsFile = None
        self.__eventRanges = {}
        self.__eventRangesFile = None
        self.__jobid = None
        self.__stageout_threads = 1
        self.__pandaJobStateFile = None
        self.__yodaToOS = False

        self.__pluginName = 'pbs'
        self.__plugin = None
        self.__localSetup = None

        self.__firstJobWorkDir = None

    def setPandaJobStateFile(self, file):
        self.__pandaJobStateFile = file

    def getPandaJobStateFile(self):
        return self.__pandaJobStateFile

    def setStageoutThreads(self, num):
        self.__stageout_threads = num

    def getStageoutThreads(self):
        return self.__stageout_threads

    def prepare(self):
        if self.__globalWorkingDir != self.__localWorkingDir:
            self.__log.info("Global Working directory is different with local working directory.")
            # copy job file to local working directory
            cmd = "cp -r "+self.__globalWorkingDir+"/HPCJob.py "+self.__globalWorkingDir+"/EventServer "+self.__globalWorkingDir+"/pandayoda/ "+self.__localWorkingDir
            self.__log.info("Copying script to local working directory: %s" % cmd)
            status, output = commands.getstatusoutput(cmd)
            self.__log.info("Executing command result: (status: %s, output: %s)" %(status, output))

    def getHPCResources(self, partition, max_nodes=None, min_nodes=2, min_walltime_m=30):
        return self.__plugin.getHPCResources(partition, max_nodes, min_nodes, min_walltime_m)

    def getMode(self, defaultResources):
        mode = defaultResources['mode']
        self.__mode = mode

        self.__log.info("Running mode: %s" % mode)
        if defaultResources['partition'] is None:
            self.__log.info("Partition is not defined, will use normal mode")
            res = None
            self.__mode = 'normal'

        return self.__mode

    def setupPlugin(self, pluginName):
        # pluginName = defaultResources.get('plugin', 'pbs')
        self.__pluginName = pluginName
        plugin = 'HPC.HPCManagerPlugins.%s.%s' % (pluginName, pluginName)
        self.__log.info("HPCManager plugin: %s" % plugin)

        components = plugin.split('.')
        mod = __import__('.'.join(components[:-1]))
        for comp in components[1:]:
            mod = getattr(mod, comp)
        self.__plugin = mod(self.__logFileName)
        self.__log.info("HPCManager plugin is setup: %s" % self.__plugin)

    def setLocalSetup(self, setup):
        self.__localSetup = setup

    def getFreeResources(self, defaultResources):
        mode = self.getMode(defaultResources)

        res = None
        if mode == 'backfill':
            res = {}
            while not res:
                self.__log.info("Run in backfill mode, waiting to get resources.")
                time.sleep(60)
                res = self.getHPCResources(defaultResources['partition'], int(defaultResources['max_nodes']), int(defaultResources['min_nodes']), int(defaultResources['min_walltime_m']))

        self.__log.info("Get resources: %s" % res)
        nodes = int(defaultResources['min_nodes'])
        walltime = 0
        if res:
             for n in sorted(res.keys(), reverse=True):
                 if int(defaultResources['min_walltime_m']) * 60 <= res[n] and nodes <= n:
                     nodes = n
                     walltime = res[n] / 60
                     walltime = walltime - 2
                     break

        if walltime <= 0:
            walltime = int(defaultResources['walltime_m'])
            nodes = int(defaultResources['nodes'])
            self.__log.info("Cannot get resource, using default one: walltime in minutes: %s, nodes: %s" % (walltime, nodes))

        self.__queue = defaultResources['queue']
        self.__backfill_queue = defaultResources['backfill_queue']
        self.__nodes = nodes
        if self.__nodes > int(defaultResources['max_nodes']):
            self.__nodes = int(defaultResources['max_nodes'])
        self.__cpuPerNode = int(defaultResources['cpu_per_node'])
        self.__mppwidth = int(self.__nodes) * int(self.__cpuPerNode)
        self.__mppnppn = defaultResources['mppnppn']
        initialtime_m = int(defaultResources['initialtime_m'])
        time_per_event_m = int(defaultResources['time_per_event_m'])
        if mode == 'backfill':
            self.__queue = self.__backfill_queue
            self.__log.info("Run in backfill mode, using queue: %s" % self.__queue)

        if walltime > int(defaultResources['max_walltime_m']):
            walltime = int(defaultResources['max_walltime_m'])

        self.__walltime_m = walltime
        h, m = divmod(walltime, 60)
        self.__walltime = "%d:%02d:%02d" % (h, m, 0)
        self.__eventsPerWorker = (int(walltime) - int(initialtime_m))/time_per_event_m
        if self.__eventsPerWorker < 1:
            self.__eventsPerWorker = 1
        self.__ATHENA_PROC_NUMBER = defaultResources['ATHENA_PROC_NUMBER']
        self.__repo = defaultResources['repo']
        self.__yodaToOS = defaultResources.get('yoda_to_os', False)
        self.__copyOutputToGlobal = defaultResources.get('copyOutputToGlobal', False)
        self.__setup = defaultResources.get('setup', None)
        self.__esPath = defaultResources.get('esPath', None)

    def getCoreCount(self):
        return int(self.__mppwidth)

    def getEventsNumber(self):
        # walltime is minutes
        # 1 Yoda and (self.__nodes -1) Droid
        # plus 1 cached event per node
        #return int(self.__eventsPerWorker) * (int(self.__nodes) -1) * int(self.__ATHENA_PROC_NUMBER) + (int(self.__nodes) -1) * 1
        return int(self.__eventsPerWorker) * (int(self.__nodes) -0) * int(self.__ATHENA_PROC_NUMBER)

    def initJobs(self, jobs, eventRanges):
        self.__log.info("initJobs: %s" % jobs)
        ranks = [i for i in range(1, self.__nodes)]
        totalNeededRanks = 1
        for jobId in jobs:
            # job = {"TokenExtractCmd": tokenExtractorCommand, "AthenaMPCmd": athenaMPCommand}
            job = jobs[jobId]
            job['JobId'] = jobId
            job["AthenaMPCmd"] = "export TRF_ECHO=1; " + job["AthenaMPCmd"]
            job["CopyInputFiles"] = self.__copyInputFiles
            job["LocalWorkingDir"] = self.__localWorkingDir
            job["ATHENA_PROC_NUMBER"] = self.__ATHENA_PROC_NUMBER
            job['neededRanks'] = 0
            job['ranks'] = []
            job['yodaToOS'] = self.__yodaToOS
            job['copyOutputToGlobal'] = self.__copyOutputToGlobal
            job['setup'] = self.__setup
            job['esPath'] = self.__esPath

            eventsPerNode = int(self.__ATHENA_PROC_NUMBER) * (int(self.__eventsPerWorker))
            if jobId in eventRanges:
                job['neededRanks'] = len(eventRanges[jobId]) / eventsPerNode + (len(eventRanges[jobId]) % eventsPerNode + eventsPerNode - 1)/eventsPerNode
                if len(eventRanges[jobId]) >= eventsPerNode * 4:
                    job['neededRanks'] += 2
                elif len(eventRanges[jobId]) > eventsPerNode:
                    job['neededRanks'] += 1
                totalNeededRanks += job['neededRanks']
            self.__jobs[jobId] = job

            if self.__firstJobWorkDir is None:
                self.__firstJobWorkDir = job['GlobalWorkingDir']

        if totalNeededRanks < self.__nodes:
            self.__nodes = totalNeededRanks
        # if self.__nodes < 2:
        #     self.__nodes = 2
        self.__mppwidth = int(self.__nodes) * int(self.__cpuPerNode)

        self.__jobsFile = os.path.join(self.__globalYodaDir, "HPCJobs.json")
        with open(self.__jobsFile, 'w') as outputFile:
           json.dump(self.__jobs, outputFile)

        self.__eventRanges = eventRanges
        self.__eventRangesFile = os.path.join(self.__globalYodaDir, "JobsEventRanges.json")
        with open(self.__eventRangesFile, 'w') as outputFile:
           json.dump(self.__eventRanges, outputFile)

    def getJobsRanks(self):
        jobRanks = {}
        for jobId in self.__jobs:
            jobRanks[jobId] = self.__jobs[jobId]['ranks']

    # will remove
    def initEventRanges(self, eventRanges):
        self.__eventRanges = eventRanges
        self.__eventRangesFile = os.path.join(self.__globalYodaDir, "EventRanges.json")
        with open(self.__eventRangesFile, 'w') as outputFile:
           json.dump(self.__eventRanges, outputFile)

    # will remove
    def initJobRanks(self):
        numRanges = 0
        if self.__eventRanges:
            numRanges = len(self.__eventRanges)
        eventsPerNode = int(self.__ATHENA_PROC_NUMBER) * (int(self.__eventsPerWorker) - 1)
        if eventsPerNode == 0:
            eventsPerNode = 1
        nodes = numRanges/eventsPerNode + (numRanges%eventsPerNode + eventsPerNode - 1)/eventsPerNode + 1
        if nodes < int(self.__nodes):
            self.__nodes = nodes
            self.__mppwidth = int(self.__nodes) * int(self.__cpuPerNode)
            if self.__nodes <= 5 and self.__mode != 'backfill':
                 # self.__walltime_m =  self.__walltime_m * 2
                 h, m = divmod(self.__walltime_m, 60)
                 self.__walltime = "%d:%02d:%02d" % (h, m, 0)

    def isLocalProcess(self):
        return self.__plugin.isLocalProcess()

    def submit(self):
        if not self.__jobs:
            self.__log.info("No prepared jobs available. will not submit any jobs.")
            return
        for i in range(5):
            if self.__plugin.getName() == 'arc12233' and self.__firstJobWorkDir is not None:
                status, jobid = self.__plugin.submitJob(self.__globalWorkingDir, self.__firstJobWorkDir, self.__firstJobWorkDir, self.__queue, self.__repo, self.__mppwidth, self.__mppnppn, self.__walltime, self.__nodes, localSetup=self.__localSetup, cpuPerNode=self.__cpuPerNode)
            else:
                status, jobid = self.__plugin.submitJob(self.__globalWorkingDir, self.__globalYodaDir, self.__localWorkingDir, self.__queue, self.__repo, self.__mppwidth, self.__mppnppn, self.__walltime, self.__nodes, localSetup=self.__localSetup, cpuPerNode=self.__cpuPerNode)
            if status != 0:
                self.__log.info("Failed to submit this job to HPC. will sleep one minute and retry")
                time.sleep(60)
            else:
                self.__jobid = jobid
                break
        if status != 0:
            self.__log.info("Failed to submit this job to HPC. All retries finished. will fail") 

    def saveState(self):
        hpcState = {'GlobalWorkingDir': self.__globalWorkingDir, 'Plugin':self.__pluginName, 'JobID': self.__jobid, 'JobCommand': sys.argv, 'JobStateFile': self.__pandaJobStateFile, 'StageoutThreads': self.__stageout_threads}
        with open(self.__jobStateFile, 'w') as outputFile:
           json.dump(hpcState, outputFile)

    def recoveryState(self):
        if os.path.exists(self.__jobStateFile):
            tmpFile = open(self.__jobStateFile)
            hpcState = json.load(tmpFile)
            tmpFile.close()
            self.__globalWorkingDir = hpcState['GlobalWorkingDir']
            self.__jobid = hpcState['JobID']
            self.__pluginName = hpcState['Plugin']
            self.__stageout_threads = hpcState['StageoutThreads']
            self.setupPlugin(self.__pluginName)

    def poll(self):
        if self.__plugin.isLocalProcess():
            return 'Complete'

        if self.__jobid is None:
            self.__log.info("HPC job id is None, will return failed.")
            self.__isFinished = True
            return 'Failed'

        state = self.__plugin.poll(self.__jobid)
        if self.__lastState is None or self.__lastState != state or time.time() > self.__lastTime + 60*5:
            self.__log.info("HPC job state is: %s" %(state))
            self.__lastState = state
            self.__lastTime = time.time()
        if state in ['Complete', 'Failed']:
            self.__isFinished = True
        return state

    def checkHPCJobLog(self):
        logFile = os.path.join(self.__globalYodaDir, "athena_stdout.txt")
        command = "grep 'HPCJob-Yoda failed' " + logFile
        status, output = commands.getstatusoutput(command)
        if status == 0:
            return -1, "HPCJob-Yoda failed"
        return 0, None

    def getOutputs(self):
        outputs = []
        all_files = os.listdir(self.__globalYodaDir)
        for file in all_files:
            if file.endswith(".dump"):
                filename = os.path.join(self.__globalYodaDir, file)
                handle = open(filename)
                for line in handle:
                    line = line.replace("  ", " ")
                    eventRange, status, output = line.split(" ")
                    if status == 'finished':
                        outputFileName = output.split(",")[0]
                        outputs.append((eventRange, status, outputFileName))
                    else:
                        outputs.append((eventRange, status, output))
                handle.close()
                os.rename(filename, filename + ".BAK")
        return outputs

    def isFinished(self):
        if self.__jobid is None:
            self.__log.info("HPC job id is None. Finished")
            self.__isFinished = True
        return self.__isFinished

    def finishJob(self):
        if self.__jobid and self.__plugin:
            self.__plugin.delete(self.__jobid)

    def flushOutputs(self):
        try:
            self.__log.debug("Flush Yoda outputs")
            from pandayoda.yodacore import Database
            db = Database.Backend(self.__globalYodaDir)
            db.dumpUpdates(True)
        except:
            self.__log.debug("Failed to flush outputs: %s" % traceback.format_exc())

    def old_postRun(self):
        try:
            self.__log.debug("postRun")
            all_files = os.listdir(self.__globalYodaDir)
            for file in all_files:
                path = os.path.join(self.__globalYodaDir, file)
                if file.startswith("rank_") and os.path.isdir(path):
                    self.__log.debug("Found dir %s" % path)
                    for jobId in self.__jobs:
                        dest_dir = os.path.join(self.__jobs[jobId]['GlobalWorkingDir'], file)
                        if not os.path.exists(dest_dir):
                            os.makedirs(dest_dir)
                        for localFile in os.listdir(path):
                            localPath = os.path.join(path, localFile)
                            self.__log.debug("Copying %s to %s" % (localPath, dest_dir))
                            if os.path.isdir(localPath):
                                try:
                                    shutil.copytree(localPath, dest_dir)
                                except:
                                    self.__log.warning("Failed to copy %s to %s: %s" % (localPath, dest_dir, traceback.format_exc()))
                            else:
                                try:
                                    shutil.copy(localPath, dest_dir)
                                except:
                                    self.__log.warning("Failed to copy %s to %s: %s" % (localPath, dest_dir, traceback.format_exc()))
        except:
            self.__log.warning("Failed to post run: %s" % traceback.format_exc())

    def postRun(self):
        return
