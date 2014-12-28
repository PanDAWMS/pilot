import commands
import os
import sys
import time
import json
import pickle

from Logger import Logger

import logging
logging.basicConfig(level=logging.DEBUG)

class HPCManager:
    def __init__(self, globalWorkingDir=None, localWorkingDir=None, logFileName=None, poolFileCatalog=None, inputFiles=None, copyInputFiles=False):
        self.__globalWorkingDir = globalWorkingDir
        self.__localWorkingDir = localWorkingDir
        if self.__localWorkingDir is None:
            self.__localWorkingDir = self.__globalWorkingDir

        self.__log= Logger(logFileName)
        self.__isFinished = False

        # HPC resource information
        self.__queue = None
        self.__mppwidth = None
        self.__mppnppn = None
        self.__walltime = None
        # Number of AthenaMP workers per rank
        self.__ATHENA_PROC_NUMBER = 2
        self.__eventsPerWorker = 3
        self.__failedPollTimes = 0
        self.__lastState = None
        self.__lastTime = time.time()

        self.__poolFileCatalog = poolFileCatalog
        self.__inputFiles = inputFiles
        self.__copyInputFiles = copyInputFiles

        self.__mode = None

    def initJob(self, job):
        self.__log.info("initJob: %s" % job)
        # job = {"TokenExtractCmd": tokenExtractorCommand, "AthenaMPCmd": athenaMPCommand}
        self.__job = job
        self.__job["AthenaMPCmd"] = "export ATHENA_PROC_NUMBER=" + str(self.__ATHENA_PROC_NUMBER) + "; export TRF_ECHO=1; " + self.__job["AthenaMPCmd"]
        self.__job["PoolFileCatalog"] = self.__poolFileCatalog
        self.__job["InputFiles"] = self.__inputFiles
        self.__job["CopyInputFiles"] = self.__copyInputFiles

        self.__jobFile = "HPCJob.json"
        with open(self.__jobFile, 'w') as outputFile:
           json.dump(self.__job, outputFile)

    def initEventRanges(self, eventRanges):
        self.__eventRanges = eventRanges
        self.__eventRangesFile = "EventRanges.json"
        with open(self.__eventRangesFile, 'w') as outputFile:
           json.dump(self.__eventRanges, outputFile)

    def prepare(self):
        if self.__globalWorkingDir != self.__localWorkingDir:
            self.__log.info("Global Working directory is different with local working directory.")
            # copy job file to local working directory
            cmd = "cp -r "+self.__globalWorkingDir+"/HPCJob.py "+self.__globalWorkingDir+"/EventServer "+self.__globalWorkingDir+"/pandayoda/ "+self.__localWorkingDir
            self.__log.info("Copying script to local working directory: %s" % cmd)
            status, output = commands.getstatusoutput(cmd)
            self.__log.info("Executing command result: (status: %s, output: %s)" %(status, output))

    def getHPCResources(self, partition, max_nodes=None, min_nodes=2, min_walltime_m=30):
        # copied from RunJobEdison
        cmd = 'showbf -p %s' % partition
        self.__log.info("Executing command: '%s'" % cmd)
        res_tuple = commands.getstatusoutput(cmd)
        self.__log.info("Executing command output: %s" % str(res_tuple))
        showbf_str = ""
        if res_tuple[0] == 0:
            showbf_str = res_tuple[1]

        res = {}
        self.__log.info("Available resources in %s  partition" % partition)
        self.__log.info(showbf_str)
        if showbf_str:
            shobf_out = showbf_str.splitlines()
            self.__log.info("Fitted resources")
            for l in shobf_out[2:]:
                d = l.split()
                nodes = int(d[2])

                if nodes < int(min_nodes):
                    continue

                if not d[3] == 'INFINITY':
                    wal_time_arr =  d[3].split(":")
                    if len(wal_time_arr) < 4:
                        wal_time_sec = int(wal_time_arr[0])*(60*60) + int(wal_time_arr[1])*60 + int(wal_time_arr[2])
                        if wal_time_sec > 24 * 3600:
                            wal_time_sec = 24 * 3600
                    else:
                        wal_time_sec = 24 * 3600
                        #if nodes > 1:
                        #    nodes = nodes - 1
                else:
                    wal_time_sec = 12 * 3600
               
                # Fitting Hopper policy
                # https://www.nersc.gov/users/computational-systems/hopper/running-jobs/queues-and-policies/
                nodes = max_nodes if nodes > max_nodes else nodes   
                   

                if nodes < 682 and wal_time_sec > 48 * 3600:
                    wal_time_sec = 48 * 3600
                elif nodes < 4096 and wal_time_sec > 36 * 3600:
                    wal_time_sec = 36 * 3600
                elif nodes < 5462 and wal_time_sec > 12 * 3600:
                    wal_time_sec = 12 * 3600
                elif wal_time_sec > 12 * 3600:
                    wal_time_sec = 12 * 3600

                if wal_time_sec < int(min_walltime_m) * 60:
                    continue

                self.__log.info("Nodes: %s, Walltime (str): %s, Walltime (min) %s" % (nodes, d[3], wal_time_sec/60 ))

                res.update({nodes:wal_time_sec})
        else:
            self.__log.info("No availble resources. Default values will be used.")
        self.__log.info("Get resources: %s" % res)
        return res

    def getMode(self, defaultResources):
        mode = defaultResources['mode']
        self.__mode = mode

        self.__log.info("Running mode: %s" % mode)
        if defaultResources['partition'] is None:
            self.__log.info("Partition is not defined, will use normal mode")
            res = None
            self.__mode = 'normal'

        return self.__mode

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

        h, m = divmod(walltime, 60)
        self.__walltime = "%d:%02d:%02d" % (h, m, 0)
        self.__eventsPerWorker = (int(walltime) - int(initialtime_m) - 5)/time_per_event_m
        if self.__eventsPerWorker < 1:
            self.__eventsPerWorker = 1
        self.__ATHENA_PROC_NUMBER = defaultResources['ATHENA_PROC_NUMBER']
        self.__repo = defaultResources['repo']

    def getCoreCount(self):
        return self.__mppwidth

    def getEventsNumber(self):
        # walltime is minutes
        # 1 Yoda and (self.__nodes -1) Droid
        # plus 1 cached event per node
        #return int(self.__eventsPerWorker) * (int(self.__nodes) -1) * int(self.__ATHENA_PROC_NUMBER) + (int(self.__nodes) -1) * 1
        return int(self.__eventsPerWorker) * (int(self.__nodes) -1) * int(self.__ATHENA_PROC_NUMBER)

    def submit(self):
        for i in range(5):
            status, jobid = self.submitJob()
            if status != 0:
                self.__log.info("Failed to submit this job to HPC. will sleep one minute and retry")
                time.sleep(60)
            else:
                break
        if status != 0:
            self.__log.info("Failed to submit this job to HPC. All retries finished. will fail") 
  
    def submitJob(self):
        submit_script = "#!/bin/bash -l" + "\n"
        submit_script += "#PBS -q " + self.__queue + "\n"
        if self.__repo:
            submit_script += "#PBS -A " + self.__repo + "\n"
        submit_script += "#PBS -l mppwidth=" + str(self.__mppwidth) + "\n"
        #submit_script += "#PBS -l mppnppn=" + str(self.__mppnppn) + "\n"
        submit_script += "#PBS -l walltime=" + self.__walltime + "\n"
        submit_script += "#PBS -N ES_job" + "\n"
        submit_script += "#PBS -j oe" + "\n"
        submit_script += "#PBS -o athena_stdout.txt" + "\n"
        submit_script += "#PBS -e athena_stderr.txt" + "\n"
        submit_script += "cd $PBS_O_WORKDIR" + "\n"
        submit_script += "module load mpi4py" + "\n"
        submit_script += "source /project/projectdirs/atlas/sw/python-yampl/setup.sh" + "\n"

        #submit_script += "aprun -n " + str(self.__nodes) + " -N " + str(self.__mppnppn) + " -d " + str(self.__ATHENA_PROC_NUMBER) + " -cc none python-mpi " + os.path.join(self.__globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+self.__globalWorkingDir+" --localWorkingDir="+self.__localWorkingDir+""
        submit_script += "aprun -n " + str(self.__nodes) + " -N " + str(self.__mppnppn) + " -cc none python-mpi " + os.path.join(self.__globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+self.__globalWorkingDir+" --localWorkingDir="+self.__localWorkingDir+""
        ###cmd = "mpiexec -n 2 python " + os.path.join(self.__globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+self.__globalWorkingDir+" --localWorkingDir="+self.__localWorkingDir+"&"
        self.__submit_file = os.path.join(self.__globalWorkingDir, 'submit_script')
        handle = open(self.__submit_file, 'w')
        handle.write(submit_script)
        handle.close()

        self.__log.info("submit script:\n%s" % submit_script)
        cmd = "qsub " + self.__submit_file
        self.__log.info("submitting HPC job: %s" % cmd)
        status, output = commands.getstatusoutput(cmd)
        self.__log.info("submitting HPC job: (status: %s, output: %s)" %(status, output))
        self.__jobid = None
        if status == 0:
            self.__jobid = output.replace("\n", "")
            return 0, self.__jobid
        return -1, None

    def poll(self):
        # poll the job in HPC. update it
        cmd = "qstat " + self.__jobid
        self.__log.info("polling HPC job: %s" % cmd)
        status, output = commands.getstatusoutput(cmd)
        #self.__log.info("polling HPC job: (status: %s, output: %s)" %(status, output))
        if status == 0:
            self.__failedPollTimes = 0
            state = None
            lines = output.split("\n")
            for line in lines:
                line = line.strip()
                if line.startswith(self.__jobid):
                    state = line.split(" ")[-2]

            if self.__lastState is None or self.__lastState != state or time.time() > self.__lastTime + 60*5:
                self.__log.info("HPC job state is: %s" %(state))
                self.__lastState = state
                self.__lastTime = time.time()

            if state == "C":
                self.__log.info("HPC job complete")
                self.__isFinished = True
                return "Complete"
            if state == "R":
                return "Running"
            if state == "Q":
                return "Queue"
        else:
            self.__log.info("polling HPC job: (status: %s, output: %s)" %(status, output))
            self.__failedPollTimes += 1
            if self.__failedPollTimes > 5:
                self.__isFinished = True
                return "Failed"
            else:
                return 'Unknown'

    def checkHPCJobLog(self):
        logFile = os.path.join(self.__globalWorkingDir, "athena_stdout.txt")
        command = "grep 'HPCJob-Yoda failed' " + logFile
        status, output = commands.getstatusoutput(command)
        if status == 0:
            return -1, "HPCJob-Yoda failed"
        return 0, None

    def getOutputs(self):
        outputs = []
        all_files = os.listdir(self.__globalWorkingDir)
        for file in all_files:
            if file.endswith(".dump"):
                filename = os.path.join(self.__globalWorkingDir, file)
                handle = open(filename)
                for line in handle:
                    line = line.replace("  ", " ")
                    eventRange, status, output = line.split(" ")
                    outputFileName = output.split(",")[0]
                    outputs.append((eventRange, status, outputFileName))
                handle.close()
                os.rename(filename, filename + ".BAK")
        return outputs

    def isFinished(self):
        if self.__jobid is None:
            self.__log.info("HPC job id is None. Finished")
            self.__isFinished = True
        return self.__isFinished

    def finishJob(self):
        if self.__jobid:
            command = "qdel " + self.__jobid
            status, output = commands.getstatusoutput(command)
            self.__log.debug("Run Command: %s " % command)
            self.__log.debug("Status: %s, Output: %s" % (status, output))
