import string
import os
import re
import commands
from pUtil import tolog, readpar
from FileHandling import getMaxWorkDirSize

class Node:
    """ worker node information """

    def __init__(self):
        self.cpu = 0
        self.nodename = None
        self.mem = 0
        self.disk = 0
        self.numberOfCores = self.setNumberOfCores()

        # set the batch job and machine features
        self.collectMachineFeatures()

    def setNodeName(self, nm):
        self.nodename = nm

    def displayNodeInfo(self):
        tolog("CPU: %0.2f, memory: %0.2f, disk space: %0.2f" % (self.cpu, self.mem, self.disk))
        
    def collectWNInfo(self, diskpath):
        """ collect node information (cpu, memory and disk space) """

        fd = open("/proc/meminfo", "r")
        mems = fd.readline()
        while mems:
            if mems.upper().find("MEMTOTAL") != -1:
                self.mem = float(mems.split()[1])/1024
                break
            mems = fd.readline()
        fd.close()

        fd = open("/proc/cpuinfo", "r")
        lines = fd.readlines()
        fd.close()
        for line in lines:
            if not string.find(line, "cpu MHz"):
                self.cpu = float(line.split(":")[1])
                break

        diskpipe = os.popen("df -mP %s" % (diskpath)) # -m = MB
        disks = diskpipe.read()
        if not diskpipe.close():
            self.disk = float(disks.splitlines()[1].split()[3])

        return self.mem, self.cpu, self.disk
    
    def setNumberOfCores(self) :
        """ Report the number of cores in the WN """
        # 1. Grab corecount from queuedata
        # 2. If corecount is number and corecount > 1, set ATHENA_PROC_NUMBER env variable to this value
        # 3. If corecount is 0, null, or doesn't exist, then don't set the env. variable
        # 4. If corecount is '-1', then get number of cores from /proc/cpuinfo, and set the env. variable accordingly.

        cores = []
        nCores = None

        # grab the schedconfig value
        try:
            nCores = int(readpar('corecount'))
        except ValueError: # covers the case 'NULL'
            tolog("corecount not an integer in queuedata")
        except Exception, e:
            tolog("corecount not set in queuedata: %s" % str(e))
        else:
            if nCores > 1:
                tolog("Setting number of cores: ATHENA_PROC_NUMBER=%d (from schedconfig.corecount)" % (nCores))
                os.environ['ATHENA_PROC_NUMBER'] = str(nCores)
                return nCores

        if not nCores or nCores == 0:
            tolog("Will not set ATHENA_PROC_NUMBER")
            return nCores

        if nCores == -1:
            # check locally
            try:
                cpuinfo = open("/proc/cpuinfo")
            except Exception, e:
                tolog("!!WARNING!!2998!! Failed to get the number of cores: %s" % str(e))
            else:
                for line in cpuinfo :
                    if re.match('core id', line):
                        a = re.search('core id\s*:\s*(\d+)', line)
                        cores.append(a.group(1))

                cpuinfo.close()
                nCores = len(cores)
                if nCores == 0:
                    nCores = 1

                tolog("Setting number of cores: ATHENA_PROC_NUMBER=%d (from cpuinfo)" % (nCores))
                os.environ['ATHENA_PROC_NUMBER'] = str(nCores)
        else:
            tolog("Will not set ATHENA_PROC_NUMBER (nCores=%d)" % (nCores))

        return nCores

    def getNumberOfCores(self):
        return self.numberOfCores

    def readValue(self, path):
        """ Read value from file """

        value = ""
        try:
            f = open(path, "r")
        except IOError, e:
            tolog("!!WARNING!!2344!! Failed to open file: %s" % (e))
        else:
            value = f.read()
            f.close()

            # protect against trailing new line
            if value.endswith("\n"):
                value = value[:-1]

        return value

    def setDataMember(self, directory, name):
        """ Set a batch job or machine features data member """

        data_member_value = ""
        path = os.path.join(directory, name)
        if os.path.exists(path):
            data_member_value = self.readValue(path)

        return data_member_value

    def dumpValue(self, name, value):
        """ Print a value if not empty """

        if value != "":
            tolog("%s = %s" % (name, value))
        else:
            tolog("%s was not set by the batch system" % (name))

    def collectMachineFeatures(self):
        """ Collect the machine and job features from /etc/machinefeatures and $JOBFEATURES """

        # treat float and int values as strings
        self.hs06 = ""
        self.shutdownTime = ""
        self.jobSlots = ""
        self.physCores = ""
        self.logCores = ""
        self.cpuFactorLrms = ""
        self.cpuLimitSecsLrms = ""
        self.cpuLimitSecs = ""
        self.wallLimitSecsLrms = ""
        self.wallLimitSecs = "" 
        self.diskLimitGB = ""
        self.jobStartSecs = ""
        self.memLimitMB = ""
        self.allocatedCPU = ""

        tolog("Collecting machine features")

        if os.environ.has_key('MACHINEFEATURES'):
            MACHINEFEATURES = os.environ['MACHINEFEATURES']

            self.hs06 = self.setDataMember(MACHINEFEATURES, "hs06")
            self.shutdownTime = self.setDataMember(MACHINEFEATURES, "shutdowntime")
            self.jobSlots = self.setDataMember(MACHINEFEATURES, "jobslots")
            self.physCores = self.setDataMember(MACHINEFEATURES, "phys_cores")
            self.logCores = self.setDataMember(MACHINEFEATURES, "log_cores")

            tolog("Machine features:")
            self.dumpValue("hs06", self.hs06)
            self.dumpValue("shutdowntime", self.shutdownTime)
            self.dumpValue("jobslots", self.jobSlots)
            self.dumpValue("phys_cores", self.physCores)
            self.dumpValue("log_cores", self.logCores)
        else:
            tolog("$MACHINEFEATURES not defined locally")

        if os.environ.has_key('JOBFEATURES'):
            JOBFEATURES = os.environ['JOBFEATURES']

            self.cpuFactorLrms = self.setDataMember(JOBFEATURES, "cpufactor_lrms")
            self.cpuLimitSecsLrms = self.setDataMember(JOBFEATURES, "cpu_limit_secs_lrms")
            self.cpuLimitSecs = self.setDataMember(JOBFEATURES, "cpu_limit_secs")
            self.wallLimitSecsLrms = self.setDataMember(JOBFEATURES, "wall_limit_secs_lrms")
            self.wallLimitSecs = self.setDataMember(JOBFEATURES, "wall_limit_secs")
            self.diskLimitGB = self.setDataMember(JOBFEATURES, "disk_limit_GB")
            self.jobStartSecs = self.setDataMember(JOBFEATURES, "jobstart_secs")
            self.memLimitMB = self.setDataMember(JOBFEATURES, "mem_limit_MB")
            self.allocatedCPU = self.setDataMember(JOBFEATURES, "allocated_CPU")

            tolog("Batch system job features:")
            self.dumpValue("cpufactor_lrms", self.cpuFactorLrms)
            self.dumpValue("cpu_limit_secs_lrms", self.cpuLimitSecsLrms)
            self.dumpValue("cpu_limit_secs", self.cpuLimitSecs)
            self.dumpValue("wall_limit_secs_lrms", self.wallLimitSecsLrms)
            self.dumpValue("wall_limit_secs", self.wallLimitSecs)
            self.dumpValue("disk_limit_GB", self.diskLimitGB)
            self.dumpValue("jobstart_secs", self.jobStartSecs)
            self.dumpValue("mem_limit_MB", self.memLimitMB)
            self.dumpValue("allocated_CPU", self.allocatedCPU)
        else:
            tolog("$JOBFEATURES not defined locally")

        cmd = "hostname -i"
        tolog("Executing command: %s" % (cmd))
        out = commands.getoutput(cmd)
        tolog("IP number of worker node: %s" % (out))

    def addFieldToJobMetrics(self, name, value):
        """ Add a substring field to the job metrics string """

        jobMetrics = ""
        if value != "":
            jobMetrics += "%s=%s " % (name, value)

        return jobMetrics

    def addToJobMetrics(self, jobResult, path, jobId):
        """ Add the batch job and machine features to the job metrics """

        jobMetrics = ""

#        jobMetrics += self.addFieldToJobMetrics("hs06", self.hs06)
#        jobMetrics += self.addFieldToJobMetrics("shutdowntime", self.shutdownTime)
#        jobMetrics += self.addFieldToJobMetrics("jobslots", self.jobSlots)
#        jobMetrics += self.addFieldToJobMetrics("phys_cores", self.physCores)
#        jobMetrics += self.addFieldToJobMetrics("log_cores", self.logCores)
#        jobMetrics += self.addFieldToJobMetrics("cpufactor_lrms", self.cpuFactorLrms)
#        jobMetrics += self.addFieldToJobMetrics("cpu_limit_secs_lrms", self.cpuLimitSecsLrms)
#        jobMetrics += self.addFieldToJobMetrics("cpu_limit_secs", self.cpuLimitSecs)
#        jobMetrics += self.addFieldToJobMetrics("wall_limit_secs_lrms", self.wallLimitSecsLrms)
#        jobMetrics += self.addFieldToJobMetrics("wall_limit_secs", self.wallLimitSecs)
#        jobMetrics += self.addFieldToJobMetrics("disk_limit_GB", self.diskLimitGB)
#        jobMetrics += self.addFieldToJobMetrics("jobstart_secs", self.jobStartSecs)
#        jobMetrics += self.addFieldToJobMetrics("mem_limit_MB", self.memLimitMB)
#        jobMetrics += self.addFieldToJobMetrics("allocated_CPU", self.allocatedCPU)

        # Get the max disk space used by the payload (at the end of a job)
        if jobResult == "finished" or jobResult == "failed" or jobResult == "holding":
            max_space = getMaxWorkDirSize(path, jobId)
            if max_space > 0L:
                jobMetrics += self.addFieldToJobMetrics("workDirSize", max_space)
            else:
                tolog("Will not add max space = %d to job metrics" % (max_space))

        return jobMetrics

    def getBenchmarkDictionary(self, si):
        """ Execute the benchmack test if required by the site information object """

        benchmark_dictionary = None
        if si.shouldExecuteBenchmark():
            benchmark_dictionary = si.executeBenchmark()
        else:
            tolog("Not required to run the benchmark test")

        return benchmark_dictionary
