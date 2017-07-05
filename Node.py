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
        self.benchmarks = None

        # set the batch job and machine features
        self.collectMachineFeatures()

    def setNodeName(self, nm):
        self.nodename = nm

    def displayNodeInfo(self):
        tolog("CPU: %0.2f, memory: %0.2f, disk space: %0.2f" % (self.cpu, self.mem, self.disk))
    
    def isAVirtualMachine(self):
        """ Are we running in a virtual machine? """

        status = False

        # if we are running inside a VM, then linux will put 'hypervisor' in cpuinfo
        with open("/proc/cpuinfo", "r") as fd:
            lines = fd.readlines()
            for line in lines:
                if "hypervisor" in line:
                    status = True
                    break

        return status

    def collectCoreInfo(self):
        """ Collect information about cores and threads from lscpu """

        # Thread(s) per core:    1
        # Core(s) per socket:    1
        # Socket(s):             8

        threads_per_code = 0
        cores_per_socket = 0
        sockets = 0

        threads_pattern = re.compile(r"Thread\(s\)\ per\ core\:.+(\d+)+")
        cores_pattern = re.compile(r"Core\(s\)\ per\ socket\:.+(\d+)+")
        sockets_pattern = re.compile(r"Socket\(s\)\:.+(\d+)+")

        lscpu = os.popen('lscpu')
        if lscpu:
            lines = lscpu.readlines()
            lscpu.close()

            if lines:
                for line in lines:
                    threads_found = re.findall(threads_pattern, line)
                    if len(threads_found) > 0:
                        tolog("Found %d thread(s) per core" % (threads_found[0]))
                    cores_found = re.findall(cores_pattern, line)
                    if len(cores_found) > 0:
                        tolog("Found %d core(s) per socket" % (cores_found[0]))
                    sockets_found = re.findall(sockets_pattern, line)
                    if len(sockets_found) > 0:
                        tolog("Found %d socket(s)" % (sockets_found[0]))
            else:
                tolog("No info from lscpu")
        else:
            tolog("Failed to open lscpu (cannot report core info with benchmarks)")

        return threads_per_code, cores_per_socket, sockets

    def collectWNInfo(self, diskpath):
        """ collect node information (cpu, memory and disk space) """

        with open("/proc/meminfo", "r") as fd:
            mems = fd.readline()
            while mems:
                if mems.upper().find("MEMTOTAL") != -1:
                    self.mem = float(mems.split()[1])/1024
                    break
                mems = fd.readline()

        with open("/proc/cpuinfo", "r") as fd:
            lines = fd.readlines()
            for line in lines:
                if not string.find(line, "cpu MHz"):
                    self.cpu = float(line.split(":")[1])
                    break

        diskpipe = os.popen("df -mP %s" % (diskpath)) # -m = MB
        disks = diskpipe.read()
        if not diskpipe.close():
            self.disk = float(disks.splitlines()[1].split()[3])

        return self.mem, self.cpu, self.disk

    def getNumberOfCores(self):
        """ Report the number of cores """

        return self.numberOfCores

    def setNumberOfCores(self, nCores=0):
        """ Get the number of cores """
        # Note: this is usually as specified in the job description, ie the requested number of codes, not the
        # physical number of cores on the WN
        # Note also that this method will be called when the node object is created - before the job description
        # is downloaded, to get a default initial value. The method should be called once the job has been downloaded.

        # Since pilot version 69.2, the pilot will only set the number of cores if known by ATHENA_PROC_NUMBER
        # If nCores is not specified, try to get it from the environment
        if nCores == 0:
            if 'ATHENA_PROC_NUMBER' in os.environ:
                try:
                    nCores = int(os.environ['ATHENA_PROC_NUMBER'])
                except:
                    nCores = 1
            else:
                nCores = 1

        self.numberOfCores = nCores

    def getNumberOfCoresFromEnvironment(self):
        """ Get the number of cores from the environment """

        nCores = None
        if 'ATHENA_PROC_NUMBER' in os.environ:
            try:
                nCores = int(os.environ['ATHENA_PROC_NUMBER'])
            except:
                nCores = None

        return nCores

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

    def getBenchmarkDictionary(self):
        """ Return the benchmark dictionary """

        return self.benchmarks
