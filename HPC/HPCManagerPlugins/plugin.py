
from HPC.Logger import Logger

class Plugin:
    def __init__(self, logFileName):
        self.__log= Logger(logFileName)

    def isLocalProcess(self):
        return False

    def getName(self):
        return 'plugin'

    def getHPCResources(self, partition, max_nodes=None, min_nodes=2, min_walltime_m=30):
        pass

    def submitJob(self):
        pass

    def poll(self, jobid):
        pass

    def delete(self, jobid):
        pass
