
import commands
import os

from HPC.Logger import Logger
from HPC.HPCManagerPlugins.plugin import Plugin

class arc(Plugin):
    def __init__(self, logFileName):
        self.__log= Logger(logFileName)
        self.__failedPollTimes = 0

    def getHPCResources(self, partition, max_nodes=None, min_nodes=2, min_walltime_m=30):
        return None

    def submitJob(self, globalWorkingDir, globalYodaDir, localWorkingDir, queue, repo, mppwidth, mppnppn, walltime, nodes, localSetup=None):
        submit_script = "#!/bin/bash -l" + "\n"
        submit_script += "module load mpi4py" + "\n"
        if localSetup:
            submit_script += localSetup + "\n"
        submit_script += "export PYTHONPATH=%s:$PYTHONPATH\n" % globalWorkingDir
        submit_script += "env" + "\n"

        submit_script += "aprun -n " + str(nodes) + " -N " + str(mppnppn) + " -cc none python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+" 1>yoda_stdout.txt 2>yoda_stderr.txt"
        self.__log.debug("ARC submit script: %s" % submit_script)
        ret = os.system(submit_script)
        return 0, None

    def poll(self, jobid):
        return None
