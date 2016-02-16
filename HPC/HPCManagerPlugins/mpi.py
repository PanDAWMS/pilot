
import commands
import os
import subprocess
import time
import traceback

from HPC.Logger import Logger
from HPC.HPCManagerPlugins.plugin import Plugin

class mpi(Plugin):
    def __init__(self, logFileName):
        self.__log= Logger(logFileName)
        self.__failedPollTimes = 0

    def getHPCResources(self, partition, max_nodes=None, min_nodes=2, min_walltime_m=30):
        return None

    def convertNodeList(self, nodelist):
        try:
            if '[' in nodelist:
                numNames = []
                tmp = nodelist
                preName, numList = tmp.split('[')
                numList,postName = numList.split(']')
                for items in numList.split(","):
                    if not '-' in items:
                        numNames.append(preName + items + postName)
                    else:
                        start, end = items.split('-')
                        numLen = len(start)
                        for i in range(int(start), int(end) + 1):
                            num = str(i).zfill(numLen)
                            numNames.append(preName + str(num) + postName)
                return ','.join(numNames)
            else:
                return nodelist
        except:
            self.__log.debug(traceback.format_exc())
            return nodelist

    def submitJob(self, globalWorkingDir, globalYodaDir, localWorkingDir, queue, repo, mppwidth, mppnppn, walltime, nodes, localSetup=None, cpuPerNode=None):

        nodelist = ""
        if os.environ.has_key('SLURM_NODELIST'):
            nodelist = os.environ['SLURM_NODELIST']
        elif os.environ.has_key('PBS_NODELIST'):
            nodelist = os.environ['PBS_NODELIST']
        nodelist = self.convertNodeList(nodelist)

        submit_script = "#!/bin/bash -l" + "\n"
        submit_script += "module load mpi4py openmpi-ccm" + "\n"
        if localSetup:
            submit_script += localSetup + "\n"
        submit_script += "export PYTHONPATH=%s:$PYTHONPATH\n" % globalWorkingDir
        submit_script += "env" + "\n"

        # submit_script += "srun -N " + str(nodes) + " python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+" 1>yoda_stdout.txt 2>yoda_stderr.txt"
        submit_script += "srun -N " + str(nodes) + " python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir
        # submit_script += "mpirun --host "+nodelist+" python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir

        # submit_script += "mpirun -bynode python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+" 1>yoda_stdout.txt 2>yoda_stderr.txt"
        # submit_script += "python " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+" 1>" + globalYodaDir+ "/yoda_stdout.txt 2>" + globalYodaDir+ "/yoda_stderr.txt"
        # submit_script += "python " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+""
        self.__log.debug("ARC submit script: %s" % submit_script)
        # hpcJob = subprocess.Popen(submit_script, stdout=sys.stdout, stderr=sys.stdout, shell=True)
        yoda_stdout = open(os.path.join(globalYodaDir, 'yoda_stdout.txt'), 'a')
        yoda_stderr = open(os.path.join(globalYodaDir, 'yoda_stderr.txt'), 'a')
        hpcJob = subprocess.Popen(submit_script, stdout=yoda_stdout, stderr=yoda_stderr, shell=True)

        while (hpcJob and hpcJob.poll() is None):
            self.__log.debug("Yoda process is running%s")
            time.sleep(30)

        self.__log.debug("Yoda process terminated")
        self.__log.debug("Yoda process return code: %s" % hpcJob.returncode)

        return 0, None

    def poll(self, jobid):
        return None
