
import commands
import os
import subprocess
import time
import traceback

from HPC.Logger import Logger
from HPC.HPCManagerPlugins.plugin import Plugin

class poe(Plugin):
    def __init__(self, logFileName):
        self.__log= Logger(logFileName)
        self.__failedPollTimes = 0

    def isLocalProcess(self):
        return True

    def getName(self):
        return 'poe'

    def getHPCResources(self, partition, max_nodes=None, min_nodes=2, min_walltime_m=30):
        return None

    def submitJob(self, globalWorkingDir, globalYodaDir, localWorkingDir, queue, repo, mppwidth, mppnppn, walltime, nodes, localSetup=None, cpuPerNode=None, dumpEventOutputs=False):

        submit_script = "#!/bin/bash -l" + "\n"
        submit_script += "#@ tasks_per_node = 1" + "\n"
        submit_script += "source /etc/profile.d/modules.sh" + "\n"
        submit_script += "module load mpi4py" + "\n"
        if localSetup:
            submit_script += localSetup + "\n"
        submit_script += "source ${VO_ATLAS_SW_DIR}/local/setup-yampl.sh" + "\n"
        submit_script += "export PYTHONPATH=/cvmfs/atlas.cern.ch/repo/sw/local/noarch/python-yampl/1.0/lib.linux-x86_64-2.6:$PYTHONPATH" + "\n"
        submit_script += "export PYTHONPATH=%s:$PYTHONPATH\n" % globalWorkingDir
        #submit_script += "export CMTEXTRATAGS=ATLAS,useDBRelease" + "\n"
        submit_script += "env" + "\n"

        # submit_script += "srun -N " + str(nodes) + " python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+" 1>yoda_stdout.txt 2>yoda_stderr.txt"
        # submit_script += "srun -N " + str(nodes) + " python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir
        # submit_script += "mpirun --host "+nodelist+" python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir

        # submit_script += "python " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+" 1>" + globalYodaDir+ "/yoda_stdout.txt 2>" + globalYodaDir+ "/yoda_stderr.txt"
        submit_script += "poe parrot_run python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+" --outputDir=" + os.path.dirname(globalYodaDir) + " --dumpEventOutputs"
        self.__log.debug("POE submit script: %s" % submit_script)
        # hpcJob = subprocess.Popen(submit_script, stdout=sys.stdout, stderr=sys.stdout, shell=True)
        yoda_stdout = open(os.path.join(globalYodaDir, 'yoda_stdout.txt'), 'a')
        yoda_stderr = open(os.path.join(globalYodaDir, 'yoda_stderr.txt'), 'a')
        hpcJob = subprocess.Popen(submit_script, stdout=yoda_stdout, stderr=yoda_stderr, shell=True)

        t1 = time.time()
        i = 20
        while (hpcJob and hpcJob.poll() is None):
            if i == 0:
                self.__log.debug("Yoda process is running")
                i = 20
            time.sleep(30)
            i -= 1

        self.__log.debug("Yoda process terminated")
        self.__log.debug("Yoda process return code: %s" % hpcJob.returncode)

        return 0, None

    def poll(self, jobid):
        return None
