
import commands
import os

from HPC.Logger import Logger
from HPC.HPCManagerPlugins.plugin import Plugin

class slurm(Plugin):
    def __init__(self, logFileName):
        self.__log= Logger(logFileName)
        self.__failedPollTimes = 0

    def getHPCResources(self, partition, max_nodes=None, min_nodes=2, min_walltime_m=30):
        # copied from RunJobEdison
        #cmd = 'showbf -p %s' % partition
        cmd = 'sinfo '
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

    def submitJob(self, globalWorkingDir, globalYodaDir, localWorkingDir, queue, repo, mppwidth, mppnppn, walltime, nodes, localSetup=None, cpuPerNode=None):
        submit_script = "#!/bin/bash -l" + "\n"
        submit_script += "#SBATCH -p " + queue + "\n"
        if repo:
            submit_script += "#SBATCH -A " + repo + "\n"
        # submit_script += "#SBATCH -n " + str(mppwidth) + "\n"
        submit_script += "#SBATCH -N " + str(nodes) + "\n"
        submit_script += "#SBATCH -t " + walltime + "\n"
        submit_script += "#SBATCH --ntasks-per-node=1\n"
        submit_script += "#SBATCH --cpus-per-task=" + str(cpuPerNode) + "\n"
        submit_script += "#SBATCH -J ES_job" + "\n"
        submit_script += "#SBATCH -o athena_stdout.txt" + "\n"
        submit_script += "#SBATCH -e athena_stderr.txt" + "\n"
        submit_script += "cd $SBATCH_O_WORKDIR" + "\n"
        submit_script += "module load mpi4py" + "\n"
        if localSetup:
            submit_script += localSetup + "\n"
        #submit_script += "source /project/projectdirs/atlas/sw/python-yampl/setup.sh" + "\n"
        #submit_script += "export PYTHONPATH=/project/projectdirs/atlas/sw/python-yampl/python-yampl/1.0/lib.linux-x86_64-2.6:$PYTHONPATH" + "\n"
        submit_script += "export PYTHONPATH=%s:$PYTHONPATH\n" % globalWorkingDir
        #submit_script += "export PYTHONPATH=/project/projectdirs/atlas/pilot/grid_env/boto/lib/python2.6/site-packages:$PYTHONPATH\n"
        #submit_script += "export PYTHONPATH=/project/projectdirs/atlas/pilot/grid_env/external:$PYTHONPATH\n"
        #submit_script += "export LD_LIBRARY_PATH=/project/projectdirs/atlas/sw/python-yampl/yampl/1.0/lib:$LD_LIBRARY_PATH" + "\n"
        #submit_script += "export X509_USER_PROXY=/global/homes/w/wguan/x509up_u23959" + "\n"
        #submit_script += "export X509_CERT_DIR=/project/projectdirs/atlas/pilot/grid_env/external/grid-security/certificates" + "\n"
        submit_script += "env" + "\n"
        # submit_script += "module avail" + "\n"
        # submit_script += "module list" + "\n"

        #submit_script += "srun -n " + str(nodes) + " -N " + str(mppnppn) + " python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+""
        submit_script += "srun -N " + str(nodes) + " python-mpi " + os.path.join(globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+globalYodaDir+" --localWorkingDir="+localWorkingDir+""
        ###cmd = "mpiexec -n 2 python " + os.path.join(self.__globalWorkingDir, "HPC/HPCJob.py") + " --globalWorkingDir="+self.__globalWorkingDir+" --localWorkingDir="+self.__localWorkingDir+"&"
        self.__submit_file = os.path.join(globalYodaDir, 'submit_script')
        handle = open(self.__submit_file, 'w')
        handle.write(submit_script)
        handle.close()

        self.__log.info("submit script:\n%s" % submit_script)
        cmd = "sbatch " + self.__submit_file
        self.__log.info("submitting HPC job: %s" % cmd)
        status, output = commands.getstatusoutput(cmd)
        self.__log.info("submitting HPC job: (status: %s, output: %s)" %(status, output))
        self.__jobid = None
        if status == 0:
            self.__jobid = output.replace("\n", "").split(" ")[-1]
            return 0, self.__jobid
        return -1, None

    def poll(self, jobid):
        # poll the job in HPC. update it
        cmd = "scontrol show job " + jobid
        self.__log.info("polling HPC job: %s" % cmd)
        status, output = commands.getstatusoutput(cmd)
        # self.__log.info("polling HPC job: (status: %s, output: %s)" %(status, output))
        if status == 0:
            self.__failedPollTimes = 0
            state = None
            lines = output.split("\n")
            for line in lines:
                line = line.strip()
                if line.startswith('JobState'):
                    state = line.split(" ")[0].split("=")[1]

            if state == "COMPLETE":
                self.__log.info("HPC job complete")
                return "Complete"
            if state == "RUNNING":
                return "Running"
            if state == "PENDING":
                return "Queue"
            if state == "FAILED":
                return "Failed"
        else:
            self.__log.info("polling HPC job: (status: %s, output: %s)" %(status, output))
            if 'Invalid job id specified' in output:
                self.__log.info("Unknown Job Id. Set Job Complete.")
                return "Complete"
            else:
                self.__failedPollTimes += 1
                if self.__failedPollTimes > 5:
                    return "Failed"
                else:
                    return 'Unknown'

    def delete(self, jobid):
        command = "scancel " + jobid
        status, output = commands.getstatusoutput(command)
        self.__log.debug("Run Command: %s " % command)
        self.__log.debug("Status: %s, Output: %s" % (status, output))
