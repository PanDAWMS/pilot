# Class definition:
#   RunJobHopper
#   [Add description here]
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules

from RunJobHPC import RunJobHPC                  # Parent RunJob class
import os, sys, commands, time
import traceback
import atexit, signal
import saga

# Pilot modules
import Site, pUtil, Job, Node, RunJobUtilities
from pUtil import tolog, isAnalysisJob, readpar, getExperiment
from FileStateClient import updateFileStates, dumpFileStates
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from datetime import datetime


class RunJobHopper(RunJobHPC):

    # private data members
    __runjob = "RunJobHopper"                   # String defining the sub class
    __instance = None                           # Boolean used by subclasses to become a Singleton
    #__error = PilotErrors()                    # PilotErrors object

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobHPC, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getRunJob(self):
        """ Return a string with the execution class name """

        return self.__runjob

    def getRunJobFileName(self):
        """ Return the filename of the module """

        return super(RunJobHopper, self).getRunJobFileName()

    # def argumentParser(self):  <-- see example in RunJob.py

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return False
    
    def get_backfill(self, partition, max_nodes = None):
    
        #  Function collect information about current available resources and  
        #  return number of nodes with possible maximum value for walltime according Titan policy
        #
        
        cmd = 'showbf -p %s' % partition
        res_tuple = commands.getstatusoutput(cmd)
        showbf_str = ""
        if res_tuple[0] == 0:
            showbf_str = res_tuple[1]
    
        res = {}
        tolog ("Available resources in %s  partition" % partition)
        tolog (showbf_str)
        if showbf_str:
            shobf_out = showbf_str.splitlines()
            tolog ("Fitted resources")
            for l in shobf_out[2:]:
                d = l.split()
                nodes = int(d[2])
                if not d[3] == 'INFINITY':
                    wal_time_arr =  d[3].split(":")
                    if len(wal_time_arr) < 4:
                        wal_time_sec = int(wal_time_arr[0])*(60*60) + int(wal_time_arr[1])*60 + int(wal_time_arr[2]) 
                        if wal_time_sec > 24 * 3600:
                            wal_time_sec = 24 * 3600
                    else:
                        wal_time_sec = 24 * 3600
                        if nodes > 1:
                            nodes = nodes - 1
                else:
                    wal_time_sec = 12 * 3600
                
                # Fitting Hopper policy 
                # https://www.nersc.gov/users/computational-systems/hopper/running-jobs/queues-and-policies/
                nodes = max_nodes if nodes > max_nodes else nodes    
                    
                if nodes < 65 and wal_time_sec > 96 * 3600:
                    wal_time_sec = 96 * 3600
                elif nodes < 683 and wal_time_sec > 48 * 3600:
                    wal_time_sec = 48 * 3600
                elif nodes < 4097 and wal_time_sec > 36 * 3600:
                    wal_time_sec = 36 * 3600
                elif wal_time_sec > 12 * 3600:
                    wal_time_sec = 12 * 3600

                tolog ("Nodes: %s, Walltime (str): %s, Walltime (min) %s" % (nodes, d[3], wal_time_sec/60 ))

                res.update({nodes:wal_time_sec}) 
        else:
            tolog ("No availble resources. Default values will be used.")
        
        return res
    
    def get_hpc_resources(self, partition, max_nodes = None, min_nodes = 1, min_walltime = 30):
    
        #   
        #  Function return number of nodes and walltime for submission  
        #
        nodes = min_nodes
        walltime =  min_walltime       
        
        backfill = self.get_backfill(partition, max_nodes)
        if backfill:
            for n in sorted(backfill.keys(), reverse=True): 
                if min_walltime <= backfill[n] and nodes <= n:
                    nodes = n
                    walltime = backfill[n] / 60 
                    walltime = walltime - 2
                    break
                if walltime <= 0:
                    walltime = min_walltime
                    nodes = min_nodes
        
        return nodes, walltime

    def jobStateChangeNotification(self, src_obj, fire_on, value):
        tolog("Job state changed to '%s'" % value)
        return True


    def executePayload(self, thisExperiment, runCommandList, job, repeat_num = 0):
        """ execute the payload """
        
        t0 = os.times() 
        res_tuple = (0, 'Undefined')
    
        # special setup command. should be placed in queue defenition (or job defenition) ?
        setup_commands = ['source $MODULESHOME/init/bash',
                          'export LD_LIBRARY_PATH=/opt/cray/lib64:$LD_LIBRARY_PATH',
                          'export CRAY_ROOTFS=DSL',
                          'module load python']
        
        # loop over all run commands (only >1 for multi-trfs)
        current_job_number = 0
        getstatusoutput_was_interrupted = False
        number_of_jobs = len(runCommandList)
        for cmd in runCommandList:
            nodes, walltime = self.get_hpc_resources(self.partition_comp, self.max_nodes, self.nodes, self.min_walltime)
            cpu_number = self.cpu_number_per_node * nodes
            tolog("Launch parameters \nWalltime limit         : %s (min)\nRequested nodes (cores): %s (%s)" % (walltime,nodes,cpu_number))
                    
            current_job_number += 1
            try:
                # add the full job command to the job_setup.sh file
                to_script = "\n".join(cmd['environment'])
                to_script = to_script + ("\nexport G4FORCENUMBEROFTHREADS=%s" % self.number_of_threads) # needed for GEANT
                to_script =  to_script + "\n" + "\n".join(setup_commands)
                to_script = "%s\naprun -n %s -d %s %s %s" % (to_script, cpu_number/self.number_of_threads, self.number_of_threads ,cmd["payload"], cmd["parameters"])    
                
                thisExperiment.updateJobSetupScript(job.workdir, to_script=to_script)
                
                # Simple SAGA fork variant
                tolog("******* SAGA call to execute payload *********")
                try:
    
                    js = saga.job.Service("pbs://localhost")
                    jd = saga.job.Description()
                    if self.project_id:
                        jd.project = self.project_id # should be taken from resourse description (pandaqueue)

                    jd.wall_time_limit = walltime 

                    jd.executable      = to_script
                    jd.total_cpu_count = cpu_number 
                    jd.output = job.stdout
                    jd.error = job.stderr
                    jd.queue = self.executed_queue   # should be taken from resourse description (pandaqueue)
                    jd.working_directory = job.workdir
                    
                    fork_job = js.create_job(jd)
                    fork_job.add_callback(saga.STATE, self.jobStateChangeNotification)
                    
                    #tolog("\n(PBS) Command: %s\n"  % to_script)
                    fork_job.run()
                    #tolog("\nCommand was started at %s.\nState is: %s" % ( str(datetime.now()), fork_job.state))
                    tolog("Local Job ID: %s" % fork_job.id)
                    for i in range(self.waittime * 60):
                        time.sleep(1)
                        if fork_job.state != saga.job.PENDING:
                            break
                    if fork_job.state == saga.job.PENDING:
                        repeat_num = repeat_num + 1
                        tolog("Wait time (%s minutes) exceed, job will be rescheduled (%s)" % (self.waittime, repeat_num))
                        fork_job.cancel()
                        fork_job.wait()
                        if repeat_num < 10:
                            return self.executePayload(thisExperiment, runCommandList, job, repeat_num)
                        
                    fork_job.wait()
                    tolog("Job State              : %s" % (fork_job.state))
                    tolog("Exitcode               : %s" % (fork_job.exit_code))
                    tolog("Create time            : %s" % (fork_job.created))
                    tolog("Start time             : %s" % (fork_job.started))
                    tolog("End time               : %s" % (fork_job.finished))
                    tolog("Walltime limit         : %s (min)" % (jd.wall_time_limit))
                    tolog("Allocated nodes (cores): %s (%s)" % (nodes,cpu_number))
                    cons_time = datetime.strptime(fork_job.finished, '%c') - datetime.strptime(fork_job.started, '%c')
                    cons_time_sec =  (cons_time.microseconds + (cons_time.seconds + cons_time.days * 24 * 3600) * 10**6) / 10**6
                    tolog("Execution time         : %s (sec. %s)" % (str(cons_time), cons_time_sec))
                    #job.timeExe = int(fork_job.finished - fork_job.started)
                    
                    res_tuple = (fork_job.exit_code, "Look into: %s" % job.stdout)
                    
                    ####################################################
                except saga.SagaException, ex:
                    # Catch all saga exceptions
                    tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
                    # Trace back the exception. That can be helpful for debugging.
                    tolog(" \n*** Backtrace:\n %s" % ex.traceback)
                    break
                    
                tolog("**********************************************")
                tolog("******* SAGA call finished *******************")
                tolog("**********************************************")  
    
            except Exception, e:
                tolog("!!FAILED!!3000!! Failed to run command %s" % str(e))
                getstatusoutput_was_interrupted = True
                if self.getFailureCode(): 
                    job.result[2] = self.getFailureCode()
                    tolog("!!FAILED!!3000!! Failure code: %d" % (self.getFailureCode()))
                    break
 
            if res_tuple[0] == 0:
                tolog("Job command %d/%d finished" % (current_job_number, number_of_jobs))
            else:
                tolog("Job command %d/%d failed: res = %s" % (current_job_number, number_of_jobs, str(res_tuple)))
                break
    
        t1 = os.times()
        t = map(lambda x, y:x-y, t1, t0) # get the time consumed
        job.cpuConsumptionUnit, job.cpuConsumptionTime, job.cpuConversionFactor = pUtil.setTimeConsumed(t)
        tolog("Job CPU usage: %s %s" % (job.cpuConsumptionTime, job.cpuConsumptionUnit))
        tolog("Job CPU conversion factor: %1.10f" % (job.cpuConversionFactor))
        job.timeExe = int(round(t1[4] - t0[4]))
        
        tolog("Original exit code: %s" % (res_tuple[0]))
        if res_tuple[0] != None:
            tolog("Exit code: %s (returned from OS)" % (res_tuple[0]%255))
            res0, exitAcronym, exitMsg = self.getTrfExitInfo(res_tuple[0], job.workdir)
        else:
            tolog("Exit code: None (returned from OS, Job was canceled)")
            res0 = None
            exitMsg = "Job was canceled by internal call"
        # check the job report for any exit code that should replace the res_tuple[0]
        
        res = (res0, res_tuple[1], exitMsg)
    
        # dump an extract of the payload output
        if number_of_jobs > 1:
            _stdout = job.stdout
            _stderr = job.stderr
            _stdout = _stdout.replace(".txt", "_N.txt")
            _stderr = _stderr.replace(".txt", "_N.txt")
            tolog("NOTE: For %s output, see files %s, %s (N = [1, %d])" % (job.payload, _stdout, _stderr, number_of_jobs))
        else:
            tolog("NOTE: For %s output, see files %s, %s" % (job.payload, job.stdout, job.stderr))
    
        # JEM job-end callback
        try:
            from JEMstub import notifyJobEnd2JEM
            notifyJobEnd2JEM(job, tolog)
        except:
            pass # don't care (fire and forget)
    
        return res, job, getstatusoutput_was_interrupted, current_job_number


if __name__ == "__main__":

    tolog("Starting RunJobHopper")
    # Get error handler
    error = PilotErrors()

    # Get runJob object
    runJob = RunJobHopper()
    
    # Setup HPC specific parameters for Edison
    
    runJob.cpu_number_per_node = 24
    runJob.walltime = 120
    runJob.max_nodes = 10 
    runJob.number_of_threads = 1
    runJob.min_walltime = 10  # minutes
    runJob.waittime = 15      # minutes
    runJob.nodes = 2
    runJob.partition_comp = 'hopper'
    runJob.project_id = ""
    runJob.executed_queue = readpar('localqueue') 
    
    # Define a new parent group
    os.setpgrp()

    # Protect the runJob code with exception handling
    hP_ret = False
    try:
        # always use this filename as the new jobDef module name
        import newJobDef

        jobSite = Site.Site()

        return_tuple = runJob.argumentParser()
        tolog("argumentParser returned: %s" % str(return_tuple))
        jobSite.setSiteInfo(return_tuple)

#            jobSite.setSiteInfo(argParser(sys.argv[1:]))

        # reassign workdir for this job
        jobSite.workdir = jobSite.wntmpdir

        if runJob.getPilotLogFilename() != "":
            pUtil.setPilotlogFilename(runJob.getPilotLogFilename())

        # set node info
        node = Node.Node()
        node.setNodeName(os.uname()[1])
        node.collectWNInfo(jobSite.workdir)

        # redirect stder
        sys.stderr = open("%s/runjob.stderr" % (jobSite.workdir), "w")

        tolog("Current job workdir is: %s" % os.getcwd())
        tolog("Site workdir is: %s" % jobSite.workdir)
        # get the experiment object
        thisExperiment = getExperiment(runJob.getExperiment())

        tolog("RunJob will serve experiment: %s" % (thisExperiment.getExperiment()))

        # set the cache (used e.g. by LSST)
        #if runJob.getCache():
        #    thisExperiment.setCache(runJob.getCache())

        region = readpar('region')
        #JR = JobRecovery()
        try:
            job = Job.Job()
            job.setJobDef(newJobDef.job)
            job.workdir = jobSite.workdir
            job.experiment = runJob.getExperiment()
            # figure out and set payload file names
            job.setPayloadName(thisExperiment.getPayloadName(job))
        except Exception, e:
            pilotErrorDiag = "Failed to process job info: %s" % str(e)
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
            runJob.failJob(0, error.ERR_UNKNOWN, job, pilotErrorDiag=pilotErrorDiag)

        # prepare for the output file data directory
        # (will only created for jobs that end up in a 'holding' state)
        job.datadir = runJob.getParentWorkDir() + "/PandaJob_%s_data" % (job.jobId)

        # register cleanup function
        atexit.register(runJob.cleanup, job)

        # to trigger an exception so that the SIGTERM signal can trigger cleanup function to run
        # because by default signal terminates process without cleanup.
        def sig2exc(sig, frm):
            """ signal handler """

            error = PilotErrors()
            runJob.setGlobalPilotErrorDiag("!!FAILED!!3000!! SIGTERM Signal %s is caught in child pid=%d!\n" % (sig, os.getpid()))
            tolog(runJob.getGlobalPilotErrorDiag())
            if sig == signal.SIGTERM:
                runJob.setGlobalErrorCode(error.ERR_SIGTERM)
            elif sig == signal.SIGQUIT:
                runJob.setGlobalErrorCode(error.ERR_SIGQUIT)
            elif sig == signal.SIGSEGV:
                runJob.setGlobalErrorCode(error.ERR_SIGSEGV)
            elif sig == signal.SIGXCPU:
                runJob.setGlobalErrorCode(error.ERR_SIGXCPU)
            elif sig == signal.SIGBUS:
                runJob.setGlobalErrorCode(error.ERR_SIGBUS)
            elif sig == signal.SIGUSR1:
                runJob.setGlobalErrorCode(error.ERR_SIGUSR1)
            else:
                runJob.setGlobalErrorCode(error.ERR_KILLSIGNAL)
            runJob.setFailureCode(runJob.getGlobalErrorCode)
            # print to stderr
            print >> sys.stderr, runJob.getGlobalPilotErrorDiag()
            raise SystemError(sig)

        signal.signal(signal.SIGTERM, sig2exc)
        signal.signal(signal.SIGQUIT, sig2exc)
        signal.signal(signal.SIGSEGV, sig2exc)
        signal.signal(signal.SIGXCPU, sig2exc)
        signal.signal(signal.SIGBUS, sig2exc)

        # see if it's an analysis job or not
        analysisJob = isAnalysisJob(job.trf.split(",")[0])
        if analysisJob:
            tolog("User analysis job")
        else:
            tolog("Production job")
        tolog("runJob received a job with prodSourceLabel=%s" % (job.prodSourceLabel))

        # setup starts here ................................................................................

        # update the job state file
        job.jobState = "setup"
        #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # send [especially] the process group back to the pilot
        job.setState([job.jobState, 0, 0])
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # prepare the setup and get the run command list
        ec, runCommandList, job, multi_trf = runJob.setup(job, jobSite, thisExperiment)
        if ec != 0:
            tolog("!!WARNING!!2999!! runJob setup failed: %s" % (job.pilotErrorDiag))
            runJob.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)
        tolog("Setup has finished successfully")

        # job has been updated, display it again
        job.displayJob()

        # (setup ends here) ................................................................................

        tolog("Setting stage-in state until all input files have been copied")
        job.setState(["stagein", 0, 0])
        # send the special setup string back to the pilot (needed for the log transfer on xrdcp systems)
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # stage-in .........................................................................................

        # update the job state file
        job.jobState = "stagein"
        #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # update copysetup[in] for production jobs if brokerage has decided that remote I/O should be used
        if job.transferType == 'direct':
            tolog('Brokerage has set transfer type to \"%s\" (remote I/O will be attempted for input files, any special access mode will be ignored)' %\
                  (job.transferType))
            RunJobUtilities.updateCopysetups('', transferType=job.transferType)

        # stage-in all input files (if necessary)
        job, ins, statusPFCTurl, usedFAXandDirectIO = runJob.stageIn(job, jobSite, analysisJob)
        if job.result[2] != 0:
            tolog("Failing job with ec: %d" % (ec))
            runJob.failJob(0, job.result[2], job, ins=ins, pilotErrorDiag=job.pilotErrorDiag)

        # after stageIn, all file transfer modes are known (copy_to_scratch, file_stager, remote_io)
        # consult the FileState file dictionary if cmd3 should be updated (--directIn should not be set if all
        # remote_io modes have been changed to copy_to_scratch as can happen with ByteStream files)
        # and update the run command list if necessary.
        # in addition to the above, if FAX is used as a primary site mover and direct access is enabled, then
        # the run command should not contain the --oldPrefix, --newPrefix, --lfcHost options but use --usePFCTurl
        if job.inFiles != ['']:
            runCommandList = RunJobUtilities.updateRunCommandList(runCommandList, runJob.getParentWorkDir(), job.jobId, statusPFCTurl, analysisJob, usedFAXandDirectIO)

        # (stage-in ends here) .............................................................................

        # change to running state since all input files have been staged
        tolog("Changing to running state since all input files have been staged")
        job.setState(["running", 0, 0])
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # update the job state file
        job.jobState = "running"
        #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # run the job(s) ...................................................................................

        # Set ATLAS_CONDDB if necessary, and other env vars
        RunJobUtilities.setEnvVars(jobSite.sitename)

        # execute the payload
        res, job, getstatusoutput_was_interrupted, current_job_number = runJob.executePayload(thisExperiment, runCommandList, job)

        # if payload leaves the input files, delete them explicitly
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)

        # payload error handling
        ed = ErrorDiagnosis()
        if res[0] == None:
            job.jobState = "cancelled"
            job.setState(["cancelled", 0, 0])
            rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())
        else:
            job = ed.interpretPayload(job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, runJob.getFailureCode())
        
        if job.result[1] != 0 or job.result[2] != 0:
            runJob.failJob(job.result[1], job.result[2], job, pilotErrorDiag=job.pilotErrorDiag)

        # stage-out ........................................................................................

        # update the job state file
        tolog(runJob.getOutputDir()) 
        
        job.jobState = "stageout"
        #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # verify and prepare and the output files for transfer
        ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(job.outFiles, job.logFile, job.workdir)
        if ec:
            # missing output file (only error code from prepareOutFiles)
            runJob.failJob(job.result[1], ec, job, pilotErrorDiag=pilotErrorDiag)
        tolog("outsDict: %s" % str(outsDict))

        # update the current file states
        updateFileStates(outs, runJob.getParentWorkDir(), job.jobId, mode="file_state", state="created")
        dumpFileStates(runJob.getParentWorkDir(), job.jobId)

        # create xml string to pass to dispatcher for atlas jobs
        outputFileInfo = {}
        if outs or (job.logFile and job.logFile != ''):
            # get the datasets for the output files
            dsname, datasetDict = runJob.getDatasets(job)

            # re-create the metadata.xml file, putting guids of ALL output files into it.
            # output files that miss guids from the job itself will get guids in PFCxml function

            # first rename and copy the trf metadata file for non-build jobs
            if not pUtil.isBuildJob(outs):
                runJob.moveTrfMetadata(job.workdir, job.jobId)

            # create the metadata for the output + log files
            ec, job, outputFileInfo = runJob.createFileMetadata(list(outs), job, outsDict, dsname, datasetDict, jobSite.sitename, analysisJob=analysisJob)
            if ec:
                runJob.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)

        # move output files from workdir to local DDM area
        finalUpdateDone = False
        if outs:
            tolog("Setting stage-out state until all output files have been copied")
            job.setState(["stageout", 0, 0])
            rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

            # stage-out output files
            
            ec, job, rf, latereg = runJob.stageOut(job, jobSite, outs, analysisJob, dsname, datasetDict, outputFileInfo)
            # error handling
            if job.result[0] == "finished" or ec == error.ERR_PUTFUNCNOCALL:
                rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=True)
            else:
                rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=True, latereg=latereg)
            if ec == error.ERR_NOSTORAGE:
                # update the current file states for all files since nothing could be transferred
                updateFileStates(outs, runJob.getParentWorkDir(), job.jobId, mode="file_state", state="not_transferred")
                dumpFileStates(runJob.getParentWorkDir(), job.jobId)

            finalUpdateDone = True
            if ec != 0:
                runJob.sysExit(job, rf)
            # (stage-out ends here) .......................................................................
        job.setState(["finished", 0, 0])
        if not finalUpdateDone:
            rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=True)
        runJob.sysExit(job)

    except Exception, errorMsg:

        error = PilotErrors()

        if runJob.getGlobalPilotErrorDiag() != "":
            pilotErrorDiag = "Exception caught in runJobHopper: %s" % (runJob.getGlobalPilotErrorDiag())
        else:
            pilotErrorDiag = "Exception caught in runJobHopper: %s" % str(errorMsg)

        if 'format_exc' in traceback.__all__:
            pilotErrorDiag += ", " + traceback.format_exc()    

        try:
            tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))
        except Exception, e:
            if len(pilotErrorDiag) > 10000:
                pilotErrorDiag = pilotErrorDiag[:10000]
                tolog("!!FAILED!!3001!! Truncated (%s): %s" % (e, pilotErrorDiag))
            else:
                pilotErrorDiag = "Exception caught in runJob: %s" % (e)
                tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))

#        # restore the proxy if necessary
#        if hP_ret:
#            rP_ret = proxyguard.restoreProxy()
#            if not rP_ret:
#                tolog("Warning: Problems with storage can occur since proxy could not be restored")
#            else:
#                hP_ret = False
#                tolog("ProxyGuard has finished successfully")

        tolog("sys.path=%s" % str(sys.path))
        cmd = "pwd;ls -lF %s;ls -lF;ls -lF .." % (runJob.getPilotInitDir())
        tolog("Executing command: %s" % (cmd))
        out = commands.getoutput(cmd)
        tolog("%s" % (out))

        job = Job.Job()
        job.setJobDef(newJobDef.job)
        job.pilotErrorDiag = pilotErrorDiag
        job.result[0] = "failed"
        if runJob.getGlobalErrorCode() != 0:
            job.result[2] = runJob.getGlobalErrorCode()
        else:
            job.result[2] = error.ERR_RUNJOBEXC
        tolog("Failing job with error code: %d" % (job.result[2]))
        # fail the job without calling sysExit/cleanup (will be called anyway)
        runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag, docleanup=False)
    
    
    
    
    
    
    
    
    
    
