# Class definition:
#   RunJobMira
#   [Add description here]
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from RunJobHPC import RunJobHPC                  # Parent RunJobHPC class
import Site, pUtil, Job, Node, RunJobUtilities
from pUtil import tolog, isAnalysisJob, readpar, getExperiment
from FileStateClient import updateFileStates, dumpFileStates
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from datetime import datetime
from MessageInterface import MessageInterface
from ArgoJob import ArgoJob, ArgoJobStatus
from BalsamJob import BalsamJob 
from SiteInformation import SiteInformation

# Standard python modules
import os, sys, commands, time, optparse, shlex, stat 
import traceback
import atexit, signal



class RunJobArgo(RunJobHPC):

    # private data members
    __runjob = "RunJobArgo"                      # String defining the sub class
    __instance = None                            # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                    # PilotErrors object
    # public data members
    process = ""# zjet, wjet, wqq, wcjet, etc.
    base_filename = "alpout" # should be the same as in the input cards
    # controls for warmup
    warmup_phase0_number_events     = None
    warmup_phase0_number_iterations = None
    warmup_phase1_number_events     = None
    warmup_wall_minutes             = None
    warmup_preprocess               = 'alpgen_warmup_presubmit.sh'
    warmup_preprocess_args          = None
    # controls for event generation (weighted gen + unweighting)
    evtgen_phase0_number_events     = None
    evtgen_phase0_number_iterations = None
    evtgen_phase1_number_events     = None
    evtgen_nodes                    = None
    evtgen_processes_per_node       = None
    evtgen_wall_minutes             = None
    evtgen_executable               = 'alpgenCombo.sh'
    evtgen_scheduler_args           = '--mode=script'
    evtgen_preprocess               = 'alpgen_presubmit.sh'
    evtgen_postprocess              = 'alpgen_postsubmit.sh'
    working_path                    = None
    input_url                       = None
    output_url                      = None
    pdf_filename                    = 'cteq6l1.tbl'
    username                        = None
    serial_site                     = 'argo_cluster'
    parallel_site                   = None
    group_identifier                = None
    athena_input_card_executable    = 'get_alpgen_input_card.py'
    athena_postprocess              = 'alpgen_create_input_cards.py'
    athena_postprocess_log          = 'alpgen_create_input_cards.log'
    ecm                             = None
    run_number                      = None
    job_config                      = None
    evgen_job_opts                  = None
    athena_input_card_name          = 'input_card.mode_1.dat' # card output by Generate_trf
    grid_ftp_server                 = 'atlasgridftp02.hep.anl.gov'
    grid_ftp_protocol               = 'gsiftp://'
    job_working_path                = '/grid/atlas/hpc/argo/jobs'
    argo_job                        = []
    
    # Required methods

    def __init__(self):
        """ Default initialization """
        pass 
    
    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobHPC, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getRunJob(self):
        """ Return a string with the experiment name """

        return self.__runjob

    def getRunJobFileName(self):
        """ Return the filename of the module """

        return super(RunJobArgo, self).getRunJobFileName()

    # def argumentParser(self):  <-- see example in RunJob.py

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return False

    def get_argo_job(self, job):

        ##-----------------------
        # create argo job
        ##-----------------------
        argo_job = ArgoJob()
        argo_job.input_url = None #self.GRID_FTP_PROTOCOL + self.GRID_FTP_SERVER + self.job_path
        if self.input_url is not None:
            argo_job.input_url = self.input_url
        
        argo_job.output_url = self.grid_ftp_protocol + self.grid_ftp_server + self.job_path
        if self.output_url is not None:
            argo_job.output_url = self.output_url
        
        argo_job.username           = self.username
        argo_job.group_identifier   = self.group_identifier
        
        ##-----------------------
        # create get alpgen input cards balsam job
        ##-----------------------
        
        input_file_imode0 = self.base_filename + '.input.0'
        input_file_imode1 = self.base_filename + '.input.1'
        input_file_imode2 = self.base_filename + '.input.2'
        
        input_cards_job = BalsamJob()
        input_cards_job.executable          = self.athena_input_card_executable
        input_cards_job.executable_args     = ('-e ' + self.ecm 
                                            + ' -r ' + self.run_number 
                                            + ' -o ' + self.job_config 
                                            + ' -j ' + self.evgen_job_opts)
        input_cards_job.output_files        = [input_file_imode0,
                                               input_file_imode1,
                                               input_file_imode2,
                                               self.athena_postprocess_log]
        input_cards_job.nodes               = 1
        input_cards_job.processes_per_node  = 1
        input_cards_job.wall_minutes        = 0 # running on condor cluster so does not need time
        input_cards_job.username            = self.username
        input_cards_job.target_site         = self.serial_site
        input_cards_job.postprocess         = self.athena_postprocess
        input_cards_job.postprocess_args    = (' -i ' + self.athena_input_card_name + ' -p ' + self.process 
                                               + ' -n ' + str(self.evtgen_phase1_number_events)
                                               + ' --log-filename=' + str(self.athena_postprocess_log))
        if self.warmup_phase0_number_events is not None:
            input_cards_job.postprocess_args += ' --wmp-evts-itr=' + str(self.warmup_phase0_number_events)
        if self.warmup_phase0_number_iterations is not None:
            input_cards_job.postprocess_args += ' --wmp-nitr=' + str(self.warmup_phase0_number_iterations)
        if self.warmup_phase1_number_events is not None:
            input_cards_job.postprocess_args += ' --wmp-evts=' + str(self.warmup_phase1_number_events)
        
        
        argo_job.add_job(input_cards_job)
        
        ##-----------------------
        # create warm-up job
        ##-----------------------
        
        # create grid filenames
        grid1 = self.base_filename + '.grid1'
        grid2 = self.base_filename + '.grid2'
        
        # create warmup balsam job
        warmup = BalsamJob()
        warmup.executable          = self.process + 'gen90_mpi'
        warmup.executable_args     = input_file_imode0
        warmup.input_files         = [input_file_imode0]
        warmup.output_files        = [grid1,grid2]
        warmup.nodes               = 1
        warmup.processes_per_node  = 1
        warmup.wall_minutes        = 0 # running on condor cluster so does not need time
        warmup.username            = self.username
        warmup.target_site         = self.serial_site
        warmup.preprocess          = self.warmup_preprocess
        
        argo_job.add_job(warmup)
        
        ##-----------------------
        # create event generation job
        ##-----------------------
        
        # create executable
        alpgen_exe = self.process + 'gen90_mpi_ramdisk_nomrstpdfs'
        if 'argo_cluster' in self.parallel_site: # no ramdisk needed on argo_cluster
            alpgen_exe = self.process + 'gen90_mpi'
        
        # create filenames
        unw      = self.base_filename + '.unw.gz'
        unw_par  = self.base_filename + '_unw.par'
        wgt      = self.base_filename + '.wgt'
        wgt_par  = self.base_filename + '.par'
        directoryList_before = 'directoryList_before.txt'
        directoryList_after  = 'directoryList_after.txt'
        
        # create event gen balsam job
        evtgen = BalsamJob()
        evtgen.executable          = self.evtgen_executable
        evtgen.executable_args     = (alpgen_exe + ' ' + input_file_imode1 + ' ' 
                                      + input_file_imode2 + ' ' + str(self.evtgen_processes_per_node))
        evtgen.input_files         = [grid1,
                                      grid2,
                                      input_file_imode1,
                                      input_file_imode2]
        evtgen.output_files        = [unw,
                                      unw_par,
                                      directoryList_before,
                                      directoryList_after,
                                      self.evtgen_postprocess + '.out',
                                      self.evtgen_postprocess + '.err',
                                     ]
        evtgen.preprocess          = self.evtgen_preprocess
        evtgen.postprocess         = self.evtgen_postprocess
        evtgen.postprocess_args    = self.base_filename
        evtgen.nodes               = self.evtgen_nodes
        evtgen.processes_per_node  = self.evtgen_processes_per_node
        evtgen.wall_minutes        = self.evtgen_wall_minutes
        evtgen.username            = self.username
        evtgen.scheduler_args      = self.evtgen_scheduler_args
        evtgen.target_site         = self.parallel_site
        
        argo_job.add_job(evtgen)
        
        return argo_job

    def setup(self, job, jobSite, thisExperiment):
        """ prepare the setup and get the run command list """

        # start setup time counter
        t0 = time.time()
        ec = 0

        # split up the job parameters to be able to loop over the tasks
        jobParameters = job.jobPars.split("\n")[0]
        jobTrf = job.trf.split("\n")[0]
        
        parser = optparse.OptionParser(description=' program to submit alpgen jobs like a pilot')
        parser.add_option('-p','--process',dest='process',help='Alpgen Process, i.e. zjet, wjet, wqq, etc.')
        parser.add_option('-n','--nevts',dest='nevts',help='Number of weighted events requested in input file for weighted event generation',type='int')
        parser.add_option('-g','--group-id',dest='group_identifier',help='User specified string that helps the user group jobs together.')
        parser.add_option('-e','--ecm',dest='ecm',help='Center of Mass Energy.')
        parser.add_option('-r','--run-number',dest='run_number',help='Run Number')
        parser.add_option('-c','--jobConfig',dest='jobConfig',help='Job Options that will used from the Job Config tarball, i.e. MC12JobOptions/MC12.<Run Number>.<description>.py')
        parser.add_option('-j','--evgenJobOpts',dest='evgenJobOpts',help='Job Config tarball, i.e. MC12JobOpts-XX-YY-ZZ.tar.gz')
        parser.add_option('','--dev',dest='dev',help='For development only.',action='store_true',default=False)
        parser.add_option('-q','--status-queue',dest='enable_status_queue',help='Enable the setting of the message queue parameter in the ArgoJob, which means ARGO will not send message updates for this job to the queue with its job ID.',action='store_true',default=False)
        #parser.add_option('-a','--warmup-evts',dest='warmup_evts',help='For Warmup Step: Three numbers seperated by commas giving the number of events per iteration, number of iterations, and final number of events to generate. Example: "10000,10,1000000"')
        parser.add_option('-b','--evtgen-evts',dest='evtgen_evts',help='For Event Generation Step: The number of events to generation in the event generation step. The ouput of unweighted events tends to be less so request more than you want. For example W+0jets gives you 70\%, W+1jet gives you 16%, W+2jet gives you 5%, W+3jet gives you 1%, and so on.', type='int')
        parser.add_option('-o','--num-nodes',dest='numnodes',help='number of nodes to use on destination machine',type='int')
        parser.add_option('-u','--ranks-per-node',dest='ranks_per_node',help='number of MPI ranks per node to use on destination machine',type='int')
        parser.add_option('-t','--wall-time',dest='walltime',help='The wall time to submit to the queue in minutes.',type='int')
        parser.add_option('-s','--site',dest='site',help='Balsam site name on which to run the event generation')
        parser.add_option('-x','--no-submit',dest='submit',help='do not submit the message to ARGO. For testing purposes.',action='store_false',default=True)
        parser.add_option('','--wmp-evts-itr',dest='wm_evts_per_itr',help='Warmup: Number of weighted events per interation.')
        parser.add_option('','--wmp-nitr',dest='wm_nitr',help='Warmup: Number of iterations')
        parser.add_option('','--wmp-evts',dest='wm_evts',help='Warmup: Number of final events to produce.')
        
        try: 
            options, args = parser.parse_args(shlex.split(jobParameters))
        except:
            ec = self.__error.ERR_SETUPFAILURE
            job.pilotErrorDiag = "Failure to parse job arguments"
            tolog("Failure to parse job arguments for ARGO job")
            return ec, job
            
        tolog("ARGO job will be launched with next parameters: %s" % jobParameters)
        
        self.process                          = options.process
        self.username                         = 'pilot, %s' % job.prodUserID[:120]  #os.environ['USER']
        self.group_identifier                 = options.group_identifier
        self.ecm                              = options.ecm
        self.run_number                       = options.run_number
        self.job_config                       = options.jobConfig
        self.evgen_job_opts                   = options.evgenJobOpts
        self.warmup_phase0_number_events      = options.wm_evts_per_itr
        self.warmup_phase0_number_iterations  = options.wm_nitr
        self.warmup_phase1_number_events      = options.wm_evts
        self.evtgen_phase1_number_events      = options.evtgen_evts
        self.evtgen_nodes                     = options.numnodes
        self.evtgen_processes_per_node        = options.ranks_per_node
        self.evtgen_wall_minutes              = options.walltime
        self.parallel_site                    = options.site
        
        self.dev                              = options.dev
        
        self.job_path = os.path.join(self.job_working_path,job.jobId)
        
        tolog("ARGO job path: %s" % self.job_path)
        
        self.argo_job = self.get_argo_job(job)
        
        if options.dev:
            job.serial_site = 'argo_cluster_dev'
        
        # verify that the multi-trf job is setup properly

        os.chdir(jobSite.workdir)
        tolog("Current job workdir is %s" % os.getcwd())

        job.timeSetup = int(time.time() - t0)
        tolog("Total setup time: %d s" % (job.timeSetup))

        return ec, job        

    def executePayload(self, thisExperiment, job):
        
        t0 = os.times() 
        res_tuple = None
        
        # loop over all run commands (only >1 for multi-trfs)
        getstatusoutput_was_interrupted = False
        job_status = None
        tolog("About to launch ARGO job")
        # Poll MQ for Job Status
        try:
            # Initiate MQ interface and send job
            self.argo_job.job_status_routing_key = '%s_job_status' % job.jobId #'status_' + jobID
            si = SiteInformation()
            mi = MessageInterface()
            mi.host = 'atlasgridftp02.hep.anl.gov'
            mi.port = 5671
            mi.ssl_cert = si.getSSLCertificate() #'/grid/atlas/hpc/pilot_certs/xrootdsrv-cert.pem'
            proxy_cert_path = si.getSSLCertificate()
            mi.ssl_cert = os.path.dirname(proxy_cert_path) + "/rabbitmq-cert.pem"
            if 'X509_USER_CERT' in os.environ.keys():
                mi.ssl_cert = os.environ['X509_USER_CERT'] #'/users/hpcusers/balsam_dev/gridsecurity/jchilders/xrootdsrv-cert.pem'
            
            mi.ssl_key  = mi.ssl_cert #'/grid/atlas/hpc/pilot_certs/xrootdsrv-key.pem'
            mi.ssl_key = os.path.dirname(proxy_cert_path) + "/rabbitmq-key.pem"
            if 'X509_USER_KEY' in os.environ.keys():
                mi.ssl_key  = os.environ['X509_USER_KEY'] #'/users/hpcusers/balsam_dev/gridsecurity/jchilders/xrootdsrv-key.pem'
            
            #mi.ssl_ca_certs = os.path.dirname(proxy_cert_path) + "/rabbitmq-cacerts.pem"
            mi.ssl_ca_certs = '/grid/atlas/hpc/pilot_certs/cacerts.pem' 
            #if 'X509_CA_CERTS' in os.environ.keys():
            #    mi.ssl_ca_certs = os.environ['X509_CA_CERTS'] #'/users/hpcusers/balsam_dev/gridsecurity/jchilders/cacerts.pem'
            #tolog("CA certs: %s" % (mi.ssl_ca_certs))
            ca_certs = os.path.dirname(proxy_cert_path) + "/rabbitmq-cacerts.pem"
            if os.path.isfile(ca_certs): 
                mi.ssl_ca_certs = ca_certs
 
            mi.exchange_name = 'argo_users'

            #Create queue to get messages about ARGO Job status from MQ
            tolog('Opening connection with MQ')
            mi.open_blocking_connection()
            tolog('Create queue [%s]  to retrieve messages with job status' % self.argo_job.job_status_routing_key)

            mi.create_queue(self.argo_job.job_status_routing_key, self.argo_job.job_status_routing_key)

            # submit ARGO job to MQ
            
            #tolog('Opening connection with MQ')
            #mi.open_blocking_connection()
            routing_key = 'argo_job'
            if self.dev:
                routing_key = 'argo_job_dev'
            tolog('Sending msg with job to ARGO')
            mi.send_msg(self.argo_job.serialize(), routing_key)
            tolog(' done sending ')
            
            # Waiting till job done or failed    
            ARGO_err_msg = ''
            while True:
                time.sleep(5)
                message = mi.receive_msg(self.argo_job.job_status_routing_key, True)
                if message[2]:
                    tolog ("Got message from queue [%s]: method [%s], properties [%s], body [ %s ]" % (self.argo_job.job_status_routing_key, message[0], message[1], message[2]))
                    job_status = ArgoJobStatus.get_from_message(message[2])
                    job.hpcStatus = job_status.state
                    rt = RunJobUtilities.updatePilotServer(job, self.getPilotServer(), self.getPilotPort())

                    tolog("Extracted state: %s" % job_status.state)
                    if job_status.state == job_status.HISTORY:
                        res_tuple = (0, "Done")
                        break
                    elif job_status.is_failed():
                        res_tuple = (1, "Failed")
                        ARGO_err_msg = ARGO_err_msg + ' ' + job_status.message
                    elif job_status.state == job_status.FAILED:
                        res_tuple = (1, "Failed")
                        ARGO_err_msg = ARGO_err_msg + ' ' + job_status.message
                        runJob.failJob(1, 0, job, ins=job.inFiles, pilotErrorDiag=ARGO_err_msg)
                        break
                time.sleep(5)
                  
            mi.close()
            tolog(' closing connection to MQ')
                
            tolog("Job State: %s" % (job_status.state))
            #job.timeExe = int(fork_job.finished - fork_job.started)
                
            ####################################################
    
        except Exception, e:
            tolog("!!FAILED!!3000!! Failed to run command %s" % str(e))
            getstatusoutput_was_interrupted = True
            res_tuple = (1, "Failed")
            self.failJob(0, self.__error.ERR_GENERALERROR, job, pilotErrorDiag=str(e))
        else:   
            if res_tuple[0] == 0:
                tolog("ARGO Job finished")
            else:
                tolog("ARGO Job failed: res = %s" % (str(res_tuple)))
    
        t1 = os.times()
        # CPU consumption metrics 
        # t = map(lambda x, y:x-y, t1, t0) # get the time consumed
        # job.cpuConsumptionUnit, job.cpuConsumptionTime, job.cpuConversionFactor = pUtil.setTimeConsumed(t)
        # tolog("Job CPU usage: %s %s" % (job.cpuConsumptionTime, job.cpuConsumptionUnit))
        # tolog("Job CPU conversion factor: %1.10f" % (job.cpuConversionFactor))
        job.timeExe = int(round(t1[4] - t0[4]))
        
        tolog("Original exit code: %s" % (res_tuple[0]))
        if res_tuple[0] != None:
            tolog("Exit code: %s (returned from OS)" % (res_tuple[0]%255))
            res0 = res_tuple[0]%255
            if job_status:
                exitMsg = job_status.message
            else:
                exitMsg = res_tuple[1]
        else:
            tolog("Exit code: None (returned from OS, Job was canceled or interrupted)")
            res0 = None
            exitMsg = "Job was canceled by internal call"
        # check the job report for any exit code that should replace the res_tuple[0]
        
        res = (res0, res_tuple[1], exitMsg)
    
        # dump an extract of the payload output
        tolog("NOTE: For %s output, see files %s, %s" % (job.payload, job.stdout, job.stderr))
    
        # JEM job-end callback
        try:
            from JEMstub import notifyJobEnd2JEM
            notifyJobEnd2JEM(job, tolog)
        except:
            pass # don't care (fire and forget)
        
        return res, job, getstatusoutput_was_interrupted

if __name__ == "__main__":

    tolog("Starting RunJobArgo")
    # Get error handler
    error = PilotErrors()

    # Get runJob object
    runJob = RunJobArgo()

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
        tolog("runJobArgo received a job with prodSourceLabel=%s" % (job.prodSourceLabel))

        # setup starts here ................................................................................

        # update the job state file
        job.jobState = "setup"
        #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # send [especially] the process group back to the pilot
        job.setState([job.jobState, 0, 0])
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # prepare the setup and get the run command list
        ec, job = runJob.setup(job, jobSite, thisExperiment)
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
        #if job.inFiles != ['']:
        #    runCommandList = RunJobUtilities.updateRunCommandList(runCommandList, runJob.getParentWorkDir(), job.jobId, statusPFCTurl, analysisJob, usedFAXandDirectIO)

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
        res, job, getstatusoutput_was_interrupted = runJob.executePayload(thisExperiment, job)

        tolog("Check ARGO output: %s" % runJob.job_path)
        # if payload leaves the input files, delete them explicitly
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)

        # payload error handling
        ed = ErrorDiagnosis()
        if res[0] == None:
            job.jobState = "cancelled"
            job.setState(["cancelled", 0, 0])
            rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())
        #else:
        #    job = ed.interpretPayload(job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, runJob.getFailureCode())
        
        if job.result[1] != 0 or job.result[2] != 0:
            runJob.failJob(job.result[1], job.result[2], job, pilotErrorDiag=job.pilotErrorDiag)

        # stage-out ........................................................................................

        # update the job state file
        tolog(runJob.getOutputDir()) 
        
        job.jobState = "stageout"
        #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # verify and prepare and the output files for transfer
        ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(job.outFiles, job.logFile, runJob.job_path)
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
            pilotErrorDiag = "Exception caught in runJobArgo: %s" % (runJob.getGlobalPilotErrorDiag())
        else:
            pilotErrorDiag = "Exception caught in runJobArgo: %s" % str(errorMsg)

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
    
    
