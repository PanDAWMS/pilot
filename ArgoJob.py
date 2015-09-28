import sys
from pUtil import serialize, deserialize, convert_unicode_string, tolog
from BalsamJob import BalsamJob

class ArgoJob:
    
    def __init__(self,
                 preprocess                = None,
                 preprocess_args           = None,
                 postprocess               = None,
                 postprocess_args          = None,
                 input_url                 = None,
                 output_url                = None,
                 username                  = None,
                 email_address             = None,
                 group_identifier          = None,
                 job_status_routing_key    = None,
                ):
        self.preprocess               = preprocess
        self.preprocess_args          = preprocess_args
        self.postprocess              = postprocess
        self.postprocess_args         = postprocess_args
        self.input_url                = input_url
        self.output_url               = output_url
        self.username                 = username
        self.email_address            = email_address
        self.group_identifier         = group_identifier
        self.job_status_routing_key   = job_status_routing_key
        self.jobs               = []

    def add_job(self,job):
        if isinstance(job,BalsamJob):
            self.jobs.append(job)
        else:
            tolog(' Only jobs of the BalsamJob class can be added to this list. ')
            raise Exception(' JobTypeError ')

    def get_jobs_dictionary_list(self):
        # convert job objects into json strings within the list
        tmp_jobs = []
        for job in self.jobs:
            tmp_jobs.append(job.__dict__)
        return tmp_jobs

    def get_job_list_text(self):
        return serialize(self.get_jobs_dictionary_list())
      
    def serialize(self):
        # create temp ArgoJob and fill with this list of json job strings
        tmp_argojob = ArgoJob()
        tmp_argojob.__dict__ = self.__dict__.copy()
        tmp_argojob.jobs = self.get_jobs_dictionary_list()
      
        try:
            return serialize(tmp_argojob.__dict__)
        except:
            tolog(' received exception while converting ArgoJob to json string: ' + str(sys.exc_info()[1]))
            raise

    def deserialize(self,text_string):
        # fill self with json dictionary
        try:
            self.__dict__ = deserialize(text_string)
        except:
            tolog(' received exception while converting json string to ArgoJob: ' + str(sys.exc_info()[1]))
            raise

        # convert unicode strings to strings
        self.preprocess                  = convert_unicode_string(self.preprocess)
        self.preprocess_args             = convert_unicode_string(self.preprocess_args)
        self.postprocess                 = convert_unicode_string(self.postprocess)
        self.postprocess_args            = convert_unicode_string(self.postprocess_args)
        self.input_url                   = convert_unicode_string(self.input_url)
        self.output_url                  = convert_unicode_string(self.output_url)
        self.username                    = convert_unicode_string(self.username)
        self.group_identifier            = convert_unicode_string(self.group_identifier)
        self.job_status_routing_key      = convert_unicode_string(self.job_status_routing_key)

        # need to convert vector of json job strings to objects
        tmp_jobs = []
        for job_dictionary in self.jobs:
            tmp_job = BalsamJob()
            tmp_job.__dict__ = job_dictionary
            tmp_jobs.append(tmp_job)
        # now copy into job list
        self.jobs = tmp_jobs
     
class ArgoJobStatus:
    
    CREATED                 = 'CREATED'
    STAGED_IN               = 'STAGED_IN'
    STAGEIN_FAILED          = 'STAGEIN_FAILED'
    PREPROCESSED            = 'PREPROCESSED'
    PREPROCESSING_FAILED    = 'PREPROCESSING_FAILED'
    
    SUBJOB_PREPARED         = 'SUBJOB_PREPARED'
    SUBJOB_PREP_FAILED      = 'SUBJOB_PREP_FAILED'
    
    SUBJOB_STAGED_OUT       = 'SUBJOB_STAGED_OUT'
    SUBJOB_STAGEOUT_FAILED  = 'SUBJOB_STAGEOUT_FAILED'
    SUBJOB_SUBMITTED        = 'SUBJOB_SUBMITTED'
    SUBJOB_SUBMIT_FAILED    = 'SUBJOB_SUBMIT_FAILED'
    SUBJOB_QUEUED           = 'SUBJOB_QUEUED'
    SUBJOB_RUNNING          = 'SUBJOB_RUNNING'
    SUBJOB_RUN_FINISHED     = 'SUBJOB_RUN_FINISHED'
    SUBJOB_RUN_FAILED       = 'SUBJOB_RUN_FAILED'
    SUBJOB_STAGED_IN        = 'SUBJOB_STAGED_IN'
    SUBJOB_STAGEIN_FAILED   = 'SUBJOB_STAGEIN_FAILED'
    
    SUBJOB_COMPLETED        = 'SUBJOB_COMPLETED'
    SUBJOB_COMPLETE_FAILED  = 'SUBJOB_COMPLETE_FAILED'
    
    POSTPROCESSED           = 'POSTPROCESSED'
    POSTPROCESSING_FAILED   = 'POSTPROCESSING_FAILED'
    STAGED_OUT              = 'STAGED_OUT'
    STAGEOUT_FAILED         = 'STAGEOUT_FAILED'
    HISTORY                 = 'HISTORY' # final state
    FAILED                  = 'FAILED'
    
    def __init__(self):
        self.state   = None
        self.job_id  = None
        self.message = None
    
    def get_serialized_message(self):
        return serialize(self.__dict__)
    
    @staticmethod
    def get_from_message(message):
        tmp = ArgoJobStatus()
        tmp.__dict__ = deserialize(message)
        return tmp
    
    def is_failed(self):
        failed_states = [self.STAGEIN_FAILED,
                         self.PREPROCESSING_FAILED,
                         self.SUBJOB_PREP_FAILED,
                         self.SUBJOB_STAGEOUT_FAILED,
                         self.SUBJOB_SUBMIT_FAILED,
                         self.SUBJOB_RUN_FAILED,
                         self.SUBJOB_STAGEIN_FAILED,
                         self.SUBJOB_COMPLETE_FAILED,
                         self.POSTPROCESSING_FAILED,
                         self.STAGEOUT_FAILED]
        
        if self.state in failed_states:
            return True
        else:
            return False
        
        

