import sys
from pUtil import serialize, deserialize, convert_unicode_string, tolog

class BalsamJob:
    def __init__(self,
                executable          = None,
                executable_args     = None,
                input_url           = None,
                input_files         = [],
                output_url          = None,
                output_files        = [],
                preprocess          = None,
                preprocess_args     = None,
                postprocess         = None,
                postprocess_args    = None,
                nodes               = None,
                processes_per_node  = None,
                scheduler_args      = None,
                wall_minutes        = None,
                number_events       = None,
                condor_job_file     = None,
                condor_dagman_file  = None,
                target_site         = None,
               ):
        self.executable         = executable
        self.executable_args    = executable_args
        self.input_url          = input_url
        self.input_files        = input_files
        self.output_url         = output_url
        self.output_files       = output_files
        self.preprocess         = preprocess
        self.preprocess_args    = preprocess_args
        self.postprocess        = postprocess
        self.postprocess_args   = postprocess_args
        self.nodes              = nodes
        self.processes_per_node = processes_per_node
        self.scheduler_args     = scheduler_args
        self.wall_minutes       = wall_minutes
        self.number_events      = number_events
        self.condor_job_file    = condor_job_file
        self.condor_dagman_file = condor_dagman_file
        self.target_site        = target_site



    def serialize(self):
        try:
            return serialize(self.__dict__)
        except:
            tolog(' received exception while converting BalsamJob to json string: ' + str(sys.exc_info()[1]))
            raise

    """
    {"num_evts": -1, "exe": "zjetgen90", "exe_args": "input.0", "preprocess_args": null, "postprocess_args": null, 
    "scheduler_args": null, "preprocess": null, "wall_minutes": 60, "postprocess": null, "processes_per_node": 1, 
    "nodes": 1, "output_files": ["alpout.grid1,alpout.grid2"], "input_files": ["alpout.input.0", "cteq6l1.tbl"]}
   """

    def deserialize(self,text_string):
        tolog("Job received: %s", text_string)
        try:
            self.__dict__ = deserialize(text_string)
        except:
            tolog(' received exception while converting json string to BalsamJob: ' + str(sys.exc_info()[1]))
            raise

        self.executable         = convert_unicode_string(self.executable)
        self.executable_args    = convert_unicode_string(self.executable_args)
        self.input_url          = convert_unicode_string(self.input_url)
        tmp_input_files = []
        for i_file in self.input_files:
            tmp_input_files.append(convert_unicode_string(i_file))
        self.input_files = tmp_input_files
        self.output_url         = convert_unicode_string(self.output_url)
        tmp_output_files = []
        for o_file in self.output_files:
            tmp_output_files.append(convert_unicode_string(o_file))
        self.output_files = tmp_output_files
        self.preprocess         = convert_unicode_string(self.preprocess)
        self.preprocess_args    = convert_unicode_string(self.preprocess_args)
        self.postprocess        = convert_unicode_string(self.postprocess)
        self.postprocess_args   = convert_unicode_string(self.postprocess_args)
        self.scheduler_args     = convert_unicode_string(self.scheduler_args)
        self.condor_job_file    = convert_unicode_string(self.condor_job_file)
        self.condor_dagman_file = convert_unicode_string(self.condor_dagman_file)
        self.target_site        = convert_unicode_string(self.target_site)

