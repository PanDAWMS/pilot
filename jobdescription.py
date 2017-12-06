import re
import logging
import json
import numbers

log = logging.getLogger(__name__)


def camel_to_snake(name):
    """
    Changes CamelCase to snake_case, used by python.

    :param name: name to change
    :return: name in snake_case
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def snake_to_camel(snake_str):
    """
    Changes snake_case to firstLowCamelCase, used by server.

    :param snake_str: name to change
    :return: name in camelCase
    """
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + "".join(x.title() for x in components[1:])


def split(val, separator=",", min_len=0, fill_last=False):
    """
    Splits comma separated values and parses them.

    :param val:         values to split
    :param separator:   comma or whatever
    :param min_len:     minimum needed length of array, array is filled up to this value
    :param fill_last:   Flag stating the array filler, if min_value is greater then extracted array length.
                        If true, array is filled with last value, else, with Nones.
    :return: parsed array
    """
    if val is None:
        return [None for _ in range(min_len)]

    v_arr = val.split(separator)

    for i, v in enumerate(v_arr):
        v_arr[i] = parse_value(v)

    if min_len > len(v_arr):
        filler = None if not fill_last or len(v_arr) < 1 else v_arr[0]
        v_arr.extend([filler for _ in range(min_len - len(v_arr))])

    return v_arr


def get_nulls(val):
    """
    Converts every "NULL" string to python's None.

    :param val: string or whatever
    :return: val or None if val is "NULL"
    """
    return val if val != "NULL" else None


def is_float(val):
    """
    Test floatliness of the string value.

    :param val: string or whatever
    :return: True if the value may be converted to Float
    """
    try:
        float(val)
        return True
    except ValueError:
        return False


def is_long(s):
    """
    Test value to be convertable to integer.

    :param s: string or whatever
    :return: True if the value may be converted to Long
    """
    if not isinstance(s, basestring):
        try:
            long(s)
            return True
        except ValueError:
            return False

    if s[0] in ('-', '+'):
        return s[1:].isdigit()
    return s.isdigit()


def parse_value(value):
    """
    Tries to parse value as number or None. If some of this can be done, parsed value is returned. Otherwise returns
    value unparsed.

    :param value:
    :return: mixed
    """
    if not isinstance(value, basestring):
        return value
    if is_long(value):
        return long(value)
    if is_float(value):
        return float(value)
    return get_nulls(value)


def stringify_weird(arg):
    """
    Converts None to "NULL"

    :param arg:
    :return: arg or "NULL"
    """
    if arg is None:
        return "NULL"
    if isinstance(arg, numbers.Number):
        return arg
    return str(arg)


def join(arr):
    """
    Joins arrays, converting contents to strings.

    :param arr:
    :return: joined array
    """
    return ",".join(str(stringify_weird(x)) for x in arr)


def get_input_files(description):
    """
    Extracts input files from the description.

    :param description:
    :return: file list
    """
    log.info("Extracting input files from job description")
    files = {}
    if description['inFiles'] and description['inFiles'] != "NULL":
        in_files = split(description["inFiles"])
        length = len(in_files)
        ddm_endpoint = split(description.get("ddmEndPointIn"), min_len=length)
        destination_se = split(description.get("destinationSE"), min_len=length)
        dispatch_dblock = split(description.get("dispatchDblock"), min_len=length)
        dispatch_dblock_token = split(description.get("dispatchDBlockToken"), min_len=length)
        datasets = split(description.get("realDatasetsIn"), min_len=length, fill_last=True)
        dblocks = split(description.get("prodDBlocks"), min_len=length)
        dblock_tokens = split(description.get("prodDBlockToken"), min_len=length)
        size = split(description.get("fsize"), min_len=length)
        c_sum = split(description.get("checksum"), min_len=length)
        scope = split(description.get("scopeIn"), min_len=length, fill_last=True)
        guids = split(description.get("GUID"), min_len=length, fill_last=True)

        for i, f in enumerate(in_files):
            if f is not None:
                files[f] = {
                    "ddm_endpoint": ddm_endpoint[i],
                    "storage_element": destination_se[i],
                    "dispatch_dblock": dispatch_dblock[i],
                    "dispatch_dblock_token": dispatch_dblock_token[i],
                    "dataset": datasets[i],
                    "dblock": dblocks[i],
                    "dblock_token": dblock_tokens[i],
                    "size": size[i],
                    "checksum": c_sum[i],
                    'scope': scope[i],
                    "guid": guids[i]
                }
    return files


def fix_log(description, files):
    """
    Fixes log file description in output files (changes GUID and scope).

    :param description:
    :param files: output files
    :return: fixed output files
    """
    log.info("modifying log-specific values in a log file description")
    if description["logFile"] and description["logFile"] != "NULL":
        if description["logGUID"] and description["logGUID"] != "NULL" and description["logFile"] in \
                files:
            files[description["logFile"]]["guid"] = description["logGUID"]
            files[description["logFile"]]["scope"] = description["scopeLog"]

    return files


def get_output_files(description):
    """
    Extracts output files from the description.

    :param description:
    :return: output files
    """
    log.info("Extracting output files in description")
    files = {}
    if description['outFiles'] and description['outFiles'] != "NULL":
        out_files = split(description["outFiles"])
        length = len(out_files)
        ddm_endpoint = split(description.get("ddmEndPointOut"), min_len=length)
        destination_se = split(description.get("fileDestinationSE"), min_len=length)
        dblock_token = split(description.get("dispatchDBlockTokenForOut"), min_len=length)
        dblock_tokens = split(description.get("prodDBlockTokenForOut"), min_len=length)
        datasets = split(description.get("realDatasets"), min_len=length)
        dblocks = split(description.get("destinationDblock"), min_len=length)
        destination_dblock_token = split(description.get("destinationDBlockToken"), min_len=length)
        scope = split(description.get("scopeOut"), min_len=length, fill_last=True)

        for i, f in enumerate(out_files):
            if f is not None:
                files[f] = {
                    "ddm_endpoint": ddm_endpoint[i],
                    "storage_element": destination_se[i],
                    "dispatch_dblock_token": dblock_token[i],
                    "destination_dblock_token": destination_dblock_token[i],
                    "dblock_token": dblock_tokens[i],
                    "dataset": datasets[i],
                    "dblock": dblocks[i],
                    "scope": scope[i]
                }

    return fix_log(description, files)


def one_or_set(array):
    if len(array) < 1:
        return join(array)

    zero = array[0]

    for i in array:
        if i != zero:
            return join(array)

    return stringify_weird(zero)


class JobDescription(object):
    __holder = None
    __key_aliases = {
        'PandaID': 'job_id',  # it is job id, not PanDA
        'transformation': 'command',  # making it more convenient
        'jobPars': 'command_parameters',  # -.-
        'coreCount': 'cores_number',
        'prodUserID': 'user_dn',
        'prodSourceLabel': 'label',  # We don't have any other labels in there. And this is The Label, or just label
        'homepackage': 'home_package',  # lowercase, all of a sudden, splitting words
        "nSent": 'throttle',  # as it's usage says
        'minRamCount': 'minimum_ram',  # reads better
        'maxDiskCount': 'maximum_input_file_size',
        'maxCpuCount': 'maximum_cpu_usage_time',
        'attemptNr': 'attempt_number',  # bad practice to strip words API needs to be readable
    }
    __key_back_aliases = {
        'task_id': 'taskID',  # all ID's are to be placed here, because snake case lacks of all-caps abbrev info
        'jobset_id': 'jobsetID',
        'job_definition_id': 'jobDefinitionID',
        'status_code': 'StatusCode',  # uppercase starting names also should be here
    }
    __soft_key_aliases = {
        'id': 'job_id'
    }

    __input_file_keys = {   # corresponding fields in input_files
        'inFiles': '',
        "ddmEndPointIn": 'ddm_endpoint',
        "destinationSE": 'storage_element',
        "dispatchDBlockToken": 'dispatch_dblock_token',
        "realDatasetsIn": 'dataset',
        "prodDBlocks": 'dblock',
        "fsize": 'size',
        "dispatchDblock": 'dispatch_dblock',
        'prodDBlockToken': 'dblock_token',
        "GUID": 'guid',
        "checksum": 'checksum',
        "scopeIn": 'scope'
    }
    __may_be_united = ['guid', 'scope', 'dataset']  # can be sent as one for all files, if is the same

    __output_file_keys = {   # corresponding fields in output_files
        'outFiles': '',
        'ddmEndPointOut': 'ddm_endpoint',
        'fileDestinationSE': 'storage_element',
        'dispatchDBlockTokenForOut': 'dispatch_dblock_token',
        'prodDBlockTokenForOut': 'dblock_token',
        'realDatasets': 'dataset',
        'destinationDblock': 'dblock',
        'destinationDBlockToken': 'destination_dblock_token',
        'scopeOut': 'scope',
        'logGUID': 'guid',
        'scopeLog': 'scope'
    }

    def __init__(self):
        super(JobDescription, self).__init__()

        self.__key_back_aliases_from_forward = self.__key_back_aliases.copy()
        self.__key_reverse_aliases = {}
        self.__key_aliases_snake = {}
        self.input_files = {}
        self.output_files = {}

        for key in self.__key_aliases:
            alias = self.__key_aliases[key]
            self.__key_back_aliases_from_forward[alias] = key
            self.__key_aliases_snake[camel_to_snake(key)] = alias

    def get_input_file_prop(self, key):
        corresponding_key = self.__input_file_keys[key]
        ret = []

        for f in self.input_files:
            ret.append(f if corresponding_key == '' else self.input_files[f][corresponding_key])

        if corresponding_key in self.__may_be_united:
            return one_or_set(ret)

        return join(ret)

    def get_output_file_prop(self, key):
        log_file = self.log_file

        if key == 'logGUID':
            return stringify_weird(self.output_files[log_file]['guid'])
        if key == 'scopeLog':
            return stringify_weird(self.output_files[log_file]['scope'])

        corresponding_key = self.__output_file_keys[key]
        ret = []

        for f in self.output_files:
            if key != 'scopeOut' or f != log_file:
                ret.append(f if corresponding_key == '' else self.output_files[f][corresponding_key])

        if corresponding_key in self.__may_be_united:
            return one_or_set(ret)

        return join(ret)

    def load(self, new_desc):
        if isinstance(new_desc, basestring):
            new_desc = json.loads(new_desc)

        if "PandaID" in new_desc:
            log.info("Parsing description to be of readable, easy to use format")

            fixed = {}

            self.input_files = get_input_files(new_desc)
            self.output_files = get_output_files(new_desc)

            for key in new_desc:
                value = new_desc[key]

                if key not in self.__input_file_keys and key not in self.__output_file_keys:
                    old_key = key
                    if key in self.__key_aliases:
                        key = self.__key_aliases[key]
                    else:
                        key = camel_to_snake(key)

                    if key != old_key:
                        self.__key_back_aliases_from_forward[key] = old_key

                    self.__key_reverse_aliases[old_key] = key

                    fixed[key] = parse_value(value)

            new_desc = fixed
        else:
            self.input_files = new_desc['input_files']
            self.output_files = new_desc['output_files']

        self.__holder = new_desc

    def to_json(self, decompose=False, **kwargs):
        if decompose:
            prep = {}

            for k in self.__holder:
                if k not in ['input_files', 'output_files']:
                    if k in self.__key_back_aliases_from_forward:
                        rev = self.__key_back_aliases_from_forward[k]
                    else:
                        rev = snake_to_camel(k)
                    prep[rev] = stringify_weird(self.__holder[k])

            for k in self.__output_file_keys:
                prep[k] = self.get_output_file_prop(k)
            for k in self.__input_file_keys:
                prep[k] = self.get_input_file_prop(k)

        else:
            prep = self.__holder.copy()
            prep['input_files'] = self.input_files
            prep['output_files'] = self.output_files

        return json.dumps(prep, **kwargs)

    def __getattr__(self, key):
        """
        Reflection of description values into Job instance properties if they are not shadowed.
        If there is no own property with corresponding name, the value of Description is used.
        Params and return described in __getattr__ interface.
        """
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            if self.__holder is not None:
                if key in self.__holder:
                    return self.__holder[key]

                if key in self.__input_file_keys:
                    return self.get_input_file_prop(key)
                if key in self.__output_file_keys:
                    return self.get_output_file_prop(key)

                snake_key = camel_to_snake(key)
                if snake_key in self.__key_aliases_snake:
                    return stringify_weird(self.__holder[self.__key_aliases_snake[snake_key]])

                if key in self.__soft_key_aliases:
                    return self.__getattr__(self.__soft_key_aliases[key])
            raise

    def __setattr__(self, key, value):
        """
        Reflection of description values into Job instance properties if they are not shadowed.
        If there is no own property with corresponding name, the value of Description is set.
        Params and return described in __setattr__ interface.
        """
        try:
            object.__getattribute__(self, key)
            return object.__setattr__(self, key, value)
        except AttributeError:
            if self.__holder is not None:
                if key in self.__holder:
                    self.__holder[key] = value
                    return

                if key in self.__input_file_keys:
                    err = "Key JobDescription.%s is read-only\n" % key
                    if key == 'inFiles':
                        err += "Use JobDescription.input_files to manipulate input files"
                    else:
                        err += "Use JobDescription.input_files[][%s] to set up this parameter in files description" % self.__input_file_keys[key]
                    raise AttributeError(err)

                if key in self.__output_file_keys:
                    err = "Key JobDescription.%s is read-only\n" % key
                    if key == 'outFiles':
                        err += "Use JobDescription.output_files to manipulate output files"
                    else:
                        err += "Use JobDescription.output_files[][%s] to set up this parameter in files description" % self.__output_file_keys[key]
                    raise AttributeError(err)

                snake_key = camel_to_snake(key)
                if snake_key in self.__key_aliases_snake:
                    log.warning("Better to use %s to access and manipulate this value" % self.__key_aliases_snake[snake_key])
                    self.__holder[self.__key_aliases_snake[snake_key]] = parse_value(value)

                elif key in self.__soft_key_aliases:
                    return self.__setattr__(self.__soft_key_aliases[key], value)
            return object.__setattr__(self, key, value)


if __name__ == "__main__":
    import sys
    logging.basicConfig()
    log.setLevel(logging.DEBUG)

    jd = JobDescription()
    with open(sys.argv[1], "r") as f:
        contents = f.read()

    jd.load(contents)

    log.debug(jd.id)
    log.debug(jd.command)
    log.debug(jd.PandaID)
    log.debug(jd.scopeOut)
    log.debug(jd.scopeLog)
    log.debug(jd.fileDestinationSE)
    log.debug(jd.inFiles)
    log.debug(json.dumps(jd.output_files, indent=4, sort_keys=True))

    log.debug(jd.to_json(True, indent=4, sort_keys=True))
