# Error codes

class PilotErrors:
    """ Pilot error handling """

    # error codes
    ERR_UNKNOWNERROR = 0
    ERR_GENERALERROR = 1008
    ERR_DIRECTIOFILE = 1009 # harmless, just means that copy-to-scratch was skipped in favor or direct i/o access
    ERR_GETDATAEXC = 1097
    ERR_NOLOCALSPACE = 1098
    ERR_STAGEINFAILED = 1099
    ERR_REPNOTFOUND = 1100
    ERR_LRCREGCONNREF = 1101
    ERR_NOSUCHFILE = 1103
    ERR_USERDIRTOOLARGE = 1104
    ERR_LFCADDCSUMFAILED = 1105
    ERR_STDOUTTOOBIG = 1106
    ERR_MISSDBREL = 1107
    ERR_FAILEDLCGREG = 1108
    ERR_CMTCONFIG = 1109
    ERR_SETUPFAILURE = 1110
    ERR_RUNJOBEXC = 1111
    ERR_PILOTEXC = 1112
    ERR_GETLFCIMPORT = 1113
    ERR_PUTLFCIMPORT = 1114
    ERR_NFSSQLITE = 1115
    ERR_QUEUEDATA = 1116
    ERR_QUEUEDATANOTOK = 1117
    ERR_CURLSPACE = 1118
    ERR_DDMSPACE = 1119
    ERR_NOSTMATCHDEST = 1120 # not used
    ERR_NOLFCSFN = 1122
    ERR_MISSINGGUID = 1123
    ERR_OUTPUTFILETOOLARGE = 1124
    ERR_NOPFC = 1130
    ERR_PUTFUNCNOCALL = 1131
    ERR_LRCREG = 1132
    ERR_NOSTORAGE = 1133
    ERR_MKDIR = 1134
    ERR_FAILEDSIZELOCAL = 1135
    ERR_FAILEDMD5LOCAL = 1136
    ERR_STAGEOUTFAILED = 1137
    ERR_FAILEDSIZE = 1138
    ERR_PUTWRONGSIZE = 1139
    ERR_FAILEDMD5 = 1140
    ERR_PUTMD5MISMATCH = 1141
    ERR_CHMODTRF = 1143
    ERR_PANDAKILL = 1144
    ERR_GETMD5MISMATCH = 1145
    ERR_DYNTRFINST = 1146
    ERR_FAILEDRM = 1148
    ERR_TRFDOWNLOAD = 1149
    ERR_LOOPINGJOB = 1150
    ERR_GETTIMEOUT = 1151
    ERR_PUTTIMEOUT = 1152
    ERR_LOSTJOBNOTFINISHED = 1153
    ERR_LOSTJOBLOGREG = 1154
    ERR_LOSTJOBFILETRANSFER = 1155
    ERR_LOSTJOBRECOVERY = 1156
    ERR_LOSTJOBMAXEDOUT = 1158
    ERR_LOSTJOBPFC = 1159
    ERR_LRCREGSTRSIZE = 1160
    ERR_LOSTJOBXML = 1161
    ERR_LRCREGDUP = 1162
    ERR_NOPROXY = 1163
    ERR_MISSINGLOCALFILE = 1164
    ERR_MISSINGOUTPUTFILE = 1165
    ERR_SIGPIPE = 1166
    ERR_MISSFILEXML = 1167
    ERR_SIZETOOLARGE = 1168
    ERR_FAILEDLFCREG = 1169
    ERR_FAILEDADLOCAL = 1170
    ERR_GETADMISMATCH = 1171
    ERR_PUTADMISMATCH = 1172
    ERR_PANDAMOVERFILENOTCACHED = 1173
    ERR_PANDAMOVERTRANSFER = 1174
    ERR_GETWRONGSIZE = 1175
    ERR_NOCHILDPROCESSES = 1176
    ERR_NOVOMSPROXY = 1177
    ERR_NOSTAGEDFILES = 1178
    ERR_FAILEDLFCGETREPS = 1179
    ERR_GETGLOBUSSYSERR = 1180
    ERR_PUTGLOBUSSYSERR = 1181
    ERR_FAILEDLFCGETREP = 1182
    ERR_GUIDSEXISTSINLRC = 1183
    ERR_MISSINGPFC = 1184
    ERR_NOSOFTWAREDIR = 1186
    ERR_NOPAYLOADMETADATA = 1187
    ERR_LCGGETTURLS = 1188
    ERR_LCGGETTURLSTIMEOUT = 1189
    ERR_LFNTOOLONG = 1190
    ERR_ZEROFILESIZE = 1191
    ERR_DBRELNOTYETTRANSFERRED = 1192
    ERR_SEPROBLEM = 1193
    ERR_NOFILEVERIFICATION = 1194
    ERR_COMMANDTIMEOUT = 1195
    ERR_GETFAILEDTOMOUNTNFS4 = 1196
    ERR_GETPNFSSYSTEMERROR = 1197
    ERR_MKDIRWORKDIR = 1199
    ERR_KILLSIGNAL = 1200
    ERR_SIGTERM = 1201
    ERR_SIGQUIT = 1202
    ERR_SIGSEGV = 1203
    ERR_SIGXCPU = 1204
#    ERR_USERKILL = 1205 # not used by pilot
    ERR_SIGBUS = 1206
    ERR_SIGUSR1 = 1207
    ERR_NOPAYLOADOUTPUT = 1210
    ERR_MISSINGINSTALLATION = 1211
    ERR_PAYLOADOUTOFMEMORY = 1212
    ERR_REACHEDMAXTIME = 1213
    ERR_DAFSNOTALLOWED = 1214
    ERR_NOTCPCONNECTION = 1215
    ERR_NOPILOTTCPSERVER = 1216
    ERR_RUNEVENTEXC = 1218
    ERR_CORECOUNTMISMATCH = 1217
    ERR_UUIDGEN = 1219
    ERR_UNKNOWN = 1220
    ERR_FILEEXIST = 1221
    ERR_GETKEYPAIR = 1222
    ERR_BADALLOC = 1223
    ERR_ESRECOVERABLE = 1224
    ERR_ESMERGERECOVERABLE = 1225
    ERR_GLEXEC = 1226
    ERR_ESATHENAMPDIED = 1227
    # internal error codes
    ERR_DDMREG = 1
    ERR_FILEONTAPE = 2

    pilotError = {
        ERR_UNKNOWNERROR : "",
        ERR_GENERALERROR : "General pilot error, consult batch log",
        ERR_GETDATAEXC : "Get function can not be called for staging input file",
        ERR_NOLOCALSPACE : "No space left on local disk",
        ERR_STAGEINFAILED : "Get error: Staging input file failed",
        ERR_REPNOTFOUND : "Get error: Replica not found",
        ERR_LRCREGCONNREF : "LRC registration error: Connection refused",
        # 1102 : "Expected output file does not exist", # not used, see ERR_MISSINGOUTPUTFILE below
        ERR_NOSUCHFILE : "No such file or directory",
        ERR_USERDIRTOOLARGE : "User work directory too large",
        ERR_LFCADDCSUMFAILED : "Put error: Failed to add file size and checksum to LFC",
        ERR_STDOUTTOOBIG : "Payload stdout file too big",
        ERR_MISSDBREL : "Get error: Missing DBRelease file",
        ERR_FAILEDLCGREG : "Put error: LCG registration failed",
        ERR_CMTCONFIG : "Required CMTCONFIG incompatible with WN",
        ERR_SETUPFAILURE : "Failed during setup",
        ERR_RUNJOBEXC : "Exception caught by RunJob*", 
        ERR_PILOTEXC : "Exception caught by pilot", 
        ERR_GETLFCIMPORT : "Get error: Failed to import LFC python module",
        ERR_PUTLFCIMPORT : "Put error: Failed to import LFC python module",
        ERR_NFSSQLITE : "NFS SQLite locking problems",
        ERR_QUEUEDATA : "Pilot could not download queuedata",
        ERR_QUEUEDATANOTOK : "Pilot found non-valid queuedata",
        ERR_CURLSPACE : "Pilot could not curl space report",
        ERR_DDMSPACE : "Pilot aborted due to DDM space shortage",
        ERR_NOSTMATCHDEST : "Space token descriptor does not match destination path",
        # 1121 : "Can not read the xml file for registering output files to dispatcher", # not used
        ERR_NOLFCSFN : "Bad replica entry returned by lfc_getreplicas(): SFN not set in LFC for this guid",
        ERR_MISSINGGUID : "Missing guid in output file list",
        ERR_OUTPUTFILETOOLARGE : "Output file too large",
        ERR_NOPFC : "Get error: Failed to get PoolFileCatalog",
        ERR_PUTFUNCNOCALL : "Put function can not be called for staging out",
        ERR_LRCREG : "LRC registration error (consult log file)",
        ERR_NOSTORAGE : "Put error: Fetching default storage URL failed",
        ERR_MKDIR : "Put error: Error in mkdir on localSE, not allowed or no available space",
        ERR_FAILEDSIZELOCAL : "Could not get file size in job workdir",
        ERR_FAILEDMD5LOCAL : "Error running md5sum on the file in job workdir",
        ERR_STAGEOUTFAILED : "Put error: Error in copying the file from job workdir to localSE",
        ERR_FAILEDSIZE : "Put error: could not get the file size on localSE",
        ERR_PUTWRONGSIZE : "Put error: Problem with copying from job workdir to local SE: size mismatch",
        ERR_FAILEDMD5 : "Put error: Error running md5sum on the file on local SE",
        ERR_PUTMD5MISMATCH : "Put error: Problem with copying from job workdir to local SE: md5sum mismatch",
        # 1142 : "Put error: failed to register the file on local SE", # not used
        ERR_CHMODTRF : "Failed to chmod trf",
        ERR_PANDAKILL : "This job was killed by panda server",
        ERR_GETMD5MISMATCH : "Get error: md5sum mismatch on input file",
        ERR_DYNTRFINST : "Trf installation dir does not exist and could not be installed",
        # 1147 : "Put error: dccp returned readOnly",  # not used
        ERR_FAILEDRM : "Put error: Failed to remove readOnly file in dCache", 
        ERR_TRFDOWNLOAD : "wget command failed to download trf",
        ERR_LOOPINGJOB : "Looping job killed by pilot",
        ERR_GETTIMEOUT : "Get error: Input file staging timed out",
        ERR_PUTTIMEOUT : "Put error: File copy timed out",
        ERR_LOSTJOBNOTFINISHED : "Lost job was not finished",
        ERR_LOSTJOBLOGREG : "Failed to register log file",
        ERR_LOSTJOBFILETRANSFER : "Failed to move output files for lost job",
        ERR_LOSTJOBRECOVERY : "Pilot could not recover job",
        # 1157 : "Could not create log file", # not used
        ERR_LOSTJOBMAXEDOUT : "Reached maximum number of recovery attempts",
        ERR_LOSTJOBPFC : "Job recovery could not read PoolFileCatalog.xml file (guids lost)",
        ERR_LRCREGSTRSIZE : "LRC registration error: file name string size exceeded limit of 250",
        ERR_LOSTJOBXML : "Job recovery could not generate xml for remaining output files",
        ERR_LRCREGDUP : "LRC registration error: Non-unique LFN",
        ERR_NOPROXY : "Grid proxy not valid",
        ERR_MISSINGLOCALFILE : "Get error: Local input file missing",
        ERR_MISSINGOUTPUTFILE : "Put error: Local output file missing",
        ERR_SIGPIPE : "Put error: File copy broken by SIGPIPE",
        ERR_MISSFILEXML : "Get error: Input file missing in PoolFileCatalog.xml",
        ERR_SIZETOOLARGE : "Get error: Total file size too large",
        ERR_FAILEDLFCREG : "Put error: File registration failed",
        ERR_FAILEDADLOCAL : "Error running adler32 on the file in job workdir",
        ERR_GETADMISMATCH : "Get error: adler32 mismatch on input file",
        ERR_PUTADMISMATCH : "Put error: adler32 mismatch on output file",
        ERR_PANDAMOVERFILENOTCACHED : "PandaMover staging error: File is not cached",
        ERR_PANDAMOVERTRANSFER : "PandaMover transfer failure",
        ERR_GETWRONGSIZE : "Get error: Problem with copying from local SE to job workdir: size mismatch",
        ERR_NOCHILDPROCESSES : "Pilot has no child processes (job wrapper has either crashed or did not send final status)",
        ERR_NOVOMSPROXY : "Voms proxy not valid",
        ERR_NOSTAGEDFILES : "Get error: No input files are staged",
        ERR_FAILEDLFCGETREPS : "Get error: Failed to get replicas",
        ERR_GETGLOBUSSYSERR : "Get error: Globus system error",
        ERR_PUTGLOBUSSYSERR : "Put error: Globus system error",
        ERR_FAILEDLFCGETREP : "Get error: Failed to get replica",
        ERR_GUIDSEXISTSINLRC : "LRC registration error: Guid-metadata entry already exists",
        ERR_MISSINGPFC : "Put error: PoolFileCatalog could not be found in workdir",
        # 1185 : "Put error: Error running adler32 on the file in job workdir", # not used
        ERR_NOSOFTWAREDIR : "Software directory does not exist",
        ERR_NOPAYLOADMETADATA : "Payload metadata is not available",
        ERR_LCGGETTURLS : "lcg-getturls failed",
        ERR_LCGGETTURLSTIMEOUT : "lcg-getturls was timed-out", 
        ERR_LFNTOOLONG : "LFN too long (exceeding limit of 150 characters)",
        ERR_ZEROFILESIZE : "Illegal zero file size",
        ERR_DBRELNOTYETTRANSFERRED : "DBRelease file has not been transferred yet",
        ERR_NOFILEVERIFICATION : "File verification failed",
        ERR_COMMANDTIMEOUT : "Command timed out",
        ERR_GETFAILEDTOMOUNTNFS4 : "Get error: Failed to mount NSF4",
        ERR_GETPNFSSYSTEMERROR : "Get error: PNFS system error",
        # 1198 : "Can not check the child process status from the heartbeat process", # not used
        ERR_MKDIRWORKDIR : "Could not create directory",
        ERR_KILLSIGNAL : "Job terminated by unknown kill signal",
        ERR_SIGTERM : "Job killed by signal: SIGTERM",
        ERR_SIGQUIT : "Job killed by signal: SIGQUIT",
        ERR_SIGSEGV : "Job killed by signal: SIGSEGV",
        ERR_SIGXCPU : "Job killed by signal: SIGXCPU",
        ERR_SIGUSR1 : "Job killed by signal: SIGUSR1",
        ERR_SIGBUS : "Job killed by signal: SIGBUS",
        ERR_NOPAYLOADOUTPUT : "No payload output",
        ERR_MISSINGINSTALLATION : "Missing installation",
        ERR_PAYLOADOUTOFMEMORY : "Payload ran out of memory",
        ERR_REACHEDMAXTIME : "Reached batch system time limit",
        ERR_DAFSNOTALLOWED : "Site does not allow requested direct access or file stager",
        ERR_NOTCPCONNECTION : "Failed to open TCP connection to localhost (worker node network problem)",
        ERR_NOPILOTTCPSERVER : "Pilot TCP server has died",
        ERR_CORECOUNTMISMATCH : "Mismatch between core count in job and queue definition",
        ERR_RUNEVENTEXC : "Exception caught by runEvent", 
        ERR_UNKNOWN : "Job failed due to unknown reason (consult log file)",
        ERR_FILEEXIST : "File already exist",
        ERR_GETKEYPAIR : "Failed to get security key pair",
        ERR_BADALLOC : "TRF failed due to bad_alloc",
        ERR_ESMERGERECOVERABLE : "Recoverable Event Service Merge error",
        ERR_ESRECOVERABLE: "Recoverable Event Service error",
	ERR_GLEXEC: "gLExec related error",
        ERR_ESATHENAMPDIED: "AthenaMP ended Event Service job prematurely"
        }

    getErrorCodes = [1097, 1099, 1100, 1103, 1107, 1113, 1130, 1145, 1151, 1164, 1167, 1168, 1171, 1175, 1178, 1179, 1180, 1182]
    putErrorCodes = [1101, 1114, 1122, 1131, 1132, 1133, 1134, 1135, 1136, 1137, 1138, 1140, 1141, 1152, 1154, 1155, 1181]
    recoverableErrorCodes = [0] + putErrorCodes

    # Error codes that will issue a Pilot-controlled resubmission
    PilotResubmissionErrorCodes = [1008, 1098, 1099, 1110, 1113, 1114, 1115, 1116, 1117, 1137, 1139, 1151, 1152, 1171, 1172, 1177, 1179, 1180, 1181, 1182, 1188, 1189, 1195, 1196, 1197, 1219]

    # Error codes used with FAX fail-over (only an error code in this list will allow FAX fail-over)
    PilotFAXErrorCodes = [1103] + PilotResubmissionErrorCodes

    def getPilotErrorDiag(self, code=0):
        """ Return text corresponding to error code """
        
        pilotErrorDiag = ""
        if code in self.pilotError.keys():
            pilotErrorDiag = self.pilotError[code]
        else:
            pilotErrorDiag = "Unknown pilot error code"
        return pilotErrorDiag

    def isGetErrorCode(self, code=0):
        """ Determine whether code is in the put error list or not """

        state = False
        if code in self.getErrorCodes:
            state = True

        return state

    def isPutErrorCode(self, code=0):
        """ Determine whether code is in the put error list or not """

        state = False
        if code in self.putErrorCodes:
            state = True

        return state

    def isRecoverableErrorCode(self, code=0):
        """ Determine whether code is a recoverable error code or not """

        state = False
        if code in self.recoverableErrorCodes:
            state = True

        return state
    
    def isPilotResubmissionErrorCode(self, code=0):
        """ Determine whether code issues a Pilot-controlled resubmission """

        state = False
        if code in self.PilotResubmissionErrorCodes:
            state = True

        return state

    def isPilotFAXErrorCode(self, code=0):
        """ Determine whether code allows for a FAX fail-over """

        state = False
        if code in self.PilotFAXErrorCodes:
            state = True

        return state

    def getErrorStr(self, s):
        """
        Avoids exception if an error is not in the dictionary.
        An empty string is returned if the error is not in the dictionary.
        """
        try:
            rets = self.pilotError[s]
        except:
            rets = ''
        return rets

    def getErrorName(self, code):
        """ From the error code to get the error name"""
        for k in self.__class__.__dict__.keys():
            if self.__class__.__dict__[k] == code:
                return k
        return None
