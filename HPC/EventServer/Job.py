import os

class Job:
    def __init__(self):
        pass

    def getTokenExtractorCmd(self):
        setup = "export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;source $AtlasSetup/scripts/asetup.sh 19.X.0,rel_6,notest,here --cmtconfig x86_64-slc6-gcc47-opt"
        tokenExtractorCmd = setup + ";" + "TokenExtractor -v  --source /afs/cern.ch/user/w/wguan/Panda/Yoda/TokenExtractor_filelist 1>tokenExtract_stdout.txt 2>tokenExtract_stderr.txt"
        return tokenExtractorCmd

    def getAthenaMPCommand(self):
        setup = "export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;source $AtlasSetup/scripts/asetup.sh 19.X.0,rel_6,notest,here --cmtconfig x86_64-slc6-gcc47-opt"
        setupMultiCore = "export ATHENA_PROC_NUMBER=2"
        athenaMPCommand = setup + ";" + setupMultiCore + ";" + "export TRF_ECHO=1; AtlasG4_tf.py --inputEvgenFile=/afs/cern.ch/user/w/wguan/Panda/Yoda/EVNT.01461041._000001.pool.root.1  --outputHitsFile=panda.jeditest.HITS.37e5baea-c2dc-4d01-863b-d5afdfa78dc4.000001.HITS.pool.root.1 --maxEvents=3 --randomSeed=1 --firstEvent=1001 --geometryVersion ATLAS-GEO-18-01-03 --physicsList QGSP_BERT --conditionsTag OFLCOND-MC12-SDR-06 --preExec \"from AthenaMP.AthenaMPFlags import jobproperties as jps\" \"jps.AthenaMPFlags.Strategy='TokenScatterer'\" \"from AthenaCommon.AppMgr import ServiceMgr as svcMgr\" \"from AthenaServices.AthenaServicesConf import OutputStreamSequencerSvc\" \"outputStreamSequencerSvc = OutputStreamSequencerSvc()\" \"outputStreamSequencerSvc.SequenceIncidentName = 'NextEventRange'\" \"outputStreamSequencerSvc.IgnoreInputFileBoundary = True\" \"svcMgr += outputStreamSequencerSvc\" --postExec 'svcMgr.PoolSvc.ReadCatalog += [\"xmlcatalog_file:/afs/cern.ch/user/w/wguan/Panda/Yoda/PFC.xml\"]' 1>athena_stdout.txt 2>athena_stderr.txt"
        return athenaMPCommand

