import commands
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import traceback
try:
    import argparse
except:
    print "!!WARNING!!4545!! argparse python module could not be imported - too old python version?"

import pUtil
from ThreadPool import ThreadPool
from objectstoreSiteMover import objectstoreSiteMover
from Mover import getInitialTracingReport

from EventStager import EventStager

logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG,
                    format='%(asctime)s\t%(process)d\t%(levelname)s\t%(message)s')


class MVEventStager(EventStager):
    def __init__(self, workDir, setup, esPath, token, experiment, userid, sitename, outputDir=None, yodaToOS=False, threads=10, isDaemon=False):
        super(MVEventStager, self).__init__(workDir, setup, esPath, token, experiment, userid, sitename, outputDir, yodaToOS, threads, isDaemon)

        self.__workDir = workDir
        self.__updateEventRangesDir = os.path.join(self.__workDir, 'MVupdateEventRanges')
        if not os.path.exists(self.__updateEventRangesDir):
            os.makedirs(self.__updateEventRangesDir)
        self.__logFile = os.path.join(workDir, 'EventStager.log')
        self.__setup = setup
        self.__siteMover = objectstoreSiteMover(setup)
        self.__esPath = esPath
        self.__token = token
        self.__experiment = experiment
        self.__outputDir = outputDir

        self.__userid = userid
        self.__sitename = sitename

        self.__report =  getInitialTracingReport(userid=self.__userid, sitename=self.__sitename, dsname=None, eventType="objectstore", analysisJob=False, jobId=None, jobDefId=None, dn=self.__userid)

        self.__eventRanges = {}
        self.__eventRanges_staged = {}
        self.__eventRanges_faileStaged = {}

        self.__eventStager = None
        self.__canFinish = False
        self.__status = 'new'
        self.__threads = threads
        self.__yodaToOS = yodaToOS
        self.__isDaemon = isDaemon

        self.__threadpool = ThreadPool(self.__threads)
        logging.info("Init EventStager workDir %s setup %s esPath %s token %s experiment %s userid %s sitename %s threads %s outputDir %s isDaemond %s" % (self.__workDir, self.__setup, self.__esPath, self.__token, self.__experiment, self.__userid, self.__sitename, self.__threads, self.__outputDir, self.__isDaemon))

    def renewEventStagerStatus(self):
        canFinish_file = os.path.join(self.__workDir, 'EventStagerStatusCan.json')
        finished_file = os.path.join(self.__workDir, 'EventStagerStatus.json')
        if self.__isDaemon:
            if os.path.exists(canFinish_file):
                #with open(canFinish_file) as inputFile:
                #    origin_status = json.load(inputFile)
                #    self.__canFinish = origin_status['canFinish']
                self.__canFinish = True
            if self.__status == "finished":
                status = {'status': self.__status}
                with open(finished_file, 'w') as outputFile:
                    json.dump(status, outputFile)
            elif os.path.exists(finished_file):
                os.remove(finished_file)
        else:
            if os.path.exists(finished_file):
                #with open(finished_file) as inputFile:
                #    origin_status = json.load(inputFile)
                #    self.__status = origin_status['status']
                self.__status = "finished"
            if self.__canFinish:
                status = {'canFinish': self.__canFinish}
                with open(canFinish_file, 'w') as outputFile:
                    json.dump(status, outputFile)
            elif os.path.exists(canFinish_file):
                os.remove(canFinish_file)

    def start(self):
        try:
            self.renewEventStagerStatus()
            if self.__setup and len(self.__setup.strip()):
                cmd = 'python %s/EventStager.py --workDir %s --setup %s --esPath %s --token %s --experiment %s --userid %s --sitename %s --threads %s --outputDir %s --isDaemon 2>&1 1>>%s' % (self.__workDir, self.__workDir, self.__setup, self.__esPath, self.__token, self.__experiment, self.__userid, self.__sitename, self.__threads, self.__outputDir, self.__logFile)
            else:
                cmd = 'python %s/EventStager.py --workDir %s --esPath %s --token %s --experiment %s --userid %s --sitename %s --threads %s --outputDir %s --isDaemon 2>&1 1>>%s' % (self.__workDir, self.__workDir, self.__esPath, self.__token, self.__experiment, self.__userid, self.__sitename, self.__threads, self.__outputDir, self.__logFile)
            pUtil.tolog("Start Event Stager: %s" % cmd)
            self.__eventStager = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stdout, shell=True)
        except:
            pUtil.tolog("Failed to start Event Stager: %s" % traceback.format_exc())

    def getLog(self):
        return self.__logFile

    def monitor(self):
        try:
            self.renewEventStagerStatus()
            if not self.isFinished() and self.__eventStager is None or self.__eventStager.poll() is not None:
                pUtil.tolog("Event Stager failed. Try to start it.")
                self.start()
        except:
            pUtil.tolog("Failed to monitor Event Stager: %s" % traceback.format_exc())

    def finish(self):
        try:
            pUtil.tolog("Tell Event Stager to finish after finishing staging out all events")
            self.__canFinish = True
            self.renewEventStagerStatus()
        except:
            pUtil.tolog("Failed to monitor Event Stager: %s" % traceback.format_exc())

    def terminate(self):
        try:
            pUtil.tolog("Terminate Event Stager")
            self.__eventStager.terminate()
        except:
            pUtil.tolog("Failed to terminate Event Stager: %s" % traceback.format_exc())

    def isFinished(self):
        if self.__canFinish and self.__status == 'finished':
            return True
        return False

    def getUnstagedOutputFiles(self, ext=".dump"):
        outputFiles = []
        all_files = os.listdir(self.__workDir)
        for file in all_files:
            if file.endswith(ext):
                filename = os.path.join(self.__workDir, file)
                outputFiles.append(file)
        # if outputFiles:
        #     logging.info("UnStaged Output files: %s" % outputFiles)
        return outputFiles

    def cleanStagingFiles(self):
        self.__eventRanges = {}
        self.__eventRanges_staged = {}
        self.__eventRanges_faileStaged = {}
        all_files = os.listdir(self.__workDir)
        for file in all_files:
            if file.endswith(".dump.staging"):
                filepath = os.path.join(self.__workDir, file)
                os.rename(filepath, filepath.replace(".dump.staging", ".dump"))

    def processFile(self, outputFile):
        handle = open(outputFile)
        toMvFiles = []
        yodaStagedFiles = []
        yodaReportedFiles = []
        for line in handle:
            line = line.replace("  ", " ")
            jobId, eventRange, status, output = line.split(" ")
            if status == 'stagedOut' or status.startswith("ERR"):
                yodaStagedFiles.append(line)
            elif status == 'reported':
                yodaReportedFiles.append(line)
                yodaStagedFiles.append(line)
            else:
                toMvFiles.append(line)
        handle.close()

        if toMvFiles:
            handlemv = open(outputFile + ".mv", "w")
            for i in range(3):
                tmpToMvFiles = toMvFiles
                toMvFiles = []
                for line in tmpToMvFiles:
                    line = line.replace("  ", " ")
                    jobId, eventRange, status, output = line.split(" ")
                    outputPath = output.split(",")[0]
                    newOutputPath = os.path.join(self.__outputDir, os.path.basename(outputPath))
                    command = "mv -f %s %s" % (outputPath, newOutputPath)
                    logging.debug("Execute command %s" % command)
                    status, output = commands.getstatusoutput(command)
                    logging.debug("Status %s output %s" % (status, output))
                    if status:
                        toMvFiles.append(line)
                    else:
                        handlemv.write(line.replace(outputPath, newOutputPath)+"\n")
            if toMvFiles:
                logging.error("Failed mv files: %s" % toMvFiles)
            handlemv.close()
            shutil.copyfile(outputFile + ".mv", os.path.join(self.__outputDir, os.path.basename(outputFile)))
            os.rename(outputFile, outputFile + ".BAK")

        if yodaReportedFiles:
            handleYoda = open(outputFile + ".yodaReported", "w")
            for line in yodaStagedFiles:
                handleYoda.write(line + "\n")
            handleYoda.close()
            if os.path.exists(outputFile):
                os.rename(outputFile, outputFile + ".BAK")

        if yodaStagedFiles:
            handleYoda = open(outputFile + ".yodaStaged", "w")
            for line in yodaStagedFiles:
                handleYoda.write(line + "\n")
            handleYoda.close()
            handleYoda = open(outputFile + ".yodaStaged.reported", "w")
            for chunk in pUtil.chunks(yodaStagedFiles, 100):
                try:
                    eventRanges = []
                    for outputEvents in chunk:
                        jobId, eventRangeID, status, output = outputEvents.split(" ")
                        if status == 'stagedOut' or status == 'reported':
                            eventRanges.append({"eventRangeID": eventRangeID, "eventStatus": 'finished'})
                        if status.startswith("ERR"):
                            eventRanges.append({"eventRangeID": eventRangeID, "eventStatus": 'failed'})

                    update_status, update_output = self.updateEventRanges(eventRanges)
                    logging.info("update Event Range: status: %s, output: %s" % (update_status, update_output))
                    if update_status:
                        update_status, update_output = self.updateEventRanges(eventRanges)
                        logging.info("update Event retry Range: status: %s, output: %s" % (update_status, update_output))
                    if update_status == 0:
                        try:
                            ret_outputs = json.loads(json.loads(update_output))
                            if len(ret_outputs) == len(chunk):
                                for i in range(len(ret_outputs)):
                                    try:
                                        if ret_outputs[i]:
                                            jobId, eventRangeID, status, output = chunk[i].split(" ")
                                            handleYoda.write('{0} {1} {2} {3}\n'.format(jobId, eventRangeID, status, output))
                                    except:
                                        logging.warning("Failed to book updated status %s: %s" % (output, traceback.format_exc()))
                        except:
                                    logging.warning(traceback.format_exc())
                except:
                    logging.warning(traceback.format_exc())
            handleYoda.close()
            if os.path.exists(outputFile):
                os.rename(outputFile, outputFile + ".BAK")
 

    def run(self):
        logging.info("Start to run")
        self.cleanStagingFiles()
        while not self.isFinished():
            try:
                self.renewEventStagerStatus()
                finished = True
                outputFiles = self.getUnstagedOutputFiles()
                for outputFile in outputFiles:
                    self.processFile(os.path.join(self.__workDir, outputFile))
                outputFiles = self.getUnstagedOutputFiles(".dump.mv")
                for outputFile in outputFiles:
                    newOutputFile = os.path.join(self.__outputDir, os.path.basename(outputFile))
                    localNewOutputFile = os.path.join(self.__workDir, os.path.basename(outputFile)).replace(".dump.mv", ".dump.staged.reported")
                    newOutputFile = newOutputFile.replace(".dump.mv", ".dump.staged.reported")
                    if not os.path.exists(localNewOutputFile):
                        if os.path.exists(newOutputFile):
                            shutil.copyfile(newOutputFile, localNewOutputFile)
                            logging.info("Found finished dump file %s" % localNewOutputFile)
                        else:
                            finished = False

                if self.__canFinish and finished:
                    self.__status = 'finished'
                    self.renewEventStagerStatus()
                time.sleep(60)
            except:
                logging.info(traceback.format_exc())
                #sys.exit(1)
        logging.info("Finished to run")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workDir", action="store", type=str, help='WorkDir')
    parser.add_argument("--setup", action="store", type=str, help='Setup')
    parser.add_argument("--esPath", action="store", type=str, help='esPath')
    parser.add_argument("--token", action="store", type=str, help='token')
    parser.add_argument("--experiment", action="store", type=str, help='experiment')
    parser.add_argument("--userid", action="store", type=str, help='userid')
    parser.add_argument("--sitename", action="store", type=str, help='sitename')
    parser.add_argument("--threads", action="store", type=int, default=10, help='sitename')
    parser.add_argument("--outputDir", action="store", type=str, help='outputDir')
    parser.add_argument("--YodaToOS", action="store_true", default=False)
    parser.add_argument("--isDaemon", action='store_true', default=True)

    args = parser.parse_args()
    try:
        es = MVEventStager(args.workDir, args.setup, args.esPath, args.token, args.experiment, args.userid, args.sitename, threads=args.threads, outputDir=args.outputDir, yodaToOS=args.YodaToOS, isDaemon=args.isDaemon)
        es.run()
    except:
        logging.warning("Run exception")
        logging.error(traceback.format_exc())
        sys.exit(1)
