import argparse
import commands
import json
import logging
import multiprocessing
import os
import subprocess
import sys
import time
import traceback

import pUtil
from ThreadPool import ThreadPool
from objectstoreSiteMover import objectstoreSiteMover
from Mover import getInitialTracingReport

logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG,
                    format='%(asctime)s\t%(process)d\t%(levelname)s\t%(message)s')


class EventStager:
    def __init__(self, workDir, setup, esPath, token, experiment, userid, sitename, outputDir=None, threads=10, isDaemon=False):
        self.__workDir = workDir
        self.__updateEventRangesDir = os.path.join(self.__workDir, 'updateEventRanges')
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
        self.__isDaemon = isDaemon
        self.__startTime = time.time()

        self.__processedJobs = []
        self.__handlingOthers = 0
        self.__otherProcesses = []

        if not os.environ.has_key('PilotHomeDir'):
            os.environ['PilotHomeDir'] = os.getcwd()

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
            if self.__outputDir:
                stageCmd = "MVEventStager.py"
            else:
                stageCmd = "EventStager.py"

            if self.__setup and len(self.__setup.strip()):
                cmd = 'python %s/%s --workDir %s --setup %s --esPath %s --token %s --experiment %s --userid %s --sitename %s --threads %s --outputDir %s --isDaemon 2>&1 1>>%s' % (self.__workDir, stageCmd, self.__workDir, self.__setup, self.__esPath, self.__token, self.__experiment, self.__userid, self.__sitename, self.__threads, self.__outputDir, self.__logFile)
            else:
                cmd = 'python %s/%s --workDir %s --esPath %s --token %s --experiment %s --userid %s --sitename %s --threads %s --outputDir %s --isDaemon 2>&1 1>>%s' % (self.__workDir, stageCmd, self.__workDir, self.__esPath, self.__token, self.__experiment, self.__userid, self.__sitename, self.__threads, self.__outputDir, self.__logFile)
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

    def stageOutEvent(self, output_info):
        filename, jobId, eventRangeID, status, output = output_info

        try:
            if status == 'failed':
                self.__eventRanges_staged[filename].append((jobId, eventRangeID, status, output))
                if eventRangeID not in self.__eventRanges[filename]:
                    logging.warning("stageOutEvent: %s is not in eventRanges" % eventRangeID)
                else:
                    del self.__eventRanges[filename][eventRangeID]
            if status == 'finished':
                if not os.path.exists(output):
                    if eventRangeID in self.__eventRanges[filename]:
                        del self.__eventRanges[filename][eventRangeID]
                        return

                ret_status, pilotErrorDiag, surl, size, checksum, self.arch_type = self.__siteMover.put_data(output, self.__esPath, lfn=os.path.basename(output), report=self.__report, token=self.__token, experiment=self.__experiment)
                if ret_status == 0:
                    try:
                        self.__eventRanges_staged[filename].append((jobId, eventRangeID, status, output))
                
                        if eventRangeID not in self.__eventRanges[filename]:
                            logging.warning("stageOutEvent: %s is not in eventRanges" % eventRangeID)
                        else:
                            del self.__eventRanges[filename][eventRangeID]
                        #logging.info("Remove staged out output file: %s" % output)
                        #os.remove(output)
                    except Exception, e:
                        logging.info("!!WARNING!!2233!! remove ouput file threw an exception: %s" % (e))
                else:
                    logging.info("!!WARNING!!1164!! Failed to upload file to objectstore: %d, %s" % (ret_status, pilotErrorDiag))
                    self.__eventRanges_faileStaged[filename].append((jobId, eventRangeID, status, output))
        except:
            logging.warning(traceback.format_exc())
            self.__eventRanges_faileStaged[filename].append((jobId, eventRangeID, status, output))

    def getUnstagedOutputFiles(self, ext=".dump"):
        outputFiles = []
        all_files = os.listdir(self.__workDir)
        for file in all_files:
            if file.endswith(ext):
                filename = os.path.join(self.__workDir, file)
                outputFiles.append(file)
        if outputFiles:
            logging.info("UnStaged Output files: %s" % outputFiles)
        return outputFiles

    def updateEventRange(self, event_range_id, status='finished'):
        """ Update an event range on the Event Server """
        pUtil.tolog("Updating an event range..")

        message = ""
        # url = "https://aipanda007.cern.ch:25443/server/panda"
        url = "https://pandaserver.cern.ch:25443/server/panda"
        node = {}
        node['eventRangeID'] = event_range_id

        # node['cpu'] =  eventRangeList[1]
        # node['wall'] = eventRangeList[2]
        node['eventStatus'] = status
        # tolog("node = %s" % str(node))

        # open connection
        ret = pUtil.httpConnect(node, url, path=self.__updateEventRangesDir, mode="UPDATEEVENTRANGE")
        # response = ret[1]

        if ret[0]: # non-zero return code
            message = "Failed to update event range - error code = %d" % (ret[0])
        else:
            message = ""

        return ret[0], message

    def updateEventRanges(self, event_ranges):
        """ Update an event range on the Event Server """
        pUtil.tolog("Updating event ranges..")

        message = ""
        url = "https://aipanda007.cern.ch:25443/server/panda"
        # url = "https://pandaserver.cern.ch:25443/server/panda"
        # eventRanges = [{'eventRangeID': '4001396-1800223966-4426028-1-2', 'eventStatus':'running'}, {'eventRangeID': '4001396-1800223966-4426028-2-2','eventStatus':'running'}]

        node={}
        node['eventRanges']=json.dumps(event_ranges)

        # open connection
        ret = pUtil.httpConnect(node, url, path=self.__updateEventRangesDir, mode="UPDATEEVENTRANGES")
        # response = json.loads(ret[1])

        status = ret[0]
        if ret[0]: # non-zero return code
            message = "Failed to update event range - error code = %d, error: " % (ret[0], ret[1])
        else:
            response = json.loads(json.dumps(ret[1]))
            status = int(response['StatusCode'])
            message = json.dumps(response['Returns'])

        return status, message

    def cleanStagingFiles(self):
        self.__eventRanges = {}
        self.__eventRanges_staged = {}
        self.__eventRanges_faileStaged = {}
        all_files = os.listdir(self.__workDir)
        for file in all_files:
            if file.endswith(".dump.staging"):
                filepath = os.path.join(self.__workDir, file)
                os.rename(filepath, filepath.replace(".dump.staging", ".dump"))

    def getEventRanges(self):
        outputFiles = self.getUnstagedOutputFiles()
        for file in outputFiles:
            self.__eventRanges[file] = {}
            self.__eventRanges_staged[file] = []
            self.__eventRanges_faileStaged[file] = []

            filepath = os.path.join(self.__workDir, file)
            handle = open(filepath)
            for line in handle:
                if len(line.strip()) == 0:
                    continue
                line = line.replace("  ", " ")
                jobId, eventRange, status, output = line.split(" ")
                output = output.split(",")[0]
                self.__eventRanges[file][eventRange] = {'retry':0, 'event': (jobId, eventRange, status, output)}
                self.__threadpool.add_task(self.stageOutEvent, (file, jobId, eventRange, status, output))
                if jobId not in self.__processedJobs:
                    self.__processedJobs.append(jobId)
            handle.close()
            os.rename(filepath, filepath + ".staging")

    def checkFailedStagingFiles(self):
        for file in self.__eventRanges_faileStaged:
            while self.__eventRanges_faileStaged[file]:
                jobId, eventRangeID, status, output = self.__eventRanges_faileStaged[file].pop()
                if eventRangeID not in self.__eventRanges[file]:
                    logging.warning("checkFailedStagingFiles: %s is not in eventRanges" % eventRangeID)
                else:
                    if  self.__eventRanges[file][eventRangeID]['retry'] < 3:
                        self.__eventRanges[file][eventRangeID]['retry'] += 1
                        self.__threadpool.add_task(self.stageOutEvent, (file, jobId, eventRangeID, status, output))
                    else:
                        self.__eventRanges_staged[file].append((jobId, eventRangeID, 'failed', output))
                        del self.__eventRanges[file][eventRangeID]

    def checkFinishedStagingFiles(self):
        finishedFiles = []
        for file in self.__eventRanges:
            try:
                if len(self.__eventRanges[file].keys()) == 0:
                    filepath = os.path.join(self.__workDir, file)
                    handle = open(filepath + ".staged.reported", 'w')
                    finishedEventRanges = []
                    for chunk in pUtil.chunks(self.__eventRanges_staged[file], 100):
                        try:
                            eventRanges = []
                            for outputEvents in chunk:
                                jobId, eventRangeID, status, output = outputEvents
                                if eventRangeID not in finishedEventRanges:
                                    finishedEventRanges.append(eventRangeID)
                                    eventRanges.append({"eventRangeID": eventRangeID, "eventStatus": status})

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
                                                    jobId, eventRangeID, status, output = chunk[i]
                                                    logging.info("Remove %s" % output)
                                                    os.remove(output)
                                                    handle.write('{0} {1} {2} {3}\n'.format(jobId, eventRangeID, status, output))

                                            except:
                                                logging.warning("Failed to remove %s: %s" % (output, traceback.format_exc()))
                                except:
                                    logging.warning(traceback.format_exc())
                        except:
                            logging.warning(traceback.format_exc())
                    handle.close()
                    os.rename(filepath + ".staging", filepath + ".BAK")
                    finishedFiles.append(file)
            except:
                logging.warning(traceback.format_exc())
        for file in finishedFiles:
            del self.__eventRanges[file]

    def checkLostEvents(self):
        for file in self.__eventRanges:
            for eventRange in self.__eventRanges[file]:
                jobId, eventRange, status, output = self.__eventRanges[file][eventRange]['event']
                self.__threadpool.add_task(self.stageOutEvent, (file, jobId, eventRange, status, output))

    def handleGfalFile(self, gfalFile):
        try:
            gfalFile = os.path.join(self.__workDir, gfalFile)
            os.rename(gfalFile, gfalFile + "copying")
            handle = open(gfalFile + "copying")
            cmd = handle.read()
            handle.close()
            cmd = cmd.replace(" -t 3600 ", " -t 300 ")
            logging.info("Execute command: %s" % cmd)
            status, output = commands.getstatusoutput(cmd)
            logging.info("Status %s output %s" % (status, output))
            if status == 0:
                os.rename(gfalFile + "copying", gfalFile + "finished")
            else:
                os.rename(gfalFile + "copying", gfalFile)
        except:
            logging.error("handleGfalFile %s" % traceback.format_exc())
        finally:
            self.__handlingOthers -= 1

    def handleS3File(self, s3File):
        try:
            s3File = os.path.join(self.__workDir, s3File)
            os.rename(s3File, s3File + "copying")
            handle = open(s3File + "copying")
            cmd = handle.read()
            handle.close()
            source, destination = cmd.split(" ")
            logging.info("S3 stage out from %s to %s" % (source, destination))
            ret_status, pilotErrorDiag, surl, size, checksum, self.arch_type = self.__siteMover.put_data(source, os.path.dirname(destination), lfn=os.path.basename(destination), report=self.__report, token=self.__token, experiment=self.__experiment, timeout=300)
            logging.info("Status %s output %s" % (ret_status, pilotErrorDiag))
            if ret_status == 0:
                os.rename(s3File + "copying", s3File + "finished")
            else:
                os.rename(s3File + "copying", s3File)
        except:
            logging.error("handleS3File %s" % traceback.format_exc())
        finally:
            self.__handlingOthers -= 1

    def handleOtherFiles(self):
        gfalFiles = self.getUnstagedOutputFiles(".gfalcmd")
        for gfalFile in gfalFiles:
            p = multiprocessing.Process(target=self.handleGfalFile, args=(gfalFile,))
            p.start()
            self.__otherProcesses.append(p)
            self.__handlingOthers += 1

        s3Files = self.getUnstagedOutputFiles(".s3cmd")
        for s3File in s3Files:
            p = multiprocessing.Process(target=self.handleS3File, args=(s3File,))
            p.start()
            self.__otherProcesses.append(p)
            self.__handlingOthers += 1

        termProcesses = []
        for p in self.__otherProcesses:
            if not p.is_alive():
                termProcesses.append(p)
        for p in termProcesses:
            self.__otherProcesses.remove(p)

    def run(self):
        logging.info("Start to run")
        self.cleanStagingFiles()
        timeStart = time.time() - 60
        while not self.isFinished():
            try:
                if (time.time() - timeStart) > 60:
                    self.renewEventStagerStatus()
                    self.getEventRanges()
                    self.checkFailedStagingFiles()
                    self.checkFinishedStagingFiles()
                    if self.__canFinish and len(self.__eventRanges.keys()) == 0:
                        self.__status = 'finished'
                        self.renewEventStagerStatus()
                    if self.__threadpool.is_empty():
                        self.checkLostEvents()
                    timeStart = time.time()
                self.handleOtherFiles()
                time.sleep(30)
                logging.debug("len(eventranges:%s)" % len(self.__eventRanges.keys()))
                #logging.debug("%s" % self.__eventRanges)
                logging.debug("otherProcesses:%s" % len(self.__otherProcesses))
                if len(self.__eventRanges.keys()) == 0 and len(self.__otherProcesses) == 0:
                    break
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
    parser.add_argument("--isDaemon", action='store_true', default=True)

    args = parser.parse_args()
    try:
        es = EventStager(args.workDir, args.setup, args.esPath, args.token, args.experiment, args.userid, args.sitename, threads=args.threads, outputDir=args.outputDir, isDaemon=args.isDaemon)
        es.run()
    except:
        logging.warning("Run exception")
        logging.error(traceback.format_exc())
        sys.exit(1)
