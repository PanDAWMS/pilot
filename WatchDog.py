import os
import time
import traceback
import pUtil
from PilotErrors import PilotErrors
from Configuration import Configuration

class WatchDog:
    
    def __init__(self):
        self.__env = Configuration()
    
    def pollChildren(self):
        """
        check children processes, collect zombie jobs, and update jobDic status
        """

        error = PilotErrors()

        # tolog("---pollChildren: %s" % str(jobDic))
        for k in self.__env['jobDic'].keys():
            try:
                _id, rc = os.waitpid(self.__env['jobDic'][k][0], os.WNOHANG)
            except OSError, e:
                try:
                    if self.__env['jobDic'][k][1].result[0] == "finished" or self.__env['jobDic'][k][1].result[0] == "failed" or self.__env['jobDic'][k][1].result[0] == "holding":
                        continue
                except:
                    pUtil.tolog("!!FAILED!!1000!! Pilot failed to check the job state: %s" % traceback.format_exc())

                pUtil.tolog("Harmless exception when checking job %s, %s" % (self.__env['jobDic'][k][1].jobId, e))
                if str(e).rstrip() == "[Errno 10] No child processes":
                    pilotErrorDiag = "Exception caught by pilot watchdog: %s" % str(e)
                    pUtil.tolog("!!FAILED!!1000!! Pilot setting state to failed since there are no child processes")
                    pUtil.tolog("!!FAILED!!1000!! %s" % (pilotErrorDiag))
                    self.__env['jobDic'][k][1].result[0] = "failed"
                    self.__env['jobDic'][k][1].currentState = self.__env['jobDic'][k][1].result[0]
                    if self.__env['jobDic'][k][1].result[2] == 0:
                        self.__env['jobDic'][k][1].result[2] = error.ERR_NOCHILDPROCESSES
                    if self.__env['jobDic'][k][1].pilotErrorDiag == "":
                        self.__env['jobDic'][k][1].pilotErrorDiag = pilotErrorDiag
                else:
                    pass
            else:
                if _id: # finished
                    rc = rc%255 # exit code
                    if k == "prod": # production job is done
                        self.__prodJobDone = True
                        pUtil.tolog("Production job is done")
                    if self.__env['jobDic'][k][1].result[0] != "finished" and self.__env['jobDic'][k][1].result[0] != "failed" and self.__env['jobDic'][k][1].result[0] != "holding":
                        if not rc: # rc=0, ok job
                            if not self.__env['jobDic'][k][1].result[1]:
                                self.__env['jobDic'][k][1].result[1] = rc # transExitCode (because pilotExitCode is reported back by child job)
                        else: # rc != 0, failed job
                            self.__env['jobDic'][k][1].result[1] = rc # transExitCode

    def collectZombieJob(self, tn=None):
        """
        collect zombie child processes, tn is the max number of loops, plus 1,
        to avoid infinite looping even if some child proceses really get wedged;
        tn=None means it will keep going till all children zombies collected.        
        """
        time.sleep(1)
        if self.__env['zombieJobList'] and tn > 1:
            pUtil.tolog("--- collectZombieJob: --- %d, %s" % (tn, str(self.__env['zombieJobList'])))
            tn -= 1
            for x in self.__env['zombieJobList']:
                try:
                    pUtil.tolog("Zombie collector trying to kill pid %s" % str(x))
                    _id, rc = os.waitpid(x, os.WNOHANG)
                except OSError,e:
                    pUtil.tolog("Harmless exception when collecting zombie jobs, %s" % str(e))
                    self.__env['zombieJobList'].remove(x)
                else:
                    if _id: # finished
                        self.__env['zombieJobList'].remove(x)
                self.collectZombieJob(tn=tn) # recursion

        if self.__env['zombieJobList'] and not tn: # for the infinite waiting case, we have to
            # use blocked waiting, otherwise it throws
            # RuntimeError: maximum recursion depth exceeded
            for x in self.__env['zombieJobList']:
                try:
                    _id, rc = os.waitpid(x, 0)
                except OSError,e:
                    pUtil.tolog("Harmless exception when collecting zombie jobs, %s" % str(e))
                    self.__env['zombieJobList'].remove(x)
                else:
                    if _id: # finished
                        self.__env['zombieJobList'].remove(x)
                self.collectZombieJob(tn=tn) # recursion

