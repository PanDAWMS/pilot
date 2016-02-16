import cgi
import sys
import json
import time
import urllib
import Queue
import traceback

recvQueue = Queue.Queue()
sendQueue = Queue.Queue()


# class to receive requests
class Receiver:

    # constructor
    def __init__(self, rank=None, nonMPIMode=False):
        if nonMPIMode:
            self.comm = None
            self.stat = None
            self.nRank = 0
            self.totalRanks = 1
            self.selectSource = None
        else:
            from mpi4py import MPI
            self.comm = MPI.COMM_WORLD
            self.stat = MPI.Status()
            self.nRank = self.comm.Get_rank()
            self.totalRanks = self.comm.Get_size()
            self.selectSource = MPI.ANY_SOURCE

        # for message in rank 0
        self.hasMessage = False
        self.recvQueue = recvQueue
        self.sendQueue = sendQueue


    # get rank of itself
    def getRank(self):
        return self.comm.Get_rank() if self.comm else self.nRank


    # receive request
    def receiveRequest(self):
        # wait for a request from any ranks
        self.hasMessage = False
        if self.comm:
            while not self.comm.Iprobe(source=self.selectSource,
                                       status=self.stat):
                if not self.recvQueue.empty():
                    self.hasMessage = True
                    break
                time.sleep(0.0001)
        else:
            while self.recvQueue.empty():
                time.sleep(0.0001)
            self.hasMessage = True

        if self.hasMessage:
            try:
                reqData = self.recvQueue.get()
                self.stat.Set_source(0)
                data = json.loads(reqData)
                return True,data['method'],data['params']
            except:
                errtype,errvalue = sys.exc_info()[:2]
                errMsg = 'failed to got proper request with: %s' % traceback.format_exc()
                return False,errMsg,None
        else:
            try:
                # get the request
                # reqData = self.comm.recv(source=self.selectSource, status=self.stat)
                reqData = self.comm.recv(source=self.stat.Get_source())
                # decode
                data = json.loads(reqData)
                return True,data['method'],data['params']
            except:
                errtype,errvalue = sys.exc_info()[:2]
                errMsg = 'failed to got proper request with: %s' % traceback.format_exc()
                return False,errMsg,None


    # return response 
    def returnResponse(self,rData):
        if self.hasMessage:
            try:
                #data = urllib.urlencode(rData)
                data = json.dumps(rData)
                self.sendQueue.put(data)
                return True,None
            except:
                errtype,errvalue = sys.exc_info()[:2]
                errMsg = 'failed to retrun response with: %s' % traceback.format_exc()
                return False,errMsg,None
        else:
            try:
                #data = urllib.urlencode(rData)
                data = json.dumps(rData)
                self.comm.send(data,dest=self.stat.Get_source())
                return True,None
            except:
                errtype,errvalue = sys.exc_info()[:2]
                errMsg = 'failed to retrun response with: %s' % traceback.format_exc()
                return False,errMsg,None


    # get rank of the requester
    def getRequesterRank(self):
        return self.stat.Get_source() if self.stat else self.nRank

        
    # decrement nRank
    def decrementNumRank(self):
        self.totalRanks -= 1
        

    # check if there is active worker rank
    def activeRanks(self):
        return self.totalRanks > 0


    def sendMessage(self, rData):
        try:
            #data = urllib.urlencode(rData)
            data = json.dumps(rData)
            for i in range(1, self.totalRanks):
                self.comm.send(data,dest=i)
            self.sendQueue.put(data)
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to retrun response with: %s' % traceback.format_exc()
            return False,errMsg,None


    def disconnect(self):
        try:
            self.comm.Disconnect()
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to disconnect with: %s' % traceback.format_exc()
            return False,errMsg


# class to send requests
class Requester:
    
    # constructor
    def __init__(self, rank=None, nonMPIMode=False):
        self.nonMPIMode = nonMPIMode
        if not self.nonMPIMode:
            from mpi4py import MPI
            self.comm = MPI.COMM_WORLD
            self.rank = 0
        else:
            self.comm = None

        # for message in rank 0
        self.hasMessage = False
        self.recvQueue = sendQueue
        self.sendQueue = recvQueue

    # get rank of itself
    def getRank(self):
        if self.nonMPIMode:
            return 0
        return self.comm.Get_rank()


    # send request
    def sendRequest(self,method,params):
        try:
            # encode
            data = {'method':method,
                    'params':params}
            reqData = json.dumps(data)
            if self.getRank() == 0:
                self.sendQueue.put(reqData)
                ansData = self.recvQueue.get(True, timeout=1000)
            else:
                # send a request ro rank0
                self.comm.send(reqData,dest=0)
                # wait for the answer from Rank 0
                #while not self.comm.Iprobe(source=0):
                #    time.sleep(0.001)
                # get the answer
                ansData = self.comm.recv(source=0)
            # decode
            #answer = cgi.parse_qs(ansData)
            answer = json.loads(ansData)
            return True,answer
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to send the request with mode %s: %s' % (self.nonMPIMode, traceback.format_exc())
            return False,errMsg
            
        
    def waitMessage(self):
        try:
            if self.getRank() == 0:
                ansData = self.recvQueue.get(True, timeout=100)
            else:
                # wait for message from Rank 0
                #while not self.comm.Iprobe(source=0):
                #    time.sleep(1)
                # get the answer
                ansData = self.comm.recv(source=0)
            # decode
            #answer = cgi.parse_qs(ansData)
            answer = json.loads(ansData)
            return True,answer
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to receive msg with: %s' % traceback.format_exc()
            return False,errMsg


    def disconnect(self):
        try:
            if self.comm:
                self.comm.Disconnect()
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to disconnect with: %s' % traceback.format_exc()
            return False,errMsg
