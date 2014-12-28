import cgi
import sys
import json
import time
import urllib
from mpi4py import MPI



# class to receive requests
class Receiver:

    # constructor
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.stat = MPI.Status()
        self.nRank = self.comm.Get_size()
        self.totalRanks = self.nRank


    # get rank of itself
    def getRank(self):
        return self.comm.Get_rank()


    # receive request
    def receiveRequest(self):
        # wait for a request from any ranks
        while not self.comm.Iprobe(source=MPI.ANY_SOURCE,
                                   status=self.stat):
            time.sleep(1)
        try:
            # get the request
            reqData = self.comm.recv(source=self.stat.Get_source())
            # decode
            data = json.loads(reqData)
            return True,data['method'],data['params']
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to got proper request with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg,None


    # return response 
    def returnResponse(self,rData):
        try:
            #data = urllib.urlencode(rData)
            data = json.dumps(rData)
            self.comm.send(data,dest=self.stat.Get_source())
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to retrun response with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg,None


    # get rank of the requester
    def getRequesterRank(self):
        return self.stat.Get_source()

        
    # decrement nRank
    def decrementNumRank(self):
        self.nRank -= 1
        

    # check if there is active worker rank
    def activeRanks(self):
        return self.nRank > 1


    def sendMessage(self, rData):
        try:
            #data = urllib.urlencode(rData)
            data = json.dumps(rData)
            for i in range(1, self.totalRanks):
                self.comm.send(data,dest=i)
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to retrun response with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg,None


# class to send requests
class Requester:
    
    # constructor
    def __init__(self):
        self.comm = MPI.COMM_WORLD


    # get rank of itself
    def getRank(self):
        return self.comm.Get_rank()


    # send request
    def sendRequest(self,method,params):
        try:
            # encode
            data = {'method':method,
                    'params':params}
            reqData = json.dumps(data)
            # send a request ro rank0
            self.comm.send(reqData,dest=0)
            # wait for the answer from Rank 0
            while not self.comm.Iprobe(source=0):
                time.sleep(1)
            # get the answer
            ansData = self.comm.recv()
            # decode
            #answer = cgi.parse_qs(ansData)
            answer = json.loads(ansData)
            return True,answer
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to send the request with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg
            
        
    def waitMessage(self):
        try:
            # wait for message from Rank 0
            while not self.comm.Iprobe(source=0):
                time.sleep(1)
            # get the answer
            ansData = self.comm.recv()
            # decode
            #answer = cgi.parse_qs(ansData)
            answer = json.loads(ansData)
            return True,answer
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to send the request with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg

