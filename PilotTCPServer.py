from SocketServer import TCPServer
import threading
import random
import socket
from pUtil import tolog

class PilotTCPServer(threading.Thread):
    """ TCP server used to send TCP messages from runJob to pilot """

    def __init__(self, handler, name='PilotTCPServer'):
        """ constructor, setting initial variables """

        # find an available port to create the TCPServer
        n = 0
        while n < 20: # try 20 times before giving up, which means run 20 pilots on one machine simultaneously :)
            n += 1
            self.port = random.randrange(1, 800, 1) + 8888
            try:
                self.srv = TCPServer(('localhost',self.port), handler)
            except socket.error, e:
                tolog("WARNING: Can not create TCP server on port %d, re-try... : %s" % (self.port, str(e)))
            else:
                # tolog("Using port %d" % self.port)
                break

        if n >= 20: # raise some exception later ??
            self.srv = None
            self.port = None
            
        self._stopevent = threading.Event() 
        threading.Thread.__init__(self, name=name)

    def run(self):
        """ main control loop """
        tolog("%s starts" % str((self.getName( ),)))

        while not self._stopevent.isSet():
            self.srv.handle_request()
                
    def join(self, timeout=None):
        """ Stop the thread and wait for it to end. """
        tolog("join called on thread %s" % self.getName())
        self._stopevent.set() # signal thread to stop
        self.srv.socket.shutdown(2) # shutdown the connection in two-ways
        threading.Thread.join(self, timeout) # wait until the thread terminates or timeout occurs
