import yampl
import signal
import threading
from time import sleep
from pUtil import tolog

class PilotYamplServer(threading.Thread):
    """ Yampl server used to send yampl messages from runEvent to AthenaMP """

    def __init__(self, name='PilotYamplServer'):
        """ Constructor, setting initial variables """

        self.srv = None
        self.receivedMessage = ""
        self.sendMessage = ""

        #
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        # Create the server socket
        try:
            self.srv = yampl.ServerSocket("service", "local_pipe")
        except Exception, e:
            tolog("!!WARNING!!2222!! Could not create Yampl server socket")
        else:
            tolog("Created a Yampl server socket")

            self._stopevent = threading.Event() 
            threading.Thread.__init__(self, name=name)

    def run(self):
        """ Main control loop """

        tolog("%s starts" % str((self.getName(),)))
        while not self._stopevent.isSet():
            # Will wait until a message has been received
            self.receivedMessage = self.receive()

    def join(self, timeout=None):
        """ Stop the thread and wait for it to end. """

        tolog("Join called on thread %s" % self.getName())
        self._stopevent.set() # signal thread to stop
        threading.Thread.join(self, timeout) # wait until the thread terminates or timeout occurs

    def send(self, message):
        """ Send a yampl message """

        self.srv.send(message)

    def receive(self):
        """ Receive a yampl message """

        size, buf = self.srv.recv()
        return buf

    def getReceivedMessage():
        """ """

        return self.receivedMessage

# yamplthread = PilotYamplServer()
# yamplthread.start()
