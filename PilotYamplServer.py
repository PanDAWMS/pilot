import yampl
import signal
from pUtil import tolog

class PilotYamplServer(object):
    """ Yampl server used to send yampl messages from runEvent to AthenaMP """

    def __init__(self, name='PilotYamplServer', socketname='EventService_EventRanges', context='local'):
        """ Constructor, setting initial variables """

        self.srv = None
        self.receivedMessage = ""
        self.sendMessage = ""

        # Default signals
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        # Create the server socket
        try:
            self.srv = yampl.ServerSocket(socketname, context)
        except Exception, e:
            tolog("!!WARNING!!2222!! Could not create Yampl server socket")
        else:
            tolog("Created a Yampl server socket")

    def alive(self):
        """ Is the server alive? """

        if self.srv:
            return True
        else:
            return False

    def send(self, message):
        """ Send a yampl message """

        if self.alive():
            self.srv.send_raw(message)
        else:
            tolog("!!WARNING!!2221!! Yampl server not available (not created) - cannot send Yampl message")

    def receive(self):
        """ Receive a yampl message """

        if self.alive():
            size, buf = self.srv.try_recv_raw()
        else:
            tolog("!!WARNING!!2221!! Yampl server not available (not created) - cannot receive any Yampl messages")
            buf = ""
            size = 0

        return size, buf
