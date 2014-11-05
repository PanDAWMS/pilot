import os
import commands
from pUtil import tolog

class ProxyGuard:
    """
    This class is used to hide the grid proxy for the payload
    """

    def __init__(self):
        """ Default init """

        self.proxy = None
        self.read = False
        self.x509_user_proxy = None

    def isRead(self):
        """ return True if proxy has been read to memory """
        return self.read

    def isRestored(self):
        """ return True if proxy has been restored """
        # this function is used by the pilot (the proxy is hidden by runJob)
        if os.path.exists(self.x509_user_proxy):
            return True
        else:
            return False

    def setX509UserProxy(self):
        """ reads and sets the name of the user proxy """

        rc, rs = commands.getstatusoutput("echo $X509_USER_PROXY") 
        if rc != 0:
            tolog("Could not get X509_USER_PROXY: %d, %s" % (rc, rs))
            return False

        if rs == "":
            tolog("X509_USER_PROXY is not set")
            return False

        if not os.path.exists(rs):
            tolog("$X509_USER_PROXY does not exist: %s" % (rs))
            return False

        self.x509_user_proxy = rs
        return True

    def setProxy(self):
        """ reads the grid proxy into memory """

        status = False

        # set the user proxy
        if not self.setX509UserProxy():
            tolog("X509_USER_PROXY not known")
            return status

        # get the proxy
        try:
            f = open(self.x509_user_proxy, 'r')
        except Exception, e:
            tolog("!!WARNING!!7777!! Could not open the proxy file: %s" % str(e))
        else:
            self.proxy = f.read()
            f.close()
            self.read = True
            status = True

        return status

    def removeProxy(self):
        """ removes the grid proxy from disk """

        status = False
        try:
            rc, rs = commands.getstatusoutput("rm %s" % (self.x509_user_proxy))
        except Exception, e:
            tolog("!!WARNING!!7777!! Could not remove proxy from disk: %s" % str(e))
        else:
            if rc == 0:
                tolog("Proxy removed from disk")
                status = True
            else:
                tolog("Could not remove proxy from disk: %d, %s" % (rc, rs))

        return status

    def hideProxy(self):
        """ hides the grid proxy """

        status = False

        # set the proxy (read into memory)
        if self.setProxy():
            tolog("Proxy was successfully read into memory")

            # delete the proxy from disk
            if not self.removeProxy():
                tolog("Hide proxy failed")
            else:
                tolog("Hide proxy succeeded")
                status = True
        else:
            tolog("Hide proxy failed")

        return status

    def putProxy(self):
        """ writes the grid proxy back to disk """

        status = False

        # write the proxy back to disk
        try:
            f = open(self.x509_user_proxy, 'w')
        except Exception, e:
            tolog("!!WARNING!!7777!! Could not open the proxy file: %s" % str(e))
        else:
            print >>f, self.proxy
            f.close()
            status = True
        
        return status

    def restoreMode(self):
        """ restores the file permission of the grid proxy """

        status = False
        try:
            rc, rs = commands.getstatusoutput("chmod 600 %s" % (self.x509_user_proxy))
        except Exception ,e:
            tolog("!!WARNING!!7777!! Exception caught: %s" % str(e))
        else:
            if rc == 0:
                status = True
            else:
                tolog("Could not change permission: %d, %s" % (rc, rs))

        return status

    def restoreProxy(self):
        """ restores the grid proxy """

        status = False

        # restore the proxy file
        if not self.putProxy():
            tolog("Failed to restore proxy")
        else:
            tolog("Proxy restored on disk")
            if not self.restoreMode():
                tolog("Failed to restore the file permission")
            else:
                tolog("Restored file permission")
                status = True

        return status
