import logging
import inspect
import time
import sys
import os

loggerMap = {}
logging.basicConfig(level=logging.DEBUG)

class MyFormatter(logging.Formatter):

    def __init__(self, fmt="%(levelno)s: %(msg)s"):

        logging.Formatter.__init__(self, fmt)

        self.info_fmt = '%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s: %(message)s'
        self.debug_fmt = '%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
        self.warning_fmt = '%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
        self.error_fmt = '%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
        self.critical_fmt = '%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'

    def format(self, record):

        # Save the original format configured by the user
        # when the logger formatter was instantiated
        format_orig = self._fmt

        # Replace the original format with one customized by logging level
        if record.levelno == logging.DEBUG:
            self._fmt = self.debug_fmt

        elif record.levelno == logging.INFO:
            self._fmt = self.info_fmt

        elif record.levelno == logging.WARNING:
            self._fmt = self.warning_fmt

        elif record.levelno == logging.ERROR:
            self._fmt = self.error_fmt

        elif record.levelno == logging.CRITICAL:
            self._fmt = self.critical_fmt

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user
        self._fmt = format_orig

        return result

class Logger:

    sh1 = None
    sh2 = None

    def __init__(self, filename=None):

        # get logger name
        print inspect.stack()
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[2])
        if mod == None or mod.__name__ == '__main__':
            modName = 'main'
        else:
            modName = '.'.join(mod.__name__.split('.')[-2:])
        global loggerMap
        if modName in loggerMap:
            # use existing logger
            self.log = loggerMap[modName]
        else:
            # make handler
            fmt = MyFormatter()
            fmt.converter = time.gmtime # to convert timestamps to UTC
#            if filename:
#                sh = logging.FileHandler(filename, mode='a')
#            else:
#                sh = logging.StreamHandler(sys.stdout)
            self.sh1 = logging.FileHandler(filename, mode='a')
            self.sh2 = logging.StreamHandler(sys.stdout)
            self.sh1.setLevel(logging.DEBUG)
            self.sh1.setFormatter(fmt)
            self.sh2.setLevel(logging.DEBUG)
            self.sh2.setFormatter(fmt)

            # make logger
            self.log = logging.getLogger(modName)
            self.log.propagate = False
            self.log.addHandler(self.sh1)
            self.log.addHandler(self.sh2)
            loggerMap[modName] = self.log

    def info(self, msg):
        self.log.info(msg)
        #self.sh1.flush()
        #self.sh2.flush()

    def debug(self, msg):
        self.log.debug(msg)

    def warning(self, msg):
        self.log.warning(msg)

    def error(self, msg):
        self.log.error(msg)

    def critical(self, msg):
        self.log.critical(msg)

    
