import logging
import inspect
import sys

loggerMap = {}

class Logger:
    def __init__(self, filename=None):
        # get logger name
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])
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
            fmt = logging.Formatter('%(asctime)s %(name)s: %(levelname)s  %(message)s')
            if filename:
                sh = logging.FileHandler(filename, mode='a')
            else:
                sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(logging.DEBUG)
            sh.setFormatter(fmt)
            # make logger
            self.log = logging.getLogger(modName)
            self.log.propagate = False
            self.log.addHandler(sh)
            loggerMap[modName] = self.log


    def info(self,msg):
        self.log.info(msg)

    def debug(self,msg):
        self.log.debug(msg)

    def warning(self,msg):
        self.log.warning(msg)

    def error(self,msg):
        self.log.error(msg)

    
