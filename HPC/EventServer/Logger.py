import logging
import inspect

loggerMap = {}

class Logger:
    def __init__(self, filename="log.txt", level=logging.DEBUG):
        # get logger name
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])
        if mod == None or mod.__name__ == '__main__':
            modName = 'main'
        else:
            modName = '.'.join(mod.__name__.split('.')[-2:])
        global loggerMap
        # if modName in loggerMap:
        if filename in loggerMap:
            # use existing logger
            # self.log = loggerMap[modName]
            self.log = loggerMap[filename]
        else:
            # make handler
            """
            fmt = logging.Formatter('%(asctime)s %(name)s: %(levelname)s  %(message)s')
            for handler in logging.root.handlers:
                handler.setFormatter(fmt)

            self.log = logging.getLogger(modName)
            self.log.propagate = False
            for handler in logging.root.handlers:
                self.log.addHandler(handler)
            """
            self.log = logging.getLogger(filename)
            fmt = logging.Formatter('%(asctime)s %(name)s: %(levelname)s  %(message)s')
            fileHandler = logging.FileHandler(filename, mode='a')
            fileHandler.setFormatter(fmt)
            streamHandler = logging.StreamHandler()
            streamHandler.setFormatter(fmt)
            self.log.setLevel(level)
            self.log.addHandler(fileHandler)
            # self.log.addHandler(streamHandler) 

            # loggerMap[modName] = self.log
            loggerMap[filename] = self.log


    def info(self,msg):
        self.log.info(msg)

    def debug(self,msg):
        self.log.debug(msg)

    def warning(self,msg):
        self.log.warning(msg)

    def error(self,msg):
        self.log.error(msg)

    
