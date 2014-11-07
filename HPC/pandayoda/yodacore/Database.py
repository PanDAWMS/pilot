import os
import json
import sqlite3
import datetime


# database class
class Backend:
  
    # constructor
    def __init__(self, workingDir):
        self.workingDir = workingDir
        # database file name
        self.dsFileName = os.path.join(self.workingDir, './events_sqlite.db')
        self.dsFileName_backup = os.path.join(self.workingDir, './events_sqlite_backup.db')
        # timestamp when dumping updates 
        self.dumpedTime = None
        self.conn = None
        self.conn_backup = None
        self.cur = None
        self.cur_backup = None

    def createEventTable(self):
        # delete file just in case
        try:
            os.remove(self.dsFileName)
            os.remove(self.dsFileName_backup)
        except:
            pass
        # make connection
        self.conn = sqlite3.connect(self.dsFileName)
        self.conn_backup = sqlite3.connect(self.dsFileName_backup)
        # make cursor
        self.cur = self.conn.cursor()
        self.cur_backup = self.conn_backup.cursor()
        #self.cur.execute('''PRAGMA journal_mode = OFF''')
        #self.cur_backup.execute('''PRAGMA journal_mode = OFF''')

        # make event table
        sqlM  = "CREATE TABLE JEDI_Events("
        sqlM += "eventRangeID text,"
        sqlM += "startEvent integer,"
        sqlM += "lastEvent integer,"
        sqlM += "LFN text,"
        sqlM += "GUID text,"
        sqlM += "scope text,"
        sqlM += "status text,"
        sqlM += "todump integer,"
        sqlM += "output text,"
        sqlM  = sqlM[:-1]
        sqlM += ")"
        self.cur.execute(sqlM)
        self.conn.commit()
        self.cur_backup.execute(sqlM)
        self.conn_backup.commit()

    # setup table
    def setupEventTable(self,job,eventRangeList):
        # delete file just in case
        try:
            os.remove(self.dsFileName)
            os.remove(self.dsFileName_backup)
        except:
            pass
        # make connection
        self.conn = sqlite3.connect(self.dsFileName)
        # make cursor
        self.cur = self.conn.cursor()
        self.cur.execute('''PRAGMA journal_mode = OFF''')

        self.conn_backup = sqlite3.connect(self.dsFileName_backup)
        self.cur_backup = self.conn_backup.cursor()
        self.cur_backup.execute('''PRAGMA journal_mode = OFF''')

        # make event table
        sqlM  = "CREATE TABLE JEDI_Events("
        sqlM += "eventRangeID text,"
        sqlM += "startEvent integer,"
        sqlM += "lastEvent integer,"
        sqlM += "LFN text,"
        sqlM += "GUID text,"
        sqlM += "scope text,"
        sqlM += "status text,"
        sqlM += "todump integer,"
        sqlM += "output text,"
        sqlM  = sqlM[:-1]
        sqlM += ")"
        self.cur.execute(sqlM)
        self.cur_backup.execute(sqlM)

        # insert event ranges
        sqlI  = "INSERT INTO JEDI_Events ("
        sqlI += "eventRangeID,"
        sqlI += "startEvent,"
        sqlI += "lastEvent,"
        sqlI += "LFN,"
        sqlI += "GUID,"
        sqlI += "scope,"
        sqlI += "status,"
        sqlI += "todump,"
        sqlI  = sqlI[:-1]
        sqlI += ") "
        sqlI += "VALUES("
        sqlI += ":eventRangeID,"
        sqlI += ":startEvent,"
        sqlI += ":lastEvent,"
        sqlI += ":LFN,"
        sqlI += ":GUID,"
        sqlI += ":scope,"
        sqlI += ":status,"
        sqlI += ":todump,"
        sqlI  = sqlI[:-1]
        sqlI += ")"
        for tmpDict in eventRangeList:
            tmpDict['status'] = 'ready'
            tmpDict['todump'] = 0
            self.cur.execute(sqlI,tmpDict)
            self.cur_backup.execute(sqlI,tmpDict)
        self.conn.commit()
        self.conn_backup.commit()
        # return
        return


    def insertEventRanges(self, eventRanges):
        # make connection
        self.conn = sqlite3.connect(self.dsFileName)
        # make cursor
        self.cur = self.conn.cursor()

        self.conn_backup = sqlite3.connect(self.dsFileName_backup)
        self.cur_backup = self.conn_backup.cursor()

        # insert event ranges
        sqlI  = "INSERT INTO JEDI_Events ("
        sqlI += "eventRangeID,"
        sqlI += "startEvent,"
        sqlI += "lastEvent,"
        sqlI += "LFN,"
        sqlI += "GUID,"
        sqlI += "scope,"
        sqlI += "status,"
        sqlI += "todump,"
        sqlI  = sqlI[:-1]
        sqlI += ") "
        sqlI += "VALUES("
        sqlI += ":eventRangeID,"
        sqlI += ":startEvent,"
        sqlI += ":lastEvent,"
        sqlI += ":LFN,"
        sqlI += ":GUID,"
        sqlI += ":scope,"
        sqlI += ":status,"
        sqlI += ":todump,"
        sqlI  = sqlI[:-1]
        sqlI += ")"
        for tmpDict in eventRanges:
            tmpDict['status'] = 'ready'
            tmpDict['todump'] = 0
            self.cur.execute(sqlI,tmpDict)
            self.cur_backup.execute(sqlI,tmpDict)
        self.conn.commit()
        self.conn_backup.commit()


    # get event ranges
    def getEventRanges(self,nRanges):
        # make connection
        self.conn = sqlite3.connect(self.dsFileName)
        # make cursor
        self.cur = self.conn.cursor()

        # sql to get event range
        sqlI  = "SELECT "
        sqlI += "eventRangeID,"
        sqlI += "startEvent,"
        sqlI += "lastEvent,"
        sqlI += "LFN,"
        sqlI += "GUID,"
        sqlI += "scope,"
        sqlI  = sqlI[:-1]
        sqlI += " FROM JEDI_Events WHERE status=:status ORDER BY rowid "
        # sql to update event range
        sqlU  = "UPDATE JEDI_Events SET status=:status WHERE eventRangeID=:eventRangeID "
        # get event ranges
        varMap = {}
        varMap['status'] = 'ready'
        self.cur.execute(sqlI,varMap)
        retRanges = []
        for i in range(nRanges):
            # get one row
            tmpRet = self.cur.fetchone()
            if tmpRet == None:
                break
            eventRangeID,startEvent,lastEvent,LFN,GUID,scope = tmpRet
            tmpDict = {}
            tmpDict['eventRangeID'] = eventRangeID
            tmpDict['startEvent']   = startEvent
            tmpDict['lastEvent']    = lastEvent
            tmpDict['LFN']          = LFN
            tmpDict['GUID']         = GUID
            tmpDict['scope']        = scope
            ## update status
            #varMap = {}
            #varMap['eventRangeID'] = eventRangeID
            #varMap['status'] = 'running'
            #self.cur.execute(sqlU,varMap)
            #should not update cursor here. otherwise next fetchone will return None
            # append
            retRanges.append(tmpDict)
        for retRange in retRanges:
            varMap = {}
            varMap['eventRangeID'] = retRange['eventRangeID']
            varMap['status'] = 'running'
            self.cur.execute(sqlU,varMap)
        self.conn.commit()
        # return list
        #return json.dumps(retRanges)
        return retRanges



    # update event range
    def updateEventRange(self,eventRangeID,eventStatus, output):
        # make connection
        self.conn = sqlite3.connect(self.dsFileName)
        # make cursor
        self.cur = self.conn.cursor()

        sql = "UPDATE JEDI_Events SET status=:status,todump=:todump, output=:output WHERE eventRangeID=:eventRangeID "
        varMap = {}
        varMap['eventRangeID'] = eventRangeID
        varMap['status']       = eventStatus
        varMap['todump']       = 1
        varMap['output']       = output
        self.cur.execute(sql,varMap)
        self.conn.commit()
        return



    # dump updated records
    def dumpUpdates(self,forceDump=False):
        # make connection
        self.conn = sqlite3.connect(self.dsFileName)
        # make cursor
        self.cur = self.conn.cursor()

        timeNow = datetime.datetime.utcnow()
        # forced or first dump or enough interval
        if forceDump or self.dumpedTime == None or \
                timeNow-self.dumpedTime > datetime.timedelta(seconds=60):
            # sql to get event ranges to be dumped
            sqlG = "SELECT eventRangeID,status,output FROM JEDI_Events WHERE todump=:todump "
            # sql to reset flag
            sqlR = "UPDATE JEDI_Events SET todump=:todump WHERE eventRangeID=:eventRangeID AND status=:status"
            # get event ranges to be dumped
            varMap = {}
            varMap['todump'] = 1
            self.cur.execute(sqlG,varMap)
            # dump
            res = self.cur.fetchall()
            if len(res) > 0:
                outFileName = timeNow.strftime("%Y-%m-%d-%H-%M-%S") + '.dump'
                outFileName = os.path.join(self.workingDir, outFileName)
                outFile = open(outFileName,'w')
                for eventRangeID,status,output in res:
                    outFile.write('{0} {1} {2}\n'.format(eventRangeID,status,output))
                    # reset flag
                    varMap = {}
                    varMap['todump'] = 0
                    varMap['status'] = status
                    varMap['eventRangeID'] = eventRangeID
                    self.cur.execute(sqlR,varMap)
                outFile.close()
            self.conn.commit()
            # update timestamp
            self.dumpedTime = timeNow
        # return
        return
 
