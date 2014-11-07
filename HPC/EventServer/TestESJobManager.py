import logging
logging.basicConfig(level=logging.DEBUG)


import time
from Job import Job
from EventServerJobManager import EventServerJobManager

#eventRange = str({'eventRangeID': '4078809-2254021619-92559356-1001-3', 'LFN': 'EVNT.01461041._000001.pool.root.1', 'lastEvent': 1001, 'startEvent': 1001, 'GUID': 'BABC9918-743B-C742-9049-FC3DCC8DD774'})
#eventRange = str([{u'eventRangeID': u'4078809-2254021619-92559356-1001-3', u'LFN': u'EVNT.01461041._000001.pool.root.1', u'lastEvent': 1001, u'startEvent': 1001, u'GUID': u'BABC9918-743B-C742-9049-FC3DCC8DD774'}])
#eventRange = str([{u'eventRangeID': u'4098297-2257969310-105151393-1001-3', u'LFN': u'EVNT.01461041._000001.pool.root.1', u'lastEvent': 1001, u'startEvent': 1001, u'GUID': u'BABC9918-743B-C742-9049-FC3DCC8DD774'}])
#eventRange = "No more events"
eventRange = {u'eventRangeID': u'4078809-2254021619-92559356-1001-3', u'LFN': u'EVNT.01461041._000001.pool.root.1', u'lastEvent': 1001, u'startEvent': 1001, u'GUID': u'BABC9918-743B-C742-9049-FC3DCC8DD774'}


job = Job()
test = EventServerJobManager()
try:
    test.initMessageThread(socketname='EventService_EventRanges', context='local')
    test.initTokenExtractorProcess(job.getTokenExtractorCmd())
    test.initAthenaMPProcess(job.getAthenaMPCommand())
except Exception, e:
    print "Failed to init EventServerJobManager: %s" % str(e)
    test.terminate()

i = 0
output_file = open("output_file", "w")
while not test.isDead():
    
    if test.isNeedMoreEvents():
        i += 1
        if i == 3:
            test.insertEventRange("No more events")
        else:
            test.insertEventRange(eventRange)
        
    test.poll()
    output = test.getOutput()
    if output is not None:
        print output
        output_file.write(output+"\n")
    time.sleep(1)

output = test.getOutput()
while output:
   print output
   output_file.write(output+"\n")
   output = test.getOutput()
output_file.close()
eventRangesStatus = test.getEventRangesStatus()
print eventRangesStatus

