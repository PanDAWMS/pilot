import json
from pandayoda.yodacore import Database

eventRangeList = [
    {'eventRangeID':'1-2-3',
     'startEvent':0,
     'lastEvent':9,
     'LFN':'NTUP_SUSY.01272447._000001.root.2',
     'GUID':'c58cc417-f369-44d8-81b4-72a76c1f2b79',
     'scope':'mc12_8TeV'},
    {'eventRangeID':'4-5-6',
     'startEvent':10,
     'lastEvent':19,
     'LFN':'NTUP_SUSY.01272447._000001.root.2',
     'GUID':'c58cc417-f369-44d8-81b4-72a76c1f2b79',
     'scope':'mc12_8TeV'},
    ]


db = Database.Backend('./')

#db.setupEventTable(None,eventRangeList)
db.createEventTable()
db.insertEventRanges(eventRangeList)

while True:
    tmpListS = db.getEventRanges(1)
    tmpList = json.loads(tmpListS)
    print len(tmpList),"ranges"
    if tmpList == []:
        break
    for tmpItem in tmpList:
        print tmpItem
        print "update",tmpItem["eventRangeID"] 
        db.updateEventRange(tmpItem["eventRangeID"],"finished")
db.dumpUpdates()
