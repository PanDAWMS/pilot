#

import os
from pUtil import httpConnect, tolog

def downloadEventRanges(jobId, jobsetID, taskID):
    """ Download event ranges from the Event Server """

    # Return the server response (instruction to AthenaMP)
    # Note: the returned message is a string (of a list of dictionaries). If it needs to be converted back to a list, use json.loads(message)

    tolog("Downloading new event ranges for jobId=%s, taskID=%s and jobsetID=%s" % (jobId, taskID, jobsetID))

    # message = "[{u'lastEvent': 2, u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-1', u'startEvent': 2, u'GUID':u'74DFB3ED-DAA7-E011-8954-001E4F3D9CB1'}]"

    message = ""
#    url = "https://aipanda007.cern.ch:25443/server/panda"
    url = "https://pandaserver.cern.ch:25443/server/panda"
    node = {}
    node['pandaID'] = jobId
    node['jobsetID'] = jobsetID
    node['taskID'] = taskID 

    # open connection
    ret = httpConnect(node, url, path=os.getcwd(), mode="GETEVENTRANGES")
    response = ret[1]

    if ret[0]: # non-zero return code
        message = "Failed to download event range - error code = %d" % (ret[0])
    else:
        message = response['eventRanges']

    if message == "" or message == "[]":
        message = "No more events"

    return message

def updateEventRange(event_range_id, eventRangeList, status='finished'):
    """ Update an event range on the Event Server """

    tolog("Updating an event range..")

    # PanDA dev server: url = "https://aipanda007.cern.ch:25443/server/panda"
    url = "https://pandaserver.cern.ch:25443/server/panda"
    node = {}
    node['eventRangeID'] = event_range_id

    if eventRangeList != []:
        pass
        # node['cpu'] =  eventRangeList[1]
        # node['wall'] = eventRangeList[2]
    node['eventStatus'] = status

    # open connection
    ret = httpConnect(node, url, path=os.getcwd(), mode="UPDATEEVENTRANGE")
    # response = ret[1]

    if ret[0]: # non-zero return code
        message = "Server responded with error code = %d" % (ret[0])
    else:
        message = ""

    return message
