#

import json
import os
import traceback
from pUtil import httpConnect, tolog
from EventRangesPandaProxy import downloadEventRangesPandaProxy, updateEventRangePandaProxy, updateEventRangesPandaProxy

def downloadEventRanges(jobId, jobsetID, taskID, pandaProxySecretKey=None, numRanges=10, url="https://pandaserver.cern.ch:25443/server/panda"):
    """ Download event ranges from the Event Server """

    try:
        # url should be '%s:%s/server/panda' % (env['pshttpurl'], str(env['psport']))

        if os.environ.has_key('EventRanges') and os.path.exists(os.environ['EventRanges']):
            try:
                with open(os.environ['EventRanges']) as json_file:
                    events = json.load(json_file)
                os.rename(os.environ['EventRanges'], os.environ['EventRanges'] + ".loaded")
                tolog(events)
                return json.dumps(events)
            except:
                tolog('Failed to open event ranges json file: %s' % traceback.format_exc())

        # Return the server response (instruction to AthenaMP)
        # Note: the returned message is a string (of a list of dictionaries). If it needs to be converted back to a list, use json.loads(message)

        tolog("Downloading new event ranges for jobId=%s, taskID=%s and jobsetID=%s" % (jobId, taskID, jobsetID))
                
        if pandaProxySecretKey is not None and pandaProxySecretKey != "" :
            return  downloadEventRangesPandaProxy(jobId, jobsetID, pandaProxySecretKey)

        # message = "[{u'lastEvent': 2, u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-1', u'startEvent': 2, u'GUID':u'74DFB3ED-DAA7-E011-8954-001E4F3D9CB1'}]"

        message = ""

        node = {}
        node['pandaID'] = jobId
        node['jobsetID'] = jobsetID
        node['taskID'] = taskID 
        node['nRanges'] = numRanges

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
    except Exception, e:
        tolog("Failed to download event ranges: %s: %s" % (str(e), traceback.format_exc()))
    return None

def updateEventRange(event_range_id, eventRangeList, jobId, pandaProxySecretKey, status='finished', os_bucket_id=-1, errorCode=None):
    """ Update an list of event ranges on the Event Server """
    # parameter eventRangeList is not used

    try:
        tolog("Updating an event range..")

        if pandaProxySecretKey is not None and pandaProxySecretKey != "" :
            return  updateEventRangePandaProxy(event_range_id, eventRangeList, jobId, pandaProxySecretKey, status, os_bucket_id, errorCode)

        eventrange = {'eventRangeID': event_range_id, 'eventStatus': status}

        if os_bucket_id != -1:
            eventrange['objstoreID'] = os_bucket_id
        if errorCode:
            eventrange['errorCode'] = errorCode

        status, message = updateEventRanges([eventrange])
        if status == 0:
            message = json.loads(message)[0]
            if str(message).lower() == 'true':
                message = ""

        return message
    except:
        tolog("Failed to update event range: %s" % traceback.format_exc())
    return None

def updateEventRanges(event_ranges, pandaProxySecretKey=None, jobId=None, url="https://pandaserver.cern.ch:25443/server/panda", version=0):
    """ Update an event range on the Event Server """
    tolog("Updating event ranges...")

    try:
        if pandaProxySecretKey is not None and pandaProxySecretKey != "" :
            return  updateEventRangesPandaProxy(event_ranges, pandaProxySecretKey, jobId)
        message = ""

        # eventRanges = [{'eventRangeID': '4001396-1800223966-4426028-1-2', 'eventStatus':'running'}, {'eventRangeID': '4001396-1800223966-4426028-2-2','eventStatus':'running'}]

        node={}
        node['eventRanges']=json.dumps(event_ranges)
        if version:
            node['version'] = 1

        # open connection
        ret = httpConnect(node, url, path=os.getcwd(), mode="UPDATEEVENTRANGES")
        # response = json.loads(ret[1])

        status = ret[0]
        if ret[0]: # non-zero return code
            message = "Failed to update event range - error code = %d, error: %s" % (ret[0], ret[1])
        else:
            response = json.loads(json.dumps(ret[1]))
            status = int(response['StatusCode'])
            message = json.dumps(response['Returns'])

        return status, message
    except:
        tolog("Failed to update event ranges: %s" % traceback.format_exc())
    return -1, None
