#

import json
import os
import cgi
from pUtil import httpConnect, tolog
from Configuration import Configuration
try:
    import requests
except  ImportError:
    tolog("pp: unable to import module 'requests', which is necessary if panda proxy is used")

def envPandaProxyURL():
    """ Compose "panda proxy URL" from environment variables in Configuration module
        pilot options -O <panda_proxy_url> and -P <panda_proxy_port> which are accessible using env['panda_proxy_url'] and env['panda_proxy_port'] in the code
        the format is the same to panda proxy format: hostname as url and port
        No panda proxy protocol supported yet.
    """
    env=Configuration()
    pandaProxyURL= 'http://%s:%s/server/panda' % (env['panda_proxy_url'], str(env['panda_proxy_port'])) 

    tolog("pp: composed from command line (env) pandaProxyURL=%s" % pandaProxyURL)
    
    # quickfix: use default
    pandaProxyURL = "http://aipanda084.cern.ch:25064/proxy/panda"
    
    #  https:// is also supported, but now it can not be used, because it requires client authorization (not suits for BOINC)
    # pandaProxyURL = 'https://aipanda084.cern.ch:25128/proxy/panda'
    
    tolog("pp: pandaProxyURL=%s" % pandaProxyURL)
    return pandaProxyURL

def envPandaServerURL():
    """ Compose "panda server URL" from environment variables in Configuration module
    """
    env=Configuration()
    pandaServerURL = 'https://%s:%s/server/panda' % (env['pshttpurl'], str(env['psport'])) 
    tolog("pp: pandaServerURL=%s" % pandaServerURL)
    return pandaServerURL


def downloadEventRangesPandaProxy(jobId, jobsetId, pandaProxySecretKey):
    """ Download event ranges from the Panda proxy """

    tolog("pp: start to download new event ranges for jobId=%s, jobsetId=%s" % (jobId, jobsetId))

            
    data = {
        'pandaID' : jobId, 
        'jobsetID':jobsetId,
        'secretKey':pandaProxySecretKey,
        'baseURL':envPandaServerURL()
        }
    postURL = envPandaProxyURL()+'/getEventRanges'
    tolog( "pp: POST url: '%s', request data=%s" % (postURL,data))
    res = requests.post(postURL, data)
    if res.status_code != 200:
        tolog('pp: downloadEventRangesPandaProxy request failed, ret status: %s' % res.status_code )
        message = "Failed to download event range - error code = %d" % (res.status_code)
    else:
        tolog("pp: res=%s" % res)
        tolog("pp: res.text.encode('ascii') = %s" % res.text.encode('ascii'))
        tmpDict =  cgi.parse_qs(res.text.encode('ascii'))
        tolog("pp: tmpDict = %s" % tmpDict)
        #resCgiParsed = {'eventRanges': ['[{\"eventRangeID\": \"10204500-3123350504-7783643170-1-10\", \"LFN\": \"EVNT.01416937._000001.pool.root.1\", \"lastEvent\": 1, \"startEvent\": 1, \"scope\": \"valid1\", \"GUID\": \"1141217E-433C-3E47-8898-615B968DA5E5\"}, {\"eventRangeID\": \"10204500-3123350504-7783643170-2-10\", \"LFN\": \"EVNT.01416937._000001.pool.root.1\", \"lastEvent\": 2, \"startEvent\": 2, \"scope\": \"valid1\", \"GUID\": \"1141217E-433C-3E47-8898-615B968DA5E5\"}, {\"eventRangeID\": \"10204500-3123350504-7783643170-3-10\", \"LFN\": \"EVNT.01416937._000001.pool.root.1\", \"lastEvent\": 3, \"startEvent\": 3, \"scope\": \"valid1\", \"GUID\": \"1141217E-433C-3E47-8898-615B968DA5E5\"}, {\"eventRangeID\": \"10204500-3123350504-7783643170-4-10\", \"LFN\": \"EVNT.01416937._000001.pool.root.1\", \"lastEvent\": 4, \"startEvent\": 4, \"scope\": \"valid1\", \"GUID\": \"1141217E-433C-3E47-8898-615B968DA5E5\"}]'], 'StatusCode': ['0']}
        retStatus = int(tmpDict['StatusCode'][0])
        if  retStatus== 0:
            message = tmpDict['eventRanges'][0]
            tolog('pp: downloadEventRangesPandaProxy ranges: %s' % message)
        else:
            message = "Failed to download event range from panda proxy - error code = %d" % statusCode
            tolog('pp: download event range: panda proxy returned error: %s' % statusCode)
    if message == "" or message == "[]":
        tolog('pp: no more events' )
        message = "No more events"
    tolog("pp: return =>message<= : =>%s<= type: %s" % (message, type(message)))
    return message

    
def updateEventRangePandaProxy(event_range_id, eventRangeList, jobId, pandaProxySecretKey, status, os_bucket_id, errorCode):
    """ Wrapper around updateEventRangePandaProxy_normalized, 
        wrapper is used :
        1) to conform with upper levels which expect only message as return value
        2) to make code clear by removing unused parameters:
        - eventRangeList is not used even in the original updateEventRanges (from EventRanges.py)
        #- (os_bucket_id, errorCode) are not used, as there is no corresponding parameters ('objstoreID', 'errorCode') in the request to pandaProxy 
        # changed  - can be used, after update of pandaProxy API
    """
    retStatus, message = updateEventRangePandaProxy_normalized(event_range_id,  jobId, pandaProxySecretKey, status, os_bucket_id)
    return message
    
    
def updateEventRangePandaProxy_normalized(eventRangeId, jobId, pandaProxySecretKey, eventRangeStatus, objectStoreId):
    """ Update event range
        returns (status, message) -- string type both
        
        on success: status="0" message="" 
        message may contain kill instructions to stop athinaMP: "tobekilled" "softkill"
    """
    data = {
        'eventRangeID':eventRangeId,
        'eventStatus': eventRangeStatus,
        'baseURL': envPandaServerURL(),
        'secretKey': pandaProxySecretKey,
        'objstoreID': objectStoreId,
        }
    postURL = envPandaProxyURL()+'/updateEventRange'
    tolog( "pp: POST url: '%s', request data=%s" % (postURL,data))
    res = requests.post(postURL, data)
    if res.status_code != 200:
        tolog('pp: update of event range failed, ret status: %s' % res.status_code )
        message = "Failed to update event range - error code = %d" % (res.status_code)
        return "112233", message  # can not find analogue status number in pUtil library, believe that this status number is just for debugging 
    else:
        tolog("pp: res=%s" % res)
        tolog("pp: res.text.encode('ascii') = %s" % res.text.encode('ascii'))
        tmpDict = cgi.parse_qs(res.text.encode('ascii'))
        # example: tmpDict = {'Command': ['{\"3128686946\": null}'], 'StatusCode': ['0']}
        tolog("pp: tmpDict = %s" % tmpDict)
        try:
            retStatus = tmpDict['StatusCode'][0]
        except:
            tolog("pp: Could not intepret panda response")
            return "112233", "Failed to update event range - strange response from Panda"

        if  int(retStatus) == 0:
            if jobId:
                message = json.loads(tmpDict['Command'][0])["%s"%jobId]
            else:
                littleDict = json.loads(tmpDict['Command'][0]) # {\"3128686946\": null}
                message = littleDict [littleDict.keys()[0]]
            tolog('pp: the event range was successfully updated, returned: \"%s\"' % message)
        else:
            # where is error string? orig:message = "Failed to update event range - error code = %d, error: %s"  % (ret[0], ret[1])
            message = "Failed to update event range - error code = %d" % retStatus
            tolog('pp:  update event range failed, returned statusCode: %s' % retStatus)
        return retStatus, message
    return retStatus, message

    
def updateEventRangesPandaProxy(event_ranges, pandaProxySecretKey, jobId):
    """ Update an event range on the Event Server through Panda Proxy
    """
    tolog("Updating event ranges..")

    for range in event_ranges:
        eventRangeId = range['eventRangeID']
        eventRangeStatus = range['eventStatus']
        retStatus, message =  updateEventRangePandaProxy_normalized(eventRangeId, jobId, pandaProxySecretKey, eventRangeStatus)
        if message != "":
            return retStatus, message
    return retStatus, message		
   
