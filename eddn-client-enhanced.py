import zlib
import zmq.green as zmq
import simplejson
import sys, os, datetime, time
 
"""
"  Configuration
"""
__relayEDDN             = 'tcp://eddn-relay.elite-markets.net:9500'
__timeoutEDDN           = 60000 # 10 minuts
 
__logJSONFile           = 'eddn_%DATE%.txt'
 
 
 
"""
"  Start
"""
def date(__format):
    d = datetime.datetime.utcnow()
    return d.strftime(__format)
 
def echoLogJSON(__json):
    global __logJSONFile
 
    __logJSONFileParsed = __logJSONFile.replace('%DATE%', str(date('%Y-%m-%d')))
 
    print __json
    sys.stdout.flush()
 
    f = open(__logJSONFileParsed, 'a')
    f.write(str(__json) + '\n')
    f.close()
 
 
def main():
    context     = zmq.Context()
    subscriber  = context.socket(zmq.SUB)
 
    subscriber.setsockopt(zmq.SUBSCRIBE, "")
    subscriber.setsockopt(zmq.RCVTIMEO, __timeoutEDDN)
 
    while True:
        try:
            subscriber.connect(__relayEDDN)
            print 'Connect to EDDN'
 
            while True:
                __message   = subscriber.recv()
 
                if __message == False:
                    subscriber.disconnect(__relayEDDN)
                    print 'Disconnect from EDDN'
                    break
 
                __message   = zlib.decompress(__message)
                __json      = simplejson.loads(__message)
 
                echoLogJSON(__message)
 
        except zmq.ZMQError, e:
            print 'ZMQSocketException: ' + str(e)
            subscriber.disconnect(__relayEDDN)
            print 'Disconnect from EDDN'
            time.sleep(10)
 
 
 
if __name__ == '__main__':
    main()
