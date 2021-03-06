import zlib
import zmq.green as zmq
import simplejson
import sys, os, datetime, time
 
"""
"  Configuration
"""
__relayEDDN             = 'tcp://eddn-relay.elite-markets.net:9500'
#__relayEDDN             = 'tcp://eddn-gateway.elite-markets.net:9500'
__timeoutEDDN           = 600000 # 10 minuts
#__timeoutEDDN           = 60000 # 1 minut
 
__logJSONFile           = 'eddn_%DATE%.txt'
 
 
 
"""
"  Start
"""
def date(__format):
    d = datetime.datetime.utcnow()
    return d.strftime(__format)
 
def echoLogJSON(__message, __json):
    global __logJSONFile
 
    __logJSONFileParsed = __logJSONFile.replace('%DATE%', str(date('%Y-%m-%d')))
 
    f = open(__logJSONFileParsed, 'a')
    f.write(str(__message) + '\n')
    f.close()
 
    print __json['header']['gatewayTimestamp'] + ', ' + __json['header']['softwareName'] + ' ' + __json['header']['softwareVersion'] + ', ' + __json['header']['uploaderID'] + ', ' + __json['$schemaRef']
    sys.stdout.flush()
 
 
def main():
    context     = zmq.Context()
    subscriber  = context.socket(zmq.SUB)
 
    subscriber.setsockopt(zmq.SUBSCRIBE, "")
    
    while True:
        try:
            subscriber.connect(__relayEDDN)
            print datetime.datetime.utcnow().isoformat() + ',Connect to EDDN'
            sys.stdout.flush()
            
            poller = zmq.Poller()
            poller.register(subscriber, zmq.POLLIN)
 
            while True:
                socks = dict(poller.poll(__timeoutEDDN))
                if socks:
                    if socks.get(subscriber) == zmq.POLLIN:
                        __message   = subscriber.recv(zmq.NOBLOCK)
                        __message   = zlib.decompress(__message)
                        __json      = simplejson.loads(__message)
         
                        echoLogJSON(__message, __json)
                else:
                    print datetime.datetime.utcnow().isoformat() + ',Disconnect from EDDN (After timeout)'
                    sys.stdout.flush()
                    
                    subscriber.disconnect(__relayEDDN)
                    break
 
        except zmq.ZMQError, e:
            print datetime.datetime.utcnow().isoformat() + ',Disconnect from EDDN (After receiving ZMQError)'
            print datetime.datetime.utcnow().isoformat() + ',ZMQSocketException: ' + str(e)
            sys.stdout.flush()
            
            subscriber.disconnect(__relayEDDN)
            time.sleep(10)
 
 
 
if __name__ == '__main__':
    main()
