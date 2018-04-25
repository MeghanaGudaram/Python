#from __future__ import print_function
import struct
import datetime
from time import sleep
from datetime import tzinfo, timedelta
import IdList
import zipfile
import os
import sys
import threading
import logging

class LoggerWriter:
    def __init__(self, level):
        self.level = level

    def write(self, message):
        if message != '\n':
            self.level(message)

    def flush(self):
        self.level(sys.stderr)

class Zone(tzinfo):
    def __init__(self, offset,isdst, name):
        self.offset = offset
        self.isdst = isdst
        self.name = name
    def utcoffset(self, dt):
        return timedelta(hours = self.offset) + self.dst(dt)
    def dst(self, dt):
        return timedelta(hours = 1) if self.isdst else timedelta(0)
    def tzname(self, dt):
        return self.name

def zip_file(location, idList):
    global maxSize
    files = {}
    now = datetime.datetime.now().timetuple()
    hour=now.tm_hour
    zipname='zipfile_'+str(now.tm_year)+"-"+str('{:02d}'.format(now.tm_mon))+"-"+str('{:02d}'.format(now.tm_mday))+"_"+str('{:02d}'.format(hour))+'.zip'
    while(hour==now.tm_hour):
            now = datetime.datetime.now().timetuple()
    if not(os.path.isfile(zipname)):
        zf = zipfile.ZipFile(zipname, mode='w')
        for i in range(len(idList)):
            files[i] = open("file_"+ str(idList[i]) + "_" +str(now.tm_year)+"."+str('{:02d}'.format(now.tm_mon))+"-"+str('{:02d}'.format(now.tm_mday))+"_"+str('{:02d}'.format(hour))+".b","ab+")   
            zf.write(files[i].name, compress_type=zipfile.ZIP_DEFLATED)
            files[i].close()
            os.remove(files[i].name)
        zf.close()
        files={}
        if(maxSize < (sum([os.path.getsize(f) for f in os.listdir(location) if os.path.isfile(f)]) / 1024 / 1024)):
            oldest = sorted(os.listdir(location), key=os.path.getctime)[0]
            print (oldest, maxSize, sum([os.path.getsize(f) for f in os.listdir(location) if os.path.isfile(f)]) / 1024 / 1024)
            os.remove(oldest)
        
def binfile(name, hmi, wait):
    global maxSize
    EST = Zone(-4, False, 'EST')
    b={}
    s =  struct.Struct('9s f')
    now = datetime.datetime.now().timetuple()

    #while(hmi.isConnected()):
    while(hmi==1):
        hour = now.tm_hour
        fo = open("file_"+ str(name) + "_" +str(now.tm_year)+"-"+str('{:02d}'.format(now.tm_mon))+"-"+str('{:02d}'.format(now.tm_mday))+"_"+str('{:02d}'.format(hour))+".b","ab+")   
        while(hour==now.tm_hour):
            nows = datetime.datetime.now(EST).strftime('%Y-%m-%d %H:%M:%S.%f')[14:-3]
            b = 52574.004
            values = (str(nows), b)
            packed_data = s.pack(*values)
            fo.write(packed_data)
            fo.write("\n")
            sleep(wait)
            now = datetime.datetime.now().timetuple()
        fo.close()
           
if __name__ == "__main__":
    location = sys.argv[1]
    now = datetime.datetime.now().timetuple()
    hmi=1
    threads = {}
    global maxSize
    maxSize = 120
    
    if not(os.path.exists(location)):
        os.mkdir(location)
    os.chdir(location)
    LOG_FILENAME = 'example.log'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
    logger = logging.getLogger()
    sys.stdout = LoggerWriter(logger.debug)
    sys.stderr = LoggerWriter(logger.warning)
    print logger.info
    
    zipname='zipfile_'+str(now.tm_year)+"-"+str('{:02d}'.format(now.tm_mon))+"-"+str('{:02d}'.format(now.tm_mday))+"_"+str('{:02d}'.format(now.tm_hour))+'.zip'
    if(os.path.isfile(zipname)):
        with zipfile.ZipFile(zipname, "r") as z:
            z.extractall(location)
            z.close()
            os.remove(zipname)
    logging.info('This message should go to the log file')
    DictFile = IdList.Init()
    idList = DictFile.keys()
    for i in range(len(idList)):
    #    threads[i] = threading.Thread(name=idList[i], target=binfile, args=(idList[i], hmi, DictFile[idList[i]][1]))
        threads[i] = threading.Thread(name=idList[i], target=binfile, args=(idList[i], hmi, DictFile[idList[i]][1]))

    for i in range(len(idList)):
        threads[i].start()
    zipthread = threading.Thread(name="zipthread", target=zip_file, args=(location, idList))
    zipthread.start()
