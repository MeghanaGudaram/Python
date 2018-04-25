import struct
import datetime
from time import sleep
from datetime import tzinfo, timedelta
import IdList
import zipfile
import os
import sys
import ConnectRead

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
    
def encodeBin(hmi, cmd, maxSize, location):
    EST = Zone(-4, False, 'EST')
    os.chdir(location)
    now = datetime.datetime.now().timetuple()
    if (now.tm_year % 4)==0:
        numOfDays = 366
    else:
        numOfDays = 365
    b={}

    DictFile = IdList.Init()
    idList = DictFile.keys()
    fo = {}
    s =  struct.Struct('9s f')
    year = now.tm_year
    while(year == now.tm_year):
        for nday in range(numOfDays):
            day = now.tm_mday
            while(day == now.tm_mday):
                for h in range(24):
                    hour = now.tm_hour
                    for i in range(len(idList)):
                        fo[i] = open("file_"+ str(idList[i]) + "_" +str(now.tm_year)+"-"+str('{:02d}'.format(now.tm_mon))+"-"+str('{:02d}'.format(now.tm_mday))+"_"+str('{:02d}'.format(hour))+".b","ab+")
                    zf = zipfile.ZipFile('zipfile_'+str(now.tm_year)+"-"+str('{:02d}'.format(now.tm_mon))+"-"+str('{:02d}'.format(now.tm_mday))+"_"+str('{:02d}'.format(hour))+'.zip', mode='w')
                    try:
                        #min = now.tm_min
                        #    for(min == now.tm_min):
                        while(hour==now.tm_hour):
                            nows = datetime.datetime.now(EST).strftime('%Y-%m-%d %H:%M:%S.%f')[14:-3]
                            values = ConnectRead.main(hmi, cmd, None, idList)
                            if (values[0]):
                                for i,r in enumerate(values[1]):   
                                    b[i] = r[2]
                                    print r[2]
                                    values = (str(nows), b[i])
                                    packed_data = s.pack(*values)
                                    fo[i].write(packed_data)
                                    fo[i].write("\n")
                            sleep(0.01)
                            now = datetime.datetime.now().timetuple()
                        for i in range(len(idList)):
                            zf.write(fo[i].name, compress_type=zipfile.ZIP_DEFLATED)
                            print fo[i].name 
                            fo[i].close()
                            os.remove(fo[i].name)
                        zf.close()
                        if(maxSize > (sum([os.path.getsize(f) for f in os.listdir(location) if os.path.isfile(f)]) / 1024 / 1024)):
                            oldest = sorted(os.listdir(location), key=os.path.getctime)[0]
                            print oldest, maxSize, sum([os.path.getsize(f) for f in os.listdir(location) if os.path.isfile(f)]) / 1024 / 1024
                            os.remove(oldest)
                        
                    except KeyboardInterrupt:
                        for i in range(len(idList)):
                            zf.write(fo[i].name, compress_type=zipfile.ZIP_DEFLATED) 
                            fo[i].close()
                        zf.close()
