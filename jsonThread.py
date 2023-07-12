import threading
import mysqlGet
from jsonFormat import *

class jsonThread(threading.Thread):
    def __init__(self, imei):
        threading.Thread.__init__(self)
        self.imei = imei

    def run(self):
        jf = jsonFormat()
        jf.send_JSON_timer(self.imei)
        jf.send()


        #threading.Thread.exit()
        #count = mysqlGet.getCountData(self.imei)
        #if count == 1:
        #    jf.send_realTime(self.imei)
        #else:
        #    jf.send_NonRealTime(self.imei, count)

    