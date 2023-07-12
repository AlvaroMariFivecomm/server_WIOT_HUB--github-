import threading
import time
import mysqlGet
from wmbusSender import *
from datetime import datetime, timedelta


class timeChecker(threading.Thread):
    def __init__(self, time):
        threading.Thread.__init__(self)
        self.daemon = True
        self.time = time

    def run(self):
        while True:
            devices = mysqlGet.getPendingDevices(self.time)
            if len(devices) > 0:
                for device in devices:
                    sender = wmbusSender(device)
                    sender.send()
            print("sleep")
            time.sleep(self.time)
