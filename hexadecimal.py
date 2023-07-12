import time
import os
import threading
# from mysql.connector.errors import Error
from datetime import datetime, timedelta
from paho.mqtt import client as mqtt_client
import jsonSender
import battery as bat
import mysqlGet
import mysqlSet
from timeChecker import *
import mqttConect
import aesDecoder
from jsonFormat import jsonFormat
import replyLite
import crcmod
from binascii import unhexlify
import API

ids = mysqlGet.getAllGreyList()
for id in ids:
    gl = mysqlGet.getGreyListSensorProperties("id", id)
    sensor_id = gl["sensor_id"]
    print(sensor_id)
    try:
        if sensor_id.startswith("0x"):
            sensor_id = sensor_id[2:]
            mysqlSet.updateGreyListProperty(sensor_id, "sensor_id", sensor_id[2:])
        elif len(sensor_id) != 8:
            hexa = hex(int(sensor_id))
            hexa = str(hexa)
            mysqlSet.updateGreyListProperty(sensor_id, "sensor_id", hexa[2:])
        else:
            a = 2
    except Exception:
        a = 1
   