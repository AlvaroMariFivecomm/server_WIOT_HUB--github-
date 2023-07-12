"""
    /* ===================================================== 

    * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved. 

    * File_name scriptRecepcionV33

    * Description:  The main file of the mqtt platform

    * Author:  Alejandro

    * Date:  25-08-21

    * Version:  1.33

    * =================================================== */ 
"""
from datetime import datetime
import exceptions
import timeHandler
import mysqlGet
import mysqlSet
import sigConversor

def loadIni(msg,mqttRecieved):
    try:
        try:
            cid = msg.topic
            cid = cid.replace("t","")
            sigtec = mqttRecieved[1]
            initValue = mqttRecieved[2]
            diameter = mqttRecieved[4] #?
            timeLeak = 24
            volumeLiters = 15
            flowDiameter = 100
            cDate = datetime.utcnow()
            wakeUpDate = timeHandler.stringFromDatetime(cDate)
            cSystemFeatures = mysqlGet.getSystemFeaturesFromId(cid)
            mysqlSet.updateSystemFeatures(diameter, wakeUpDate, \
            timeLeak,flowDiameter,cSystemFeatures)

        except:
            textToWrite = b"Error in handling ini payload\n"
            exceptions.exceptionHandler(textToWrite)

        if int(sigtec) >= 0 and int(sigtec) <= 3:
            G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP, \
            G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio \
            = sigConversor.signal_values(mqttRecieved[2], sigtec)
            # ?
            startupTime = mqttRecieved[3]
            battery = 100
            
    except:
        textToWrite = b"Error in the ini payload\n"
        exceptions.exceptionHandler(textToWrite)
    return  G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP, \
            G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio, \
            startupTime, battery