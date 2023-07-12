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
import time
import sys
import math

from datetime import datetime
import exceptions
import mysqlGet
import mysqlSet
import timeHandler
import sigConversor
import mqttConect
import battery as bat

def comandCheck(tmpRemote,command):

    vectorRemote = []

    if len(tmpRemote) != 0:
        for row in tmpRemote: #
            vectorRemote.append(row)
        command = vectorRemote[0][0]
    else:
        command = 'NEW;0;86400 '#SLEEP.
        msg3 = "{"+"'command'"+":'"+command+"'}"
        # msg3 = {"command":command}
    if(command != "none"):
        msg3 = "{"+"'command'"+":'"+command+"'}"
        # msg3 = {'command':command}
    else:
        command = 'NEW;0;86400'#SLEEP
        msg3 = "{"+"'command'"+":'"+command+"'}"

    return msg3

def meterPayload(msg,mqttRecieved,monthPrevious,dayPrevious,hourPrevious):
    try:
        try:
            gateway_id = int(msg.topic.replace("t", ""))

            # battery
            if int(mqttRecieved[6]) < 4001:
                tmpBattery = mqttRecieved[6]
                battery = int(tmpBattery)/100.0
                battery = math.ceil(battery)/10.0
                battery = bat.batteryConversion(battery,gateway_id)
                tmpRemote = mysqlGet.getCommandFromGatewayId(gateway_id)
            else:
                tmpRemote = "{'command':'NEW;0;300'}"
            
            msg3 = comandCheck(tmpRemote)

            time.sleep(0.01)
            client2 = mqttConect.mqttConect("p2")
            result = client2.publish("r"+str(gateway_id), str(msg3)) 

        except:
                print("caution in the publishing meter!")
                print(sys.exc_info())
                textToWrite = b"Error publishing the command\n"
                exceptions.exceptionHandler(textToWrite)

                  

        divisor = mqttRecieved[5]
        accumulated = mqttRecieved[4]
        counterSerialNumber = mqttRecieved[3]
        sigtec = mqttRecieved[1]
                    
        if int(sigtec) == 0 or int(sigtec) == 1 or int(sigtec) == 2 \
        or int(sigtec) == 3:
            firstTime = mqttRecieved[4]
            cTime = datetime.fromtimestamp(int(firstTime))

            stringDate = timeHandler.obtainStringFromDatetime(cTime)
            ### added for json ##
            Ctimestampv = stringDate
            G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP, \
            G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio \
            = sigConversor.signal_values(mqttRecieved[2], sigtec)
            serialNumber = mqttRecieved[3]
            CELL_ID = mqttRecieved[6]
        else:
            print("error")
            
        

        ############################################################
        ##### INSERT TO GATEWAY_HISTORY ############################
        value = "test1"
        name = "pruebaBD"
        if firstTime != 0:
            timestamp = Ctimestampv
        else:
            cTime = datetime.utcnow()
            timestamp = str(cTime.year) + "-" + monthPrevious + "-" \
            + dayPrevious + " " + hourPrevious + ":00:00"
        serialNumber = msg.topic.replace("t", "")
        serialNumber = int(serialNumber)
        GATEWAY_PROPERTIES_id = serialNumber

        mysqlSet.SetGatewayHistory(timestamp,accumulated,CELL_ID,\
        battery,GATEWAY_PROPERTIES_id,value, name, sigtec, G5_RSRP, \
        G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP, G4_RSRQ, G4_SINR,\
        G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio,counterSerialNumber,divisor)
        cDate = datetime.utcnow()
        tempTime = datetime.timestamp(cDate)
        remoteId = mysqlGet.GetMaxIdRemote()
        tFuga = mysqlGet.GetCountFromGatewayHistory(gateway_id)
        nPulsos = mysqlGet.getNumberOfPulsesFromGateway(gateway_id)


        fuga = True
        subdimension = False
        alarmName = False

        cDate = datetime.utcnow()
        stringDate = timeHandler.stringFromDatetime(cDate)
        
        del tmp
        tmp = []
        alarmName = ""
        if fuga == True:
            try:

                idCaracteristicas,tFuga = mysqlGet.getTLeakFromGateway(gateway_id)
                tmp = mysqlGet.getTLeakFromSystem_Features_Id(idCaracteristicas)

                if len(tmp) == 2:   
                    alarmName = tmp[1][0]

                    alarmId = tmp[1][1]
                else:
                    print("else")
                    alarmName = mysqlGet.getAlarmName(1)
                    mysqlSet.InsertAlarm(alarmName,idCaracteristicas)
                    alarmId = mysqlGet.getAlarmId()
                # inserting 0 before the number in case number < 10

                stringDate = timeHandler.obtainStringFromDatetime(cDate)
                mysqlSet.InsertAlarmHisto(alarmName,stringDate,alarmId,tFuga)

            except:
                textToWrite = b"Error inserting the leak alarm in the meter\n"
                exceptions.exceptionHandler(textToWrite)  
        elif subdimension == True:
            alarmName = mysqlGet.getAlarmName(2)
            idCaracteristicas,volumeLiters = mysqlGet.GetVolumeliters(gateway_id)
            alarmId = mysqlGet.GetIdFromSystem_Features_Id(idCaracteristicas)
            mysqlSet.InsertAlarmHistoVolum(alarmName, stringDate,alarmId,volumeLiters)

        cDate = datetime.utcnow()           
        cDate = cDate.replace(month=cDate.month-1) # minute and alarm time
        if cDate.day == 31:
            cDate = cDate.replace(day = cDate.day-1)

        count = mysqlGet.GetCountFromGatewayHistory(stringDate,gateway_id)

        if count == 0: # if count == 0 there is no flow

            alarmName = mysqlGet.getAlarmName(3)
            tmp = mysqlGet.GetCountFromGatewayHistory(gateway_id)
        # print("creo que era esto") 

        if len(tmp) > 0:    
            idCaracteristicas = tmp[0][0]

        alarmName = mysqlGet.getAlarmName(3)
        aId = alarmName
        if len(aId)!=0:
            alarmId = 3
            mysqlSet.SetAlarmHistoryNoFlow(alarmName, cDate,alarmId)

        ##############################################  
    except:
        textToWrite = b"Error in the meter\n"
        exceptions.exceptionHandler(textToWrite)
    # delta of meters
    return Ctimestampv, fuga, subdimension, CELL_ID, sigtec, accumulated