# /* ===================================================== 
# * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved. 
# * File_name scriptRecepcionV33
# * Description:  The main file of the mqtt platform
# * Author:  Alejandro
# * Date:  25-08-21
# * Version:  1.33
# * =================================================== */ 

import time
import sys
import copy
import math

from datetime import datetime
import exceptions
import mysqlGet
import mysqlSet
import timeHandler
import mqttConect
import sigConversor
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
    
def standMsg(msg,mqttRecieved,monthPrevious,dayPrevious,hourPrevious):    
    try:
        # time.sleep(5)
        # NO EJECUTAR none
        print("stand")
        try:
            vectorRemote = []
            gateway_id = int(msg.topic.replace("t", ""))
            client2 = mqttConect.mqttConect("p2")
            if int(mqttRecieved[7]) < 4001:
                tmpBattery = mqttRecieved[7]
                battery = int(tmpBattery)/100.0
                battery = math.ceil(battery)/10.0
                battery = bat.batteryConversion(battery,gateway_id)

                tmpRemote = mysqlGet.getCommandFromGatewayId(gateway_id)
            else:
                tmpRemote = "{'command':'NEW;0;300'}"
            msg3 = comandCheck(tmpRemote)

            time.sleep(0.01)
            result = client2.publish("r"+str(gateway_id), str(msg3)) 

        except:
            print("caution in the publishing stand!")
            print(sys.exc_info())
            textToWrite = b"Error publishing the command\n"
            exceptions.exceptionHandler(textToWrite)
                                
        accumulated = mqttRecieved[5]
        CELL_ID = mqttRecieved[6]
        sigtec = mqttRecieved[1]
        
        if int(sigtec) == 0 or int(sigtec) == 1 or int(sigtec) == 2 \
        or int(sigtec) == 3:
            firstTime = mqttRecieved[4]
            cTime = datetime.fromtimestamp(int(firstTime))

            stringDate = timeHandler.obtainStringFromDatetime(cTime)   
            ### added for json ##
            Ctimestampv = stringDate
            ########################
            G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP, \
            G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, \
            G3_ecio = sigConversor.signal_values(mqttRecieved[2], sigtec)
            vectorTimestamps = mqttRecieved[3].split(",")
            cVectorTimestamps = []
            cVectorTimestamps = copy.deepcopy(vectorTimestamps)
        else:
            print("error")
            


        ######################################################
        ##### INSERT TO GATEWAY_HISTORY ###################################
        value = "test1"
        name = "pruebaBD"

        if firstTime != 0:
            timestamp = Ctimestampv
        else:
            cTime = datetime.utcnow()
            timestamp = str(cTime.year) + "-" + monthPrevious + "-" \
            + dayPrevious + " " + hourPrevious + ":00:00"

        serialNumber = int(msg.topic.replace("t", ""))
        GATEWAY_PROPERTIES_id = serialNumber # to delete serial number

        mysqlSet.InsertGateway_History(cVectorTimestamps,timestamp,\
        accumulated,CELL_ID,battery,GATEWAY_PROPERTIES_id,value,\
        name, sigtec, G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, \
        G4_RSRP, G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio)

        ######################################################
        ######################################################
        ### check normal alarms (no pulses a month and too much flow for the diameter)
        
        tFuga = mysqlGet.getTimeLeakFromGateway(gateway_id)
        nPulsos = mysqlGet.getNumberOfPulsesFromGateway(gateway_id)

        print(vectorTimestamps)
        fuga = True
        subdimension = False
        i = 0

        for x in vectorTimestamps:
            if int(x) == 0:
                fuga = False
                break
            elif int(x) == nPulsos[0]:
                subdimension = True       


        cDate = datetime.utcnow()
        stringDate = timeHandler.stringFromDatetime(cDate)
        del tmp
        tmp = []
        alarmName = ""
        if fuga == True:
            try:
                idCaracteristicas,tFuga = mysqlGet.getTLeakFromGateway(idCaracteristicas)
                if len(tmp) == 2:   
                    alarmName = tmp[1][0]
                    alarmId = tmp[1][1]
                else:
                    alarmName = mysqlGet.getAlarmName(1)
                    mysqlSet.InsertAlarm(alarmName,idCaracteristicas)
                    alarmId = mysqlGet.getAlarmId()

                stringDate = timeHandler.obtainStringFromDatetime(cDate)
                mysqlSet.InsertAlarmHisto(alarmName,stringDate,alarmId,tFuga)
                
            except:
                textToWrite = b"Error inserting the leak alarm stand\n"
                exceptions.exceptionHandler(textToWrite)
                
        elif subdimension == True:
            alarmName = mysqlGet.getAlarmName(2)
            volumeLiters = mysqlGet.GetVolumeliters(gateway_id)
            alarmId = mysqlGet.GetIdFromSystem_Features_Id(idCaracteristicas)
            mysqlSet.InsertAlarmHistoVolum(alarmName, stringDate,alarmId,volumeLiters)

        cDate = datetime.utcnow()  
        cDate = datetime.fromtimestamp(int(time.time())-(2678400*1))
        
        stringDate = timeHandler.stringFromDatetime(cDate)
            
        count = mysqlGet.GetCountFromGatewayHistory(stringDate,gateway_id)
        if count == 0: # if count == 0 there is no flow
            alarmName = mysqlGet.getAlarmName(3)
            mysqlGet.GetCountFromGatewayHistory(gateway_id)

        if len(tmp) > 0:    
            idCaracteristicas = tmp[0][0]

        mysqlSet.SetAlarmHistoryNoFlow(alarmName, cDate,alarmId)
        ##############################################  
    except:
        textToWrite = b"Error in the stand\n"
        exceptions.exceptionHandler(textToWrite)
    return Ctimestampv, fuga, subdimension, CELL_ID, sigtec, accumulated, cVectorTimestamps
    