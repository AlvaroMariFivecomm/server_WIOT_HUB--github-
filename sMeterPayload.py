# /* ===================================================== 
# * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY
#   VERTICALS S.L. All rights reserved. 
# * File_name sMeterPayload
# * Description:  The file that handles the sMeter messages
# * Author:  Alejandro
# * Date:  25-08-21
# * Version:  1.4
# * =================================================== */ 

import time
import sys
import math
import copy

from datetime import datetime
import exceptions
import mysqlGet
import mysqlSet
import timeHandler
import sigConversor
import mqttConect
import battery as bat
import kamstrup

def comandCheck(tmpRemote,command):

    vectorRemote = []

    if len(tmpRemote) != 0 or command != '0':
        command = command
    else:
        command = 'NEW;0;86400'#SLEEP.
        msg3 = "{"+"'command'"+":'"+command+"'}"
        # msg3 = {"command":command}
    if(command != "none"):
        msg3 = "{"+"'command'"+":'"+command+"'}"
        # msg3 = {'command':command}
    else:
        command = 'NEW;0;86400'#SLEEP
        msg3 = "{"+"'command'"+":'"+command+"'}"

    return msg3

def sMeterPayload(msg,mqttRecieved):
    try:
        # time.sleep(5)
        # NO EJECUTAR none

        print("sMeter")
        try:
            gateway_id = int(msg.topic.replace("t", ""))
            client2 = mqttConect.mqttConect("p2")

            if int(mqttRecieved[7]) < 38000:
                tmpBattery = mqttRecieved[7]
                battery = int(tmpBattery)/100.0
                battery = math.ceil(battery)/10.0
                battery = bat.batteryConversion(battery,gateway_id)

                command = mysqlGet.getCommandFromGatewayId(gateway_id)
                if len(command) != 0 and command != '0':
                    idCommand = mysqlGet.getIdCommandFromGatewayId(gateway_id)
                    mysqlSet.UpdateRemoteConsole(idCommand)
                else:
                    command = "NEW;0;600"
                    last_command = str(mysqlGet.getCommandExecutedFromGatewayId(gateway_id)[0])
                    if last_command != command:
                        mysqlSet.SetRemoteConsole(gateway_id,command,1,1)
            else:
                tmpBattery = mqttRecieved[7]
                battery = int(tmpBattery)/100.0
                battery = math.ceil(battery)/10.0
                battery = bat.batteryConversion(battery,gateway_id)
                command = mysqlGet.getCommandFromGatewayId(gateway_id)

                if len(command) != 0 and command != '0':
                    mysqlSet.UpdateRemoteConsole(idCommand)
                    idCommand = mysqlGet.getIdCommandFromGatewayId(gateway_id)
                    # if int(str(command[0][0]).split(';')[1]) != 4:
                        # command = (("NEW;0;600",'1'),'1')
                        
                else:
                    command = "NEW;0;300"
                    last_command = str(mysqlGet.getCommandExecutedFromGatewayId(gateway_id)[0])
                    if last_command != command:
                        mysqlSet.SetRemoteConsole(gateway_id,command,1,1)

                
            msg3 = comandCheck(command,command)


            time.sleep(0.025)
            result = client2.publish("r"+str(gateway_id), str(msg3)) 
            

        except:
            print("caution in the publishing sMeter!")
            print(sys.exc_info())
            text_to_write = b"Error publishing the command\n"
            exceptions.exceptionHandler(text_to_write)
                                
        CELL_ID = mqttRecieved[6]
        sigtec = mqttRecieved[1]
        
        if int(sigtec) == 0 or int(sigtec) == 1 or int(sigtec) == 2 \
        or int(sigtec) == 3:
            cTime = int(time.time())

            string_date = timeHandler.obtainStringFromDatetimeWithMinute(datetime.fromtimestamp(cTime))

            ### added for json ##
            c_timestampv = string_date
            ########################
            G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP, \
            G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio \
            = sigConversor.signal_values(mqttRecieved[2], sigtec)
            meterSerialNumber = mqttRecieved[3].split(",")
            vector_timestamps = mqttRecieved[4].split(",")
            c_vector_timestamps = copy.deepcopy(vector_timestamps)
            accumulated =0
        else:
            print("error")
            
        

        ######################################################
        ##### INSERT TO GATEWAY_HISTORY ###################################
        value = "test1"
        name = "pruebaBD"

        timestamp = string_date
        serialNumber = msg.topic.replace("t", "")
        serialNumber = int(serialNumber)
        GATEWAY_PROPERTIES_id = serialNumber
        try:

            mysqlSet.InsertGateway_History(c_vector_timestamps,timestamp,\
            accumulated,CELL_ID,battery,GATEWAY_PROPERTIES_id,value, name,\
            sigtec, G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP,\
            G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio)

            # id_caracteristicas,tFuga = mysqlGet.getTLeakFromGateway(gateway_id)

            mysqlSet.InsertAlarm(str(mqttRecieved[5]),gateway_id)
            
        except Exception as ex:
            print("Error",ex)
        ######################################################
        ######################################################
        ### check normal alarms (no pulses a month and too much flow for the diameter)
        c_date = datetime.utcnow()
        tempTime = datetime.timestamp(c_date)

        # 0~0
        # mysqlClose(mydbConnector3,myCursor3)
        tmp_sum = 0
        for i in c_vector_timestamps:
            if int(i) != 0:
                tmp_sum += 1 
        fuga = tmp_sum == 24

        subdimension = False
        i = 0
        lenVector = len(vector_timestamps)
       
        c_date = datetime.utcnow()

        string_date = timeHandler.stringFromDatetime(c_date)
        tmp = []
        alarmName = ""
        
            
        c_date = datetime.utcnow()           
        
        if c_date.day == 31:
            c_date = c_date.replace(day = c_date.day-1)

        if c_date.month == 1:
            c_date = c_date.replace(month=12,year=c_date.year-1) # minute and alarm time
        else:
            c_date = c_date.replace(month=c_date.month-1) # minute and alarm time
            
        try:  
            string_date = timeHandler.stringFromDatetime(c_date)
            
            count = mysqlGet.GetCountFromGatewayHistoryTwo(string_date,gateway_id)
            if count == 0: # if count == 0 there is no flow
              
                # tmp = mysqlGet.GetCountFromGatewayHistory(gateway_id)
                print("count == 0")

        except:
            text_to_write = b"Error obtaining the system features id from GATEWAY_PROPERTIES in sMeter\n"
            exceptions.exceptionHandler(text_to_write)
        if len(tmp) > 0:    
            id_caracteristicas = tmp[0][0]
        try:
            aId =  mysqlGet.getAlarmName(3)

            if len(aId)!=0:
                alarmId = 3
                
        except:
            text_to_write = b"Error inserting the alarm history sMeter\n"
            exceptions.exceptionHandler(text_to_write)
            ##############################################  
    except Exception as ex:
        text_to_write = b"Error in the sMeter\n",ex
        exceptions.exceptionHandler(text_to_write)

    return c_timestampv, fuga, subdimension, CELL_ID, sigtec, accumulated, c_vector_timestamps, battery


def sMeterPayloadHexa(msg,mqttRecieved):
    try:
        # time.sleep(5)
        # NO EJECUTAR none

        print("sMeter")
        try:
            gateway_id = int(msg.topic.replace("t", ""))
            client2 = mqttConect.mqttConect("p2")

            if int(mqttRecieved[7]) < 38000:
                tmpBattery = mqttRecieved[7]
                battery = int(tmpBattery)/100.0
                battery = math.ceil(battery)/10.0
                battery = bat.batteryConversion(battery,gateway_id)

                command = mysqlGet.getCommandFromGatewayId(gateway_id)
                if len(command) != 0 and command != '0':
                    idCommand = mysqlGet.getIdCommandFromGatewayId(gateway_id)
                    mysqlSet.UpdateRemoteConsole(idCommand)
                else:
                    command = "NEW;0;600"
            else:
                tmpBattery = mqttRecieved[7]
                battery = int(tmpBattery)/100.0
                battery = math.ceil(battery)/10.0
                battery = bat.batteryConversion(battery,gateway_id)
                command = mysqlGet.getCommandFromGatewayId(gateway_id)

                if len(command) != 0 and command != '0':
                    idCommand = mysqlGet.getIdCommandFromGatewayId(gateway_id)
                    mysqlSet.UpdateRemoteConsole(idCommand)
                    # if int(str(command[0][0]).split(';')[1]) != 4:
                        # command = (("NEW;0;600",'1'),'1')
                else:
                    command = "NEW;0;300"
            msg3 = comandCheck(command,command)


            time.sleep(0.025)
            result = client2.publish("r"+str(gateway_id), str(msg3)) 
            

        except:
            print("caution in the publishing sMeter!")
            print(sys.exc_info())
            text_to_write = b"Error publishing the command\n"
            exceptions.exceptionHandler(text_to_write)
                                
        CELL_ID = mqttRecieved[6]
        sigtec = mqttRecieved[1]
        
        if int(sigtec) == 0 or int(sigtec) == 1 or int(sigtec) == 2 \
        or int(sigtec) == 3:
            cTime = int(time.time())

            string_date = timeHandler.obtainStringFromDatetime(datetime.fromtimestamp(cTime))

            ### added for json ##
            c_timestampv = string_date
            ########################
            G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP, \
            G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio \
            = sigConversor.signal_values(mqttRecieved[2], sigtec)
            meterSerialNumber = mqttRecieved[3].split(",")
            vector_timestamps = mqttRecieved[4].split(",")
            c_vector_timestamps = []
            # c_vector_timestamps = copy.deepcopy(vector_timestamps)
            for iterator in vector_timestamps:
                c_vector_timestamps.append(kamstrup.processData(str(iterator)))
            accumulated =0
            alarm = kamstrup.process_alarm(mqttRecieved[5])
        else:
            print("error")
            
        

        ######################################################
        ##### INSERT TO GATEWAY_HISTORY ###################################
        value = "test1"
        name = "pruebaBD"

        timestamp = string_date
        serialNumber = msg.topic.replace("t", "")
        serialNumber = int(serialNumber)
        GATEWAY_PROPERTIES_id = serialNumber
        try:

            mysqlSet.InsertGateway_HistoryKamstrup(c_vector_timestamps,timestamp,\
            accumulated,CELL_ID,battery,GATEWAY_PROPERTIES_id,value, name,\
            sigtec, G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP,\
            G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio)

            # id_caracteristicas,tFuga = mysqlGet.getTLeakFromGateway(gateway_id)

            mysqlSet.InsertAlarm(str(mqttRecieved[5]),gateway_id)
            
        except Exception as ex:
            print("Error",ex)
        ######################################################
        ######################################################
        ### check normal alarms (no pulses a month and too much flow for the diameter)
        c_date = datetime.utcnow()
        tempTime = datetime.timestamp(c_date)

        # 0~0
        # mysqlClose(mydbConnector3,myCursor3)
        fuga = False
        subdimension = False
        i = 0
        lenVector = len(vector_timestamps)
       
        c_date = datetime.utcnow()

        string_date = timeHandler.stringFromDatetime(c_date)
        tmp = []
        alarmName = ""
        
            
        c_date = datetime.utcnow()           
        
        if c_date.day == 31:
            c_date = c_date.replace(day = c_date.day-1)
        c_date = c_date.replace(month=c_date.month-1) # minute and alarm time
        try:  
            string_date = timeHandler.stringFromDatetime(c_date)
            
            count = mysqlGet.GetCountFromGatewayHistoryTwo(string_date,gateway_id)
            if count == 0: # if count == 0 there is no flow
              
                # tmp = mysqlGet.GetCountFromGatewayHistory(gateway_id)
                print("count == 0 kams")

        except:
            text_to_write = b"Error obtaining the system features id from GATEWAY_PROPERTIES in sMeter\n"
            exceptions.exceptionHandler(text_to_write)
        if len(tmp) > 0:    
            id_caracteristicas = tmp[0][0]
        try:
            aId =  mysqlGet.getAlarmName(3)

            if len(aId)!=0:
                alarmId = 3
                
        except:
            text_to_write = b"Error inserting the alarm history sMeter\n"
            exceptions.exceptionHandler(text_to_write)
            ##############################################  
    except Exception as ex:
        text_to_write = b"Error in the sMeter\n",ex
        exceptions.exceptionHandler(text_to_write)

    return c_timestampv, fuga, subdimension, CELL_ID, sigtec, accumulated, c_vector_timestamps, battery, alarm