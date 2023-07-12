'''/* ===================================================== 

* Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved. 

* File_name jsonServer

* Description:  The file that is a http-rest api and handles json's input

* Author:  Alejandro Beltran Roig   

* Date:  04/10/2022

* Version:  1.3

* =================================================== */ '''
import math
from jsonSender import JSON_Sender
import mysqlGet
import mysqlSet
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_restful import Resource, Api
from flask_httpauth import HTTPBasicAuth
import datetime

app = Flask(__name__)
CORS(app)
auth = HTTPBasicAuth()
json_sender = JSON_Sender("http://192.168.0.32:5000/", "none")

def getIDFromName(name):
    id = mysqlGet.getIdFromName(name)
    # id_num = "5GLITEA"+(6-len(str(serial_number)))*"0"+str(serial_number)+"4122"

    return id

@auth.verify_password
def verify_password(username, password):
    if username == 'username' and password == 'password':
        return True
    return False

@app.route("/battery/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def battery(sn):
    tmpBattery = mysqlGet.getBatteryFromGatewayId(sn)
    battery = int(tmpBattery)/10.0
    battery = math.ceil(battery)/10.0
    if battery != None:
        return jsonify({"battery": battery*100})
    else:
        return jsonify({"message":"Error getting the battery"})

@app.route("/location/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def location(sn):
    date = mysqlGet.getLocationFromGatewayId(sn)[0]
    if date != None:
        return jsonify({"location": date})
    else:
        return jsonify({"message":"Error getting the location"})

@app.route("/IMEI/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def IMEI(sn):
    date = mysqlGet.getIMEIFromGatewayId(sn)[0]
    if date != None:
        return jsonify({"IMEI": date})
    else:
        return jsonify({"message":"Error getting the IMEI"} )

@app.route("/LastConection/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def LastConection(sn):
    date = mysqlGet.getLastConnectionFromGatewayId(sn)
    if date != None:
        return jsonify({"last_connection": date})
    else:
        return jsonify({"message":"Error getting the battery status"} )

@app.route("/APN/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def APN(sn):
    date = "orangeworld"
    if date != None:
        return jsonify({"APN": date})
    else:
        return jsonify({"message":"Error getting the APN"})

@app.route("/Signal_Power/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def Signal_Power(sn):
    date = mysqlGet.getSignalPowerFromGatewayId(sn)
    if date[0] != None:
        return jsonify({"SignalPower": date})
    else:
        return jsonify({"message":"Error getting the signal power"})

@app.route("/HWversion/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def HWversion(sn):
    date = mysqlGet.getHWversionFromGatewayId(sn)
    if date != None:
        return jsonify({"HWversion": date})
    else:
        return jsonify({"message":"Error getting the hardware version"})

@app.route("/FWversion/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def FWversion(sn):
    date = mysqlGet.getFWversionFromGatewayId(sn)
    if date != None:
        return jsonify({"FWversion": date})
    else:
        return jsonify({"message":"Error getting the firmware version"})

@app.route("/battery_status/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def BatteryStatus(sn):
    date = mysqlGet.getBatteryStatusFromGatewayId(sn)
    if date != None:
        return jsonify({"BatteryStatus": date})
    else:
        return jsonify({"message":"Error getting the battery status"} )

@app.route("/status/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def status(sn):
    date = mysqlGet.getStatusFromGatewayId(sn)
    if date != None:
        return jsonify({"Status": date})
    else:
        return jsonify({"message":"Error getting the device status"})

@app.route("/TxMode/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def TransmissionMode(sn):
    date = mysqlGet.getTxModeFromGatewayId(sn)
    if date != None:
        return jsonify({"TransmissionMode": date})
    else:
        return jsonify({"message":"Error getting the transmission mode"})


@app.route("/TxInterval/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def TransmissionInterval(sn):
    date = mysqlGet.getTxIntervalFromGatewayId(sn)
    if date != None:
        return jsonify({"TransmissionInterval": date})
    else:
        return jsonify({"message":"Error getting the interval time"})


@app.route("/TxTime/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def TransmissionTime(sn):
    date = mysqlGet.getTxTimeFromGatewayId(sn)
    date = date[0].strftime('%d/%m/%YT%H:%M:%sZ')

    if date != None:
        return jsonify({"TransmissionTime": date})
    else:
        return jsonify({"message":"Error getting the transmision time of the device"})


@app.route("/measurement_interval/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def MeasurementInterval(sn):
    date =  "1 hour"
    if date != None:
        return jsonify({"MeasurementInterval": date})
    else:
        return jsonify({"message":"Error getting the measurement interval of the device"} )


@app.route("/dataserver/<int:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def dataServer(sn):
    date = json_sender.get_url()
    if date != None:
        return jsonify({"DataServer": date})
    else:
        return jsonify({"message":"Error getting the server"})


@app.route("/setReportTime/<string:sn>/<string:reportTime>", methods=["OPTIONS","POST"])
@auth.login_required

def setReportTime(sn,reportTime):
        try:    
            maxReportTime = "23:59"
            minReportPeriod = "00:00"
            gateway_id = int(sn)
            command = mysqlGet.getCommandFromGatewayId(gateway_id)
            message_to_send =command
            if (len(command) != 0 and command != '0'):
                message_to_send = f'The device: {gateway_id} allready has an active order, please whait until is executed'
            else:  
                period = reportTime
                if int(str(period).split(":")[0]) >= 0 and int(str(period).split(":")[0]) <= 23 and int(str(period).split(":")[1]) >= 0 and int(str(period).split(":")[1]) <=59:
                    board_transmision_time = mysqlGet.getLastTimestamp(gateway_id)[0]
                    deltaTime = board_transmision_time.hour-int(str(period).split(":")[0])
                    if deltaTime <= 0: # if delta time is > 0 is in the actual day
                        lasMessage = mysqlGet.getCommandExecutedFromGatewayId(gateway_id)
                        if lasMessage != None:
                            last_time = int(str(lasMessage[0]).split(";")[2])
                            secondsToWakeup = (-deltaTime * 3600) + (int(str(period).split(":")[1])-board_transmision_time.minute)*60 # if the minutes are negative there are less seconds in the hour
                            secondsToWakeup += last_time
                        secondsToWakeup = (-deltaTime * 3600) + (int(str(period).split(":")[1])-board_transmision_time.minute)*60 # if the minutes are negative there are less seconds in the hour
                        #secondsToWakeup = (board_transmision_time.hour-int(str(period).split(":")[1]))*3600+(board_transmision_time.minute-int(str(period).split(":")[1]))*60

                    else: 
                        secondsToWakeup = 86400-((deltaTime * 3600) + (board_transmision_time.minute-int(str(period).split(":")[1]))*60) # if the minutes are negative there are less seconds in the hour
                        lasMessage = mysqlGet.getCommandIdFromGatewayId(gateway_id)
                        if (len(str(lasMessage)) != 0 and lasMessage != '0'):
                            last_time = lasMessage
                            secondsToWakeup += last_time
                    message_to_send = 'setReportTime ok for device %s' % gateway_id
                    if gateway_id == 'all':
                        gateway_id = mysqlGet.getMaxId()
                        for i in range(int(gateway_id[0])+1):
                            mysqlSet.SetRemoteConsole(i,'NEW;0;%s' % secondsToWakeup,0,1)

                    else:
                        mysqlSet.SetRemoteConsole(gateway_id,'NEW;0;%s' % secondsToWakeup,0,1)
                else:
                    message_to_send = f'setReportTime not in bound, the max is: {maxReportTime} and the min is {minReportPeriod} for device {gateway_id}'
            return jsonify({"setReportTime": message_to_send})

        except Exception as ex:
            print('error')

@app.route("/setSignalThreshold/<string:sn>/<string:threshold>", methods=["OPTIONS","POST"])
@auth.login_required

def setSignalThreshold(sn,threshold):
        maxThreshold = -30
        min_Threshold = -140
        gateway_id = int(sn)
        command = mysqlGet.getCommandFromGatewayId(gateway_id)
        if (len(command) != 0 and command != '0'):
            message_to_send = f'The device: allready has an active order, please whait until is executed'
        else:    
            # if command 
            threshold = threshold
            if int(threshold) >= min_Threshold and int(threshold) <= maxThreshold:
                message_to_send = 'setSignalThreshold ok for device %s' % gateway_id
                if gateway_id == 'all':
                    gateway_id = mysqlGet.getMaxId()
                    for i in range(int(gateway_id[0])+1):
                        mysqlSet.SetRemoteConsole(i,'NEW;1;%s' % -int(threshold),0,1)
                else:
                    mysqlSet.SetRemoteConsole(gateway_id,'NEW;1;%s' % -int(threshold),0,1)
            else:
                message_to_send = f'setSignalThreshold not in bounds, the min is: {min_Threshold} and the max is {maxThreshold} for device {gateway_id}'
            
        return jsonify({"setSignalThreshold": message_to_send})

@app.route("/SetTxMode/<string:sn>/<string:mode>", methods=["OPTIONS","POST"])
@auth.login_required

def SetTxMode(sn,mode):
    if request.method == "POST":
        if mysqlSet.updateTxMode(sn,mode):
            mssg_to_send = jsonify({"SetTxMode": "okey"})
        else:
            mssg_to_send = jsonify({"SetTxMode": "error"})
        return mssg_to_send
    else:
        return "okey"

@app.route("/SetTxInterval/<string:sn>/<string:TxInterval>", methods=["OPTIONS","POST"])
@auth.login_required

def SetTxInterval(sn,TxInterval):
    if request.method == "POST":
        if  mysqlSet.updateTxInterval(sn,TxInterval):
            mssg_to_send = jsonify({"SetTxInterval": "okey"})
        else:
            mssg_to_send = jsonify({"SetTxInterval": "error"})
        return mssg_to_send
    else:
        return "okey"


@app.route("/SetTxTime/<string:sn>/<string:TxTime>", methods=["OPTIONS","POST"])
@auth.login_required

def SetTxTime(sn,TxTime):
    if request.method == "POST":
        if mysqlSet.updateTxtime(sn,TxTime):
            mssg_to_send = jsonify({"SetTxTime": "okey"})
        else:
            mssg_to_send = jsonify({"SetTxTime": "error"})
        return mssg_to_send
    else:
        return "okey"

@app.route("/SetAPN/<string:sn>/<string:APN>", methods=["OPTIONS","POST"])
@auth.login_required

def SetAPN(sn,APN):
    if request.method == "POST":
        if mysqlSet.updateAPN(sn,APN):
            mssg_to_send = jsonify({"SetAPN": "okey"})
        else:
            mssg_to_send = jsonify({"SetAPN": "error"})
        return mssg_to_send
    else:
        return "okey"

@app.route("/SetServerIP/<string:sn>/<string:server_IP>", methods=["OPTIONS","POST"])
@auth.login_required

def SetServerIP(sn,server_IP):
    #date = mysqlGet.getTxIntervalFromGatewayId(sn)
    return jsonify({"SetServerIP": "83.56.30.233"})

@app.route("/GetData/<string:id>", methods=["OPTIONS","GET"])
@auth.login_required

def GetData(id):
    date = mysqlGet.getAllData(id)
    if date != None:
        print(date["Volume"])
        return jsonify({"Volume": date["Volume"], "Pressure":date["Pressure"], "Temperature":date["Temperature"]})
    else:
        return jsonify({"message": "Error getting volume, pression and temperature"} )

def server_run():
    app.run(host='0.0.0.0',port=8080)

if __name__ == "__main__":
    server_run()
