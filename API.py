'''/* ===================================================== 

* Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved. 

* File_name jsonServer

* Description:  The file that is a http-rest api and handles json's input

* Author:  Álvaro Marí Belmonte   

* Date:  04/10/2022

* Version:  1.3

* =================================================== */ '''

import math
import mysqlGet
import mysqlSet
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from flask_restful import Resource, Api
from flask_httpauth import HTTPBasicAuth
from datetime import datetime, timedelta
from mysqlConnector import *
from narrowSender import narrowSender
from wmbusSender import *

app = Flask(__name__)
CORS(app)
auth = HTTPBasicAuth()


@auth.verify_password
def verify_password(username, password):
    if username == 'api' and password == 'swagger':
        return True
    return False

@app.route("/api_pruebas/battery_device/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def battery_device(sn):
    try:
        date = mysqlGet.getDevicePropertiesFromSn('battery',sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the battery status"}), 400)
    if date != None:
        return jsonify({"battery_device": date[0]})
    else:
        return make_response(jsonify({"message":"Error getting the battery status"}), 400)
    
@app.route("/api_pruebas/imei/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def IMEI(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the IMEI"}), 400)
    if date["imei"] != None:
        return jsonify({"IMEI": date["imei"]})
    else:
        return make_response(jsonify({"message":"Error getting the IMEI"}), 400)
    
@app.route("/api_pruebas/apn/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def APN(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the APN"}), 400)
    if date["apn"] != None:
        return jsonify({"APN": date["apn"]})
    else:
        return make_response(jsonify({"message":"Error getting the APN"}), 400)

@app.route("/api_pruebas/signal_power/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def signal_power(sn):
    try:
        date = mysqlGet.getDevicePropertiesFromSn("signal_threshold", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the signal power"}), 400)
    if date != None:
        return jsonify({"Signal_Power": date[0]})
    else:
        return make_response(jsonify({"message":"Error getting the signal power"}), 400)

@app.route("/api_pruebas/wmbus_mode/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmb_mode(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the Wireless M Bus mode"}), 400)
    if date["wmb_mode"] != None:
        return jsonify({"TxMode": date["wmb_mode"]})
    else:
        return make_response(jsonify({"message":"Error getting the Wireless M Bus mode"}), 400)

@app.route("/api_pruebas/report_time/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def report_time(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the measurement interval of the device"}), 400)
    if date["reportTime"] != None:
        return jsonify({"reportTime": date["reportTime"]})
    else:
        return make_response(jsonify({"message":"Error getting the measurement interval of the device"}), 400)
    
@app.route("/api_pruebas/tx_time/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def tx_time(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the tx_time of the device"}), 400)
    if date["last_mssg_send"] != None:
        return jsonify({"tx_time": date["last_mssg_send"]})
    else:
        return make_response(jsonify({"message":"Error getting the tx_time of the device"}), 400)

@app.route("/api_pruebas/granularity/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def granularity(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the granularity"}), 400)
    if date["reportTime"] != None:
        h = " hours"
        if date["reportTime"]/24 == 1:
            h = " hour"
        return jsonify({"TxInterval": str(date["reportTime"]/24) + h})
    else:
        return make_response(jsonify({"message":"Error getting the interval time"}), 400)

# @app.route("/data/<string:id>", methods=["OPTIONS","GET"])
# @auth.login_required

# def data(id):
#     try:
#         imei = mysqlGet.getDevicePropertiesFromSn('imei',id)
#         sender = wmbusSender(imei[0])
#         json = sender.get_json()

#     except Exception:
#         return make_response(jsonify({"message": "Error getting volume, pression and temperature"} ), 400)
#     if json != None:
#         return json
#     else:
#         return make_response(jsonify({"message": "Error getting volume, pression and temperature"} ), 400)

@app.route("/api_pruebas/data/<string:sn>/<string:fecha>", methods=["OPTIONS","GET"])
@auth.login_required

def data(sn, fecha):
    try:
        sender = narrowSender(sn, fecha)
        json = sender.get_json()
    except Exception:
        return make_response(jsonify({"message": "Error fetching data"} ), 400)
    if json != None:
        return json
    else:
        return make_response(jsonify({"message": "Error fetching data"} ), 400)

@app.route("/api_pruebas/wmbus_measurement_interval/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmbus_measurement_interval(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the Wireless M Bus measurement interval"}), 400)
    if date["wmb_measurement_interval"] != None:
        return jsonify({"measurement interval": date["wmb_measurement_interval"]})
    else:
        return make_response(jsonify({"message":"Error getting the Wireless M Bus meassurement interval"}), 400)

@app.route("/api_pruebas/wmbus_measurement_window/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmb_measurement_window(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the Wireless M Bus measurement window"}), 400)
    if date["wmb_measurement_window"] != None:
        return jsonify({"wmb measurement window": date["wmb_measurement_window"]})
    else:
        return make_response(jsonify({"message":"Error getting the Wireless M Bus meassurement window"}), 400)

@app.route("/api_pruebas/sensor_manufacturer/<string:id>", methods=["OPTIONS","GET"])
@auth.login_required

def sensor_manufacturer(id):
    try:
        date = mysqlGet.getGreyListSensorById(id)
    except Exception:
        return make_response(jsonify({"message":"Error getting the manufacturer"}), 400)
    if date["manufacturer"] != None:
        return jsonify({"Manufacturer": date["manufacturer"]})
    else:
        return make_response(jsonify({"message":"Error getting the manufacturer"}), 400)


@app.route("/api_pruebas/sensor_model/<string:id>", methods=["OPTIONS","GET"])
@auth.login_required

def sensor_model(id):
    try:
        date = mysqlGet.getGreyListSensorById(id)
    except Exception:
        return make_response(jsonify({"message":"Error getting the sensor model"}), 400)
    if date["model"] != None:
        return jsonify({"Model": date["model"]})
    else:
        return make_response(jsonify({"message":"Error getting the sensor model"}), 400)


@app.route("/api_pruebas/sensor_vertical/<string:id>", methods=["OPTIONS","GET"])
@auth.login_required

def sensor_vertical(id):
    try:
        date = mysqlGet.getGreyListSensorById(id)
    except Exception:
        return make_response(jsonify({"message":"Error getting the vertical"}), 400)
    if date["vertical"] != None:
        return jsonify({"Vertical": date["vertical"]})
    else:
        return make_response(jsonify({"message":"Error getting the vertical"}), 400)

@app.route("/api_pruebas/wmbus_white_list/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmbus_white_list(sn):
    try:
        date = mysqlGet.getWhiteListBySn('sensor_id',sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the wmb white list"}), 400)
    return jsonify({"wmb white list":' '.join(date)})

@app.route("/api_pruebas/wmbus_grey_list/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmbus_grey_list(sn):
    try:
        date = mysqlGet.getGreyListBySn('sensor_id',sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the wmb grey list"}), 400)
    return jsonify({"wmb grey list":' '.join(date)})
    

@app.route("/api_pruebas/wmbus_measurement_window_average/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmbus_measurement_window_average(sn):
    try:
        # date = mysqlGet.getDeviceProperties("sn", sn)
        date = 24
    except Exception:
        return make_response(jsonify({"message":"Error getting the wmb measurement window average"}), 400)
    if date != None:
        return jsonify({"wmb measurement window average": date})
    else:
        return make_response(jsonify({"message":"Error getting the wmb meassurement window average"}), 400)
    
@app.route("/api_pruebas/hw_version/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def hw_version(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the hardware version"}), 400)
    if date["hw"] != None:
        return jsonify({"hw_version": date["hw"]})
    else:
        return make_response(jsonify({"message":"Error getting the hardware version"}), 400)
    
@app.route("/api_pruebas/fw_version/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def fw_version(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(jsonify({"message":"Error getting the firmware version"}), 400)
    if date["fw"] != None:
        return jsonify({"fw_version": date["fw"]})
    else:
        return make_response(jsonify({"message":"Error getting the firmware version"}), 400)

@app.route("/api_pruebas/imsi/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def imsi(sn):
    try:
        # date = mysqlGet.getDeviceProperties("sn", sn)
        date = 2760111
    except Exception:
        return make_response(jsonify({"message":"Error getting the IMSI"}), 400)
    if date != None:
        return jsonify({"imsi": date})
    else:
        return make_response(jsonify({"message":"Error getting the IMSI"}), 400)

@app.route("/api_pruebas/signal_threshold/<string:sn>/<int:threshold>", methods=["OPTIONS","POST","GET"])
@auth.login_required

def signal_threshold(sn, threshold):
    try:
        mysqlSet.updateDevice_property(sn, "signal_threshold", threshold)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the signal threshold"}), 400)
    
@app.route("/api_pruebas/report_time/<string:sn>/<int:report_time>", methods=["OPTIONS","POST","GET"])
@auth.login_required

def set_report_time(sn, report_time):
    try:
        mysqlSet.updateDevice_property(sn, "reportTime", report_time)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the report time"}), 400)
    
@app.route("/api_pruebas/wmbus_mode/<string:sn>/<string:wmbus_mode>", methods=["OPTIONS","POST","GET"])
@auth.login_required

def wmbus_mode(sn, wmbus_mode):
    try:
        mysqlSet.updateDevice_property(sn, "wmb_mode", wmbus_mode)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the WMBus mode"}), 400)
    
@app.route("/api_pruebas/wmbus_measurement_window/<string:sn>/<string:window>", methods=["OPTIONS","POST","GET"])
@auth.login_required

def set_wmbus_measurement_window(sn, window):
    try:
        mysqlSet.updateDevice_property(sn, "wmb_measurement_window", window)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the wmbus measurement window"}), 400)
    

@app.route("/api_pruebas/sensor_manufacturer/<string:id>/<string:manufacturer>", methods=["OPTIONS","POST","GET"])
@auth.login_required

def set_sensor_manufacturer(id, manufacturer):
    try:
        mysqlSet.updateGreyListProperty(id, "manufacturer", manufacturer)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the sensor manufacturer"}), 400)
    
@app.route("/api_pruebas/sensor_model/<string:id>/<string:model>", methods=["OPTIONS","POST","GET"])
@auth.login_required

def set_sensor_model(id, model):
    try:
        mysqlSet.updateGreyListProperty(id, "model", model)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the sensor model"}), 400)
    
@app.route("/api_pruebas/sensor_vertical/<string:id>/<string:vertical>", methods=["OPTIONS","POST","GET"])
@auth.login_required

def set_sensor_vertical(id, vertical):
    try:
        mysqlSet.updateGreyListProperty(id, "vertical", vertical)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the sensor vertical"}), 400)
    
@app.route("/api_pruebas/wmbus_white_list/<string:sn>/<string:white_list>", methods=["OPTIONS","POST","GET"])
@auth.login_required

def set_wmbus_white_list(sn, white_list):
    lista = white_list.split(",")
    if len(lista) > 15:
        return make_response(jsonify({"message":"Error setting the wmbus white list. Devices on white list are limited to 15"}), 400)
    try:
        mysqlSet.deleteWhiteList(sn)
        for l in lista:
            mysqlSet.insertNewWhiteList(l)        
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the wmbus white list"}), 400)
    
@app.route("/api_pruebas/device", methods=["OPTIONS","POST","GET"])
@auth.login_required

def device():
    try:
        sn = request.get_json().get("sn")
        imei = request.get_json().get("imei")
        mysqlSet.insertNewDevice(imei, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 23, "23-1.05", "2.4.0", "-120", sn, 1)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting device"}), 400)


def server_run():
    app.run(host='0.0.0.0',port=8080)

if __name__ == "__main__":
    server_run()