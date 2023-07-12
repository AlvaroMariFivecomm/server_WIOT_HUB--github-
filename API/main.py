'''/* ===================================================== 

* Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved. 

* File_name jsonServer

* Description:  The file that is a http-rest api and handles json's input

* Author:  Álvaro Marí Belmonte   

* Date:  04/10/2022

* Version:  1.3

* =================================================== */ '''

import math
import sys
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from flask_restful import Resource, Api
from flask_httpauth import HTTPBasicAuth
from datetime import datetime, timedelta
from mysql import *
import mysqlGet_5G, mysqlGet_narrow, mysqlSet_5G, mysqlSet_narrow
sys.path.append('/root/prod_server_wmbus/server_5G/json_tool')
import wmbusSender
sys.path.append('/root/prod_server_wmbus/server_narrow/json_tools')
from narrowSender import *

app = Flask(__name__)
CORS(app)
auth = HTTPBasicAuth()


@auth.verify_password
def verify_password(username, password):
    if username == 'api' and password == 'swagger':
        return True
    return False

@app.route("/battery_device/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def battery_device(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDevicePropertiesFromSn('battery',sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDevicePropertiesFromSn('battery',sn)
        if date != None:
            return jsonify({"battery_device": date[0]})
        else:
            return make_response(jsonify({"message":"Error getting the battery status"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the battery status"}), 400)
    
    
@app.route("/imei/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def IMEI(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDeviceProperties("sn", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date["imei"] != None:
            return jsonify({"IMEI": date["imei"]})
        else:
            return make_response(jsonify({"message":"Error getting the IMEI"}), 400)    
    except Exception:
        return make_response(jsonify({"message":"Error getting the IMEI"}), 400)
    
    
@app.route("/apn/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def APN(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDeviceProperties("sn", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date["apn"] != None:
            return jsonify({"APN": date["apn"]})
        else:
            return make_response(jsonify({"message":"Error getting the APN"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the APN"}), 400)
    

@app.route("/signal_power/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def signal_power(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDevicePropertiesFromSn("signal_threshold", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDevicePropertiesFromSn("signal_threshold", sn)
        if date != None:
            return jsonify({"Signal_Power": date[0]})
        else:
            return make_response(jsonify({"message":"Error getting the signal power"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the signal power"}), 400)
    
    
@app.route("/wmbus_mode/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmb_mode(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDeviceProperties("sn", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date["wmb_mode"] != None:
            return jsonify({"wmbus_mode": date["wmb_mode"]})
        else:
            return make_response(jsonify({"message":"Error getting the Wireless M Bus mode"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the Wireless M Bus mode"}), 400)
    

@app.route("/report_time/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def report_time(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDeviceProperties("sn", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date["reportTime"] != None:
            return jsonify({"report_time": date["reportTime"]})
        else:
            return make_response(jsonify({"message":"Error getting the report time of the device"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the measurement interval of the device"}), 400)
    
    
@app.route("/tx_time/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def tx_time(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDeviceProperties("sn", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date["last_mssg_send"] != None:
            return jsonify({"tx_time": date["last_mssg_send"]})
        else:
            return make_response(jsonify({"message":"Error getting the tx_time of the device"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the tx_time of the device"}), 400)
    

@app.route("/data_wmbus/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def data(sn):
    try:
        if sn[:5] == '5GWMB':
            imei = mysqlGet_5G.getDevicePropertiesFromSn('imei',id)
            sender = wmbusSender(imei[0])
            json = sender.get_json()
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            json = {'error':'NIOTW not supported'}
        if json != None:
            return json
        else:
            return make_response(jsonify({"message": "Error getting volume, pression and temperature"} ), 400)
    except Exception as e:
        print(e)
        return make_response(jsonify({"message": "Error getting volume, pression and temperature"} ), 400)
    
    
@app.route("/data_narrow/<string:sn>/<string:fecha>", methods=["OPTIONS","GET"])
@auth.login_required

def data_narrow(sn, fecha):
    try:
        if sn[:5] == '5GWMB':
            json = {'error':'5GWMB not supported'}
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
            sender = narrowSender(sn, fecha)
            json = sender.get_json()
        if json != None:
            return json
        else:
            return make_response(jsonify({"message": "Error getting data of the device"} ), 400)
    except Exception as e:
        print(e)
        return make_response(jsonify({"message": "Error getting data of the device"} ), 400)
    

@app.route("/wmbus_measurement_interval/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmbus_measurement_interval(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDeviceProperties("sn", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date["wmb_measurement_interval"] != None:
            return jsonify({"measurement interval": date["wmb_measurement_interval"]})
        else:
            return make_response(jsonify({"message":"Error getting the Wireless M Bus meassurement interval"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the Wireless M Bus measurement interval"}), 400)
    

@app.route("/wmbus_measurement_window/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmb_measurement_window(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDeviceProperties("sn", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date["wmb_measurement_window"] != None:
            return jsonify({"wmbus measurement window": date["wmb_measurement_window"]})
        else:
            return make_response(jsonify({"message":"Error getting the Wireless M Bus meassurement window"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the Wireless M Bus measurement window"}), 400)

@app.route("/wmbus_white_list/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmbus_white_list(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getWhiteListBySn('sensor_id',sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getWhiteListBySn('sensor_id',sn)
        return jsonify({"wmb white list":' '.join(date)})
    except Exception:
        return make_response(jsonify({"message":"Error getting the wmb white list"}), 400)

@app.route("/wmbus_grey_list/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmbus_grey_list(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getGreyListBySn('sensor_id', sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getGreyListBySn(sn)

        return jsonify({"wmb grey list":' '.join(str(i) for i in date)})

    except Exception as e:
        print(e)
        return make_response(jsonify({"message":"Error getting the wmb grey list"}), 400)
    

@app.route("/wmbus_measurement_window_average/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def wmbus_measurement_window_average(sn):
    try:
        # date = mysqlGet.getDeviceProperties("sn", sn)
        date = 24
        if date != None:
            return jsonify({"wmbus measurement window average": date})
        else:
            return make_response(jsonify({"message":"Error getting the wmb meassurement window average"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the wmb measurement window average"}), 400)
    
    
@app.route("/hw_version/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def hw_version(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDeviceProperties("sn", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date["hw"] != None:
            return jsonify({"hw_version": date["hw"]})
        else:
            return make_response(jsonify({"message":"Error getting the hardware version"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the hardware version"}), 400)
    
@app.route("/fw_version/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def fw_version(sn):
    try:
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDeviceProperties("sn", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date["fw"] != None:
            return jsonify({"fw_version": date["fw"]})
        else:
            return make_response(jsonify({"message":"Error getting the firmware version"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the firmware version"}), 400)
    

@app.route("/imsi/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def imsi(sn):
    try:
        # date = mysqlGet.getDeviceProperties("sn", sn)
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDevicePropertiesFromSn("imsi", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date != None and sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            return jsonify({"imsi": date['imsi']})
        elif date != None and sn[:5] == '5GWMB':
            return jsonify({"imsi": date[0]})
        else:
            return make_response(jsonify({"message":"Error getting the IMSI"}), 400)
    except Exception as e:
        print(e)
        return make_response(jsonify({"message":"Error getting the IMSI"}), 400)

@app.route("/manufacturer_filter/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def manufacturer_filter(sn):
    try:
        # date = mysqlGet.getDeviceProperties("sn", sn)
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDevicePropertiesFromSn("filter_manufacturer", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date != None:
            return jsonify({"manufacturer_filter": date["filter_manufacturer"]})
        else:
            return make_response(jsonify({"message":"Error getting the filter_manufacturer"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the filter_manufacturer"}), 400)
    
@app.route("/vertical_filter/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def vertical_filter(sn):
    try:
        # date = mysqlGet.getDeviceProperties("sn", sn)
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDevicePropertiesFromSn("filter_vertical", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date != None:
            return jsonify({"vertical_filter": date["filter_vertical"]})
        else:
            return make_response(jsonify({"message":"Error getting the filter_vertical"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the filter_vertical"}), 400)
    
@app.route("/model_filter/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def model_filter(sn):
    try:
        # date = mysqlGet.getDeviceProperties("sn", sn)
        if sn[:5] == '5GWMB':
            date = mysqlGet_5G.getDevicePropertiesFromSn("filter_model", sn)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            date = mysqlGet_narrow.getDeviceProperties("sn", sn)
        if date != None:
            return jsonify({"model_filter": date["filter_model"]})
        else:
            return make_response(jsonify({"message":"Error getting the filter_vertical"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the filter_vertical"}), 400)
    
@app.route("/signal_threshold/<string:sn>", methods=["OPTIONS","POST"])
@auth.login_required

def signal_threshold(sn):
    try:
        threshold = request.get_json().get("signal_threshold")
        if sn[:5] == '5GWMB':
            mysqlSet_5G.updateDevice_property(sn, "signal_threshold", threshold)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            mysqlSet_narrow.updateDevice_property(sn, "signal_threshold", threshold)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the signal threshold"}), 400)
    
@app.route("/set_report_time/<string:sn>", methods=["OPTIONS","POST"])
@auth.login_required

def set_report_time(sn):   
    try:
        report_time = request.get_json().get("report_time")
        if sn[:5] == '5GWMB':
            mysqlSet_5G.updateDevice_property(sn, "reportTime", report_time)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            mysqlSet_narrow.updateDevice_property(sn, "reportTime", report_time)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the report time"}), 400)
    
@app.route("/set_wmbus_mode/<string:sn>", methods=["OPTIONS","POST"])
@auth.login_required

def wmbus_mode(sn):    
    try:
        wmbus_mode = request.get_json().get("wmbus_mode")
        if sn[:5] == '5GWMB':
            mysqlSet_5G.updateDevice_property(sn, "wmb_mode", wmbus_mode)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            mysqlSet_narrow.updateDevice_property(sn, "wmb_mode", wmbus_mode)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the WMBus mode"}), 400)
    
@app.route("/measurement_window/<string:sn>", methods=["OPTIONS","POST"])
@auth.login_required

def set_wmbus_measurement_window(sn):
    try:
        window = request.get_json().get("measurement_window")
        if sn[:5] == '5GWMB':
            mysqlSet_5G.updateDevice_property(sn, "wmb_measurement_window", int(window))
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            mysqlSet_narrow.updateDevice_property(sn, "wmb_measurement_window", int(window))
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the wmbus measurement window"}), 400)
    

@app.route("/sensor_manufacturer/<string:id>", methods=["OPTIONS","GET"])
@auth.login_required

def sensor_manufacturer(id):
    try:
        wmbus_id = id
        date = mysqlGet_narrow.getGreyListSensorById(wmbus_id)
        if date["manufacturer"] != None:
            return jsonify({"Manufacturer": date["manufacturer"]})
        else:
            return make_response(jsonify({"message":"Error getting the manufacturer"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the manufacturer"}), 400)
    
@app.route("/sensor_model/<string:id>", methods=["OPTIONS","GET"])
@auth.login_required

def sensor_model(id):
    try:
        wmbus_id = id
        date = mysqlGet_narrow.getGreyListSensorById(wmbus_id)
        if date["model"] != None:
            return jsonify({"Model": date["model"]})
        else:
            return make_response(jsonify({"message":"Error getting the sensor model"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error setting the sensor model"}), 400)
    
@app.route("/sensor_vertical/<string:id>", methods=["OPTIONS","GET"])
@auth.login_required

def sensor_vertical(id):
    try:   
        wmbus_id = id
        date = mysqlGet_narrow.getGreyListSensorById(wmbus_id)
        if date["vertical"] != None:
             return jsonify({"Vertical": date["vertical"]})
        else:
            return make_response(jsonify({"message":"Error getting the vertical"}), 400)
    except Exception:
        return make_response(jsonify({"message":"Error getting the vertical"}), 400)
    
@app.route("/sensor_manufacturer/<string:sn>", methods=["OPTIONS","POST"])
@auth.login_required

def set_sensor_manufacturer(sn):
    try:
        manufacturer = request.get_json().get("sensor_manufacturer")
        if sn[:5] == '5GWMB':
            mysqlSet_5G.updateDevice_property(sn, "filter_manufacturer", manufacturer)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            mysqlSet_narrow.updateDevice_property(sn, "filter_manufacturer", manufacturer)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the sensor manufacturer"}), 400)
    
@app.route("/sensor_model/<string:sn>", methods=["OPTIONS","POST"])
@auth.login_required

def set_sensor_model(sn):
    try:
        model = request.get_json().get("sensor_model")
        if sn[:5] == '5GWMB':
            mysqlSet_5G.updateDevice_property(sn, "filter_model", model)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            mysqlSet_narrow.updateDevice_property(sn, "filter_model", model)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the sensor model"}), 400)
    
@app.route("/sensor_vertical/<string:sn>", methods=["OPTIONS","POST"])
@auth.login_required

def set_sensor_vertical(sn):
    try:
        vertical = request.get_json().get("sensor_vertical")
        if sn[:5] == '5GWMB':
            mysqlSet_5G.updateDevice_property(sn, "filter_vertical", vertical)
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            mysqlSet_narrow.updateDevice_property(sn, "filter_vertical", vertical)
        return jsonify({"message":"ok"})
    except Exception:
        return make_response(jsonify({"message":"Error setting the sensor vertical"}), 400)
    
@app.route("/wmbus_white_list/<string:sn>", methods=["POST"])
@auth.login_required
def set_wmbus_white_list(sn):
    try:
        white_list = request.get_json().get("white_list")
        lista = white_list.split(",")
        if len(lista) > 15:
            return make_response(jsonify({"message": "Error setting the wmbus white list. Devices on white list are limited to 15"}), 400)
        if sn[:5] == '5GWMB':
            device = mysqlGet_5G.getDeviceProperties("sn", sn)
            for l in lista:
                mysqlSet_5G.insertNewWhiteList(l, device["id"])
        elif sn[:5] == 'NIOTW' or sn[:5] == 'FC23C':
            mysqlSet_narrow.deleteWhiteList(sn)
            if lista:
                device = mysqlGet_narrow.getDeviceProperties("sn", sn)
                for l in lista:
                    mysqlSet_narrow.insertNewWhiteList(l, device["id"])
        return make_response(jsonify({"message": "White list set successfully"}), 200)
    except Exception as e:
        print(e)
        return make_response(jsonify({"message": "Error setting the wmbus white list"}), 400)


def server_run():
    app.run(host='0.0.0.0',port=8080)

if __name__ == "__main__":
    server_run()