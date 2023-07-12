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
from datetime import *
from mysqlConnector import *
from narrowSender import narrowSender
from wmbusSender import *
from json_mobile import *

app = Flask(__name__)
CORS(app)
auth = HTTPBasicAuth()


@auth.verify_password
def verify_password(username, password):
    if username == 'api' and password == 'swagger':
        return True
    return False

@app.route("/en/a1/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def a1_en(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(JsonMobile(False, "Device not registered", []).get_json(), 400)
    if date["timestamp"] is None:
        return JsonMobile(False, "Connectivity: Failed", []).get_json()
    if date["timestamp"].date() == datetime.datetime.now().date():
        return JsonMobile(True, "Connectivity: OK", []).get_json()
    else:
        return JsonMobile(False, "Connectivity: Failed", []).get_json()

@app.route("/en/registro/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def registro_en(sn):
    try:
        imei = mysqlGet.getDeviceProperties("sn",sn)["imei"]
        imei = str(imei)
        if len(imei) == 14:
            imei = "0" + imei
    except Exception:
        return make_response(JsonMobile(False, "Error getting registry data", []).get_json(), 400)
    try:
        # mq = mysqlGet.getMqttProperties("txTopic", "t"+imei)
        mq = 'ok'
    except Exception:
        return JsonMobile(False, "MQTT topic has not been setted", []).get_json()
    if mq is None or mq == {}:
        return JsonMobile(False, "MQTT topic has not been setted", []).get_json()
    else:
        return JsonMobile(True, "MQTT topic successfully setted", []).get_json()
    
@app.route("/en/grey_list/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def grey_list_en(sn):
    try:
        date = mysqlGet.getGreyLisTToday(sn)
    except Exception as e:
        return make_response(JsonMobile(False, "Error getting the grey list", []).get_json(), 400)
    if date is None or len(date) == 0:
        return(JsonMobile(False, "Grey list not setted", []).get_json())
    else:
        return(JsonMobile(True, "Grey list setted. Devices found: ", len(date)).get_json())
    

@app.route("/en/white_list/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def white_list_en(sn):
    try:
        date = mysqlGet.getWhiteListBySn('sensor_id',sn)
    except Exception:
        return make_response(JsonMobile(False, "Error getting the white list", []).get_json(), 400)
    if date is None or len(date) == 0:
        return JsonMobile(False, "White list has not been received", []).get_json()
    else:
        return JsonMobile(True, "White list received: ", date).get_json()
    
    
@app.route("/en/coverage/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def coverage_en(sn):
    try:
        date = mysqlGet.getCoverageToday(sn)
    except Exception:
        return make_response(JsonMobile(False, "Error getting coverage", []).get_json(), 400)
    if len(date) == 0:
        return make_response(JsonMobile(False, "Coverage data has not been sent", []).get_json(), 400)
    if date["RSRP"] == '' and date["RSRQ"] == '':
        return JsonMobile(False, "Coverage data has not been received", []).get_json()
    else:
        return JsonMobile(True, "Signal quality: " + signal_quality(date["RSRP"], date["RSRQ"]), {"RSRP": date["RSRP"], "RSRQ": date["RSRQ"]}).get_json()
    
@app.route("/en/data/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def data_en(sn):
    try:
        date = mysqlGet.checkData(sn)
    except Exception:
        return make_response(JsonMobile(False, "Error checking data from device", []).get_json(), 400)
    if date:
        return JsonMobile(True, "Data received", []).get_json()
    else:
        return JsonMobile(False, "Data has not been received", []).get_json()
    
@app.route("/es/a1/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def a1_es(sn):
    try:
        date = mysqlGet.getDeviceProperties("sn", sn)
    except Exception:
        return make_response(JsonMobile(False, "Dispositivo no registrado", []).get_json(), 400)
    if date["timestamp"] is None:
        return JsonMobile(False, "Conectividad: Fallo", []).get_json()
    if date["timestamp"].date() == datetime.datetime.now().date():
        return JsonMobile(True, "Conectividad: OK", []).get_json()
    else:
        return JsonMobile(False, "Conectividad: Fallo", []).get_json()
    
@app.route("/es/registro/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def registro_es(sn):
    try:
        imei = mysqlGet.getDeviceProperties("sn",sn)["imei"]
        imei = str(imei)
        if len(imei) == 14:
            imei = "0" + imei
    except Exception:
        return make_response(JsonMobile(False, "Error al obtener la información de registro", []).get_json(), 400)
    try:
        # mq = mysqlGet.getMqttProperties("txTopic", "t"+imei)
        mq = 'ok'
    except Exception:
        return JsonMobile(False, "La conexión MQTT no se ha configurado correctamente", []).get_json()
    if mq is None or mq == {}:
        return JsonMobile(False, "La conexión MQTT no se ha configurado correctamente", []).get_json()
    else:
        return JsonMobile(True, "LA conexión MQTT se ha configurado correctamente", []).get_json()
    
    
@app.route("/es/grey_list/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def grey_list_es(sn):
    try:
        date = mysqlGet.getGreyLisTToday(sn)
    except Exception as e:
        return make_response(JsonMobile(False, "Error al obtener la grey list", []).get_json(), 400)
    if date is None or len(date) == 0:
        return(JsonMobile(False, "No se ha enviado grey list", []).get_json())
    else:
        return(JsonMobile(True, "Se ha recibido grey list. Dispositivos encontrados: ", len(date)).get_json())
    

@app.route("/es/white_list/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def white_list_es(sn):
    try:
        date = mysqlGet.getWhiteListBySn('sensor_id',sn)
    except Exception:
        return make_response(JsonMobile(False, "Error al obtener la white list", []).get_json(), 400)
    if date is None or len(date) == 0:
        return JsonMobile(False, "No se ha recibido white list", []).get_json()
    else:
        return JsonMobile(True, "Se ha recibido white list: ", date).get_json()
    
    
@app.route("/es/coverage/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def coverage_es(sn):
    try:
        date = mysqlGet.getCoverageToday(sn)
        if date["RSRP"] == '' and date["RSRQ"] == '':
            return JsonMobile(False, "No se han recibido datos de cobertura", []).get_json()
        else:
            return JsonMobile(True, "Calidad de la señal: " + signal_quality(date["RSRP"], date["RSRQ"], "es"), {"RSRP": date["RSRP"], "RSRQ": date["RSRQ"]}).get_json()
    except Exception:
        return make_response(JsonMobile(False, "Error al obtener los datos de cobertura", []).get_json(), 400)
    
@app.route("/es/data/<string:sn>", methods=["OPTIONS","GET"])
@auth.login_required

def data_es(sn):
    try:
        date = mysqlGet.checkData(sn)
    except Exception:
        return make_response(JsonMobile(False, "Error al obtener datos del dispositivo", []).get_json(), 400)
    if date:
        return JsonMobile(True, "Se han recibido datos del dispositivo", []).get_json()
    else:
        return JsonMobile(False, "No se han recibido datos del dispositivo", []).get_json()
    

def signal_quality(rsrp, rsrq, lang="en"):
    if lang == "en":
        if rsrp >= -80 and rsrq >= -10:
            return "Excellent"
        elif rsrp >= -90 and rsrq >= -15:
            return "Good"
        elif rsrp >= -100 and rsrq >= -20:
            return "Medium"
        else:
            return "Weak"
    else:
        if rsrp >= -80 and rsrq >= -10:
            return "Excelente"
        elif rsrp >= -90 and rsrq >= -15:
            return "Buena"
        elif rsrp >= -100 and rsrq >= -20:
            return "Media"
        else:
            return "Débil"



def server_run():
    app.run(host='0.0.0.0',port=8082)

if __name__ == "__main__":
    server_run()