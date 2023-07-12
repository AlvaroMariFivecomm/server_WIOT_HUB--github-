# /* =====================================================
# * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY
#   VERTICALS S.L. All rights reserved.
# * File_name scriptRecepcionV33
# * Description:  The main file of the mqtt platform
# * Author:  Alvaro
# * Date:  28-04-23
# * Version:  1.33
# * =================================================== */

import time
import os
import threading
# from mysql.connector.errors import Error
from datetime import datetime, timedelta
from paho.mqtt import client as mqtt_client
import jsonSender
import battery as bat
from datetime import *
import mysqlGet
import mysqlSet
from timeChecker import *
import mqttConect
from jsonFormat import jsonFormat
import replyLite
from binascii import unhexlify
import API
import API_mobile

# battery level cnt
minValue = 0
maxValue = 100
changeSpeed = 0.05
balanceCounter = 0
BROKER_INST = 'pablito'
topic = "#"  # topic for sub  of the mqtt suscriptor
BROKER_IP = '192.168.0.20'  # ip or domain of the broker
BROKER_PORT = 1883  # comunication port
# script credentials
BROKER_USR = 'device'
BROKER_PASS = 'device'

BATTERY_THRESHOLD = 0

BAT_REPORT_TIME = 86400
BAT_GRANULARITY = 3600
BAT_VALUES = [86400, 43200, 21700]


def parsePayloadToHexa(payload):
    frame = [hex(byte).split("0x")[1] for byte in payload]
    hex_data = ""
    for i in frame:
        if len(i) == 1:  # if the hex value is only one digit, add a 0 to the left
            i = "0" + i
        hex_data += i
    return hex_data

def parse_trama(trama):
    # Aseguramos que la trama tiene una longitud par
    assert len(trama) % 2 == 0

    # Dividimos la trama en bloques de dos caracteres
    bloques = [trama[i:i+2] for i in range(0, len(trama), 2)]

    datos_json = {}
    i = 0
    while i < len(bloques):
        # Extraemos la hora, el RSSI y la longitud de los datos
        hora = format(int(bloques[i], 16), '02d') 
        rssi = int(bloques[i+1], 16)
        longitud_datos = int(bloques[i+2], 16)

        # Aseguramos que la longitud de los datos no exceda el tamaño de la trama
        assert i + 3 + longitud_datos <= len(bloques)

        # Extraemos los datos
        datos = "".join(bloques[i+3:i+3+longitud_datos])
        # Convertimos los datos a enteros
        # datos = [int(bloque, 16) for bloque in datos]

        datos_json[f"hora{datos[4:20]}{hora}"] = {  
                        "RSSI": rssi,
                        "datos": datos
                    }

        i += 3 + longitud_datos

    return json.dumps(datos_json, indent=4)

def segundos_restantes(date):
    ahora = datetime.now()

    dia_siguiente = ahora + timedelta(days=1)
    fecha_siguiente = dia_siguiente.date()

    hora_siguiente = datetime.strptime(f'{fecha_siguiente} {date}', '%Y-%m-%d %H:%M')

    diferencia = hora_siguiente - ahora
    segundos_restantes = diferencia.total_seconds()

    return int(segundos_restantes)

def voltear_bytes(string):
    # Convertir el string en una lista de bytes
    bytes_list = [string[i:i+2] for i in range(0, len(string), 2)]
    # Invertir el orden de los bytes en la lista
    bytes_list.reverse()
    # Unir los bytes en un nuevo string y agregar un 0 al principio si es necesario
    new_string = ''.join(bytes_list)
    if len(new_string) % 2 == 1:
        new_string = '0' + new_string
    return new_string

def makingLogsDir(directory):
    if not os.path.isdir(directory):
        try:
            os.makedirs(directory)
        except OSError:
            pass

def loadImeis() -> list:
    return mysqlGet.getAllIMEIs()
    # return ["866897040488373", "866897040488374", "866897040153126"]

def defaultCase(msg):
    print("warning!")
    # print(msg.payload)
    # print(msg.topic)

def meassureToFloat(code: str, div: int) -> float:
    v_aux = []
    aux_str = ""
    for i in code:
        if len(aux_str) == 2:
            v_aux.append(aux_str)
            aux_str = ""
        aux_str += i
    v_aux.append(aux_str)
    v_aux = v_aux[::-1]
    value = "".join(str(v) for v in v_aux)
    return int(value, 16)/div

def frame50dec(subframe1, subframe2):
    is_crc_ok = crcCheck(subframe1)
    device_sn = str(subframe1[4:-4])
    alarms = str(subframe2[14:-4])
    binary_str = bin(int(alarms, 16))[2:].zfill(16)
    bits = [int(binary_str[i]) for i in range(len(binary_str))]
    bits = bits[::-1]
    data = {
        "crcBool": is_crc_ok,
        "deviceSN": device_sn,
        "alarms": {
            "dry": bits[0],
            "reverse": bits[1],
            "leak": bits[2],
            "burst": bits[3],
            "tamper": bits[4],
            "lowBattery": bits[5],
            "lowTemp": bits[6],
            "highTemp": bits[7],
            "V1AboveV4": bits[8],
            "internalError": bits[9],
            "tamperOnDisplay": bits[15]
        }
    }
    return data

def frame150dec(frame):
    frame = str(frame).replace('-', '')
    is_crc_ok = crcCheck(frame[8:])
    timestamp = int(frame[:8], 16)
    timestamp = datetime.fromtimestamp(timestamp)
    if frame[8:12] == "3f10":
        print("Everything is ok")
    data_order = ["volume", "reverseVolume", "flow",
                  "Batt", "TempAmb", "TempMedia", "Acoustic"]
    cont = 0
    data = {}
    frame_meas = frame[12:-4]
    while len(frame_meas) > 0:
        measure_type = frame_meas[:4]
        units = frame_meas[4:6]
        byteslen = frame_meas[6:8]
        sandexp = frame_meas[8:10]
        multipler = hexSignAndExpToMult(sandexp)
        value = frame_meas[10:10+int(byteslen, 16)*2]
        value = int(value, 16) * multipler
        frame_meas = frame_meas[10+int(byteslen, 16)*2:]
        data[data_order[cont]] = [measure_type,
                                  units, byteslen, multipler, value]
        if len(frame_meas) < 12:
            break
        cont += 1
    data = {
        "crcBool": is_crc_ok,
        "meassure": data_order,
        "data": data,
        "timestamp": timestamp
    }
    return data

def hexSignAndExpToMult(hex: str) -> float:
    sandexp = bin(int(hex, base=16)).replace("0b", "").zfill(16)[::-1]
    multipler = pow(-1, (int(sandexp[7], 2))) * pow(10,
                                                    (pow(-1, (int(sandexp[6], 2)))*int(sandexp[:5][::-1], 2)))
    return multipler

def crcCheck(frame: str) -> bool:
    crc = frame[-4:]
    sub = frame[:-4]
    crc16 = crcmod.predefined.Crc('xmodem')
    crc16.update(unhexlify(sub))
    return crc16.hexdigest().lower() == crc.lower()

def calculate_crc16_xmodem(data):
    crc = 0
    for b in data:
        crc = crc ^ b << 8
        for i in range(8):
            if crc & 0x8000:
                crc = crc << 1 ^ 0x1021
            else:
                crc = crc << 1
    return crc & 0xFFFF

def calcular_porcentaje_bateria(voltaje_actual):
    voltaje_minimo = 3.27
    voltaje_maximo = 4.2
    rango_voltaje = voltaje_maximo - voltaje_minimo
    porcentaje_bateria = ((voltaje_actual - voltaje_minimo) / rango_voltaje) * 100
    return max(min(porcentaje_bateria, 100), 0)

def frameSelectorType(payload) -> str:
    # The current frame types are:
    # - sigtec -> str
    # - wm_bus, 50_bytes, 150_bytes -> bytes
    try:
        dec_payload = payload.decode()
    except:  # If it's not a string, it's bytes tuple
        dec_payload = ""
    a = dec_payload[:6]
    if dec_payload[:6] == "sigtec":
        frame_type = "sigtec"
    elif dec_payload[:8] == "greylist":
        frame_type = "greyList"
    else:  # All frames inside this else are bytes, so we need to parse them
        # hex_data = parsePayloadToHexa(payload)
        hex_data = payload
        print(len(hex_data))
        print(hex_data)
        a = len(hex_data)
        frame_type = "wm_bus"
    print("Frame type: " + frame_type)
    return frame_type

def json_real_time(imei):
    js = jsonFormat()
    js.send_realTime(imei)

def getWhiteList(imei):
    device = mysqlGet.getDeviceProperties("imei", imei)
    return mysqlGet.getWhiteListBySn("sensor_id", device["sn"])


def subscribe(client: mqtt_client, vectorIMEIs: list):
    def on_message(client, userdata, msg: mqtt_client.MQTTMessage):
        nonlocal vectorIMEIs
        if msg.topic == "a1":
            # Replace here New function check IMEI dev_imeicheck
            if msg.payload.decode().split(";")[0] in vectorIMEIs:
                print("Device already registered")
                
                usr, passwrd = mysqlGet.getUserAndPassFromImei(
                    msg.payload.decode().split(";")[0])
                imsi = msg.payload.decode().split(";")[1]
                fw = msg.payload.decode().split(";")[3]
                hw = msg.payload.decode().split(";")[2]
                apn = msg.payload.decode().split(";")[4]

                mysqlSet.updateDevice_property(msg.payload.decode().split(";")[0],"imsi",imsi)
                mysqlSet.updateDevice_property(msg.payload.decode().split(";")[0],"fw",fw)
                mysqlSet.updateDevice_property(msg.payload.decode().split(";")[0],"hw",hw)
                mysqlSet.updateDevice_property(msg.payload.decode().split(";")[0],"apn",apn)
                hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                mysqlSet.updateDevice_property(msg.payload.decode().split(";")[0],"timestamp",hora)

                client = replyLite.send("a1",
                                        [msg.payload.decode().split(";")[0], usr, passwrd, None], client)
            else:
                print("Device NOT registered")
        elif msg.topic[0] == "t" and msg.topic[1:] in vectorIMEIs:
            print("\033[96m=========================================\033[0m")
            frame_type = frameSelectorType(msg.payload)
            if frame_type == "sigtec":
                try:
                    frame_battery = msg.payload.decode().split(";")[-2]
                    frame_coverage = msg.payload.decode().split(";")[-3]
                    wmbMode = msg.payload.decode().split(";")[-1]

                    if wmbMode != '0':
                        mysqlSet.updateDevice_property(msg.topic[1:], "wmb_mode", '3')
                    else:
                        mysqlSet.updateDevice_property(msg.topic[1:], "wmb_mode", '0')

                    properties = mysqlGet.getDeviceProperties('imei',msg.topic[1:])
                    
                    valores = frame_coverage.split(" ")
                    resultados = {}
                    for valor in valores:
                        if ":" in valor:
                            clave, valor = valor.split(":")
                            resultados[clave.strip()] = valor.strip()

                    client = replyLite.send(
                            "sigtech", [msg.topic[1:], properties['reportTime'], properties['wmb_measurement_window'], 
                                        properties['wmb_mode'], properties['fota'], properties['manufacturer'], properties['vertical']], client)
                    
                    mysqlSet.updateDevice_property(msg.topic[1:], 'last_mssg_send', datetime.now())
                    mysqlSet.updateDevice_property(msg.topic[1:], "fota", "None")
                    mysqlSet.updateDevice_property(msg.topic[1:], "battery", frame_battery)
                    mysqlSet.insertCoverage(
                            msg.topic[1:], resultados)
                except Exception as e:
                    print(e)
                    print('error en el sigtec')

            elif frame_type == "greyList":
                try:
                    list =  msg.payload.decode().split(";")[1].split(',')[:-1]
                    for i in list:
                        str_id = i[:-4]
                        rssi = int(i[-4:-2],16) * -1
                        average = int(i[-2:],16)
                        mysqlSet.insertSensorToGraylist(msg.topic[1:], average, rssi, str_id)
                    white_list = getWhiteList(msg.topic[1:])
                    white_list.insert(0, msg.topic[1:])
                    if len(white_list) > 1:
                        mysqlSet.updateDevice_property(msg.topic[1:], 'last_mssg_send', datetime.now())
                        client = replyLite.send(
                            "whitelist", white_list, client)
                    else:
                        mysqlSet.updateDevice_property(msg.topic[1:], 'last_mssg_send', datetime.now())
                        client = replyLite.send(
                            "empty_whitelist", msg.topic[1:], client)
                        print("no hay white list")
                except Exception as e:
                    print(e)
                    print('error en la greyList')
                    client = replyLite.send(
                            "whitelist", white_list, client)
                               
            elif frame_type == "wm_bus":
                try:
                    trama_json = parse_trama(msg.payload.decode().split(";")[1])
                    date_backup = msg.payload.decode().split(";")[0]
                    # print(trama_parseada)
                    print(datetime.now() + timedelta(hours=2))
                    print(trama_json)
                    for hora, valores in json.loads(trama_json).items():
                        rssi = valores['RSSI']
                        datos = valores['datos']
                        mysqlSet.insertDates(msg.topic[1:], datos[4:20], hora[-2:], datos, rssi, date_backup)
                    mysqlSet.updateDevice_property(msg.topic[1:], 'last_mssg_send', datetime.now())
                    client.publish("r"+msg.topic[1:], "{ACK}")
                except Exception as e:
                    print(e)
                    print('error en la trama de datos')
            else:
                print("Unknown frame type")
            gateway_id = msg.topic[0:len(msg.topic)]
            # except Exception as ex:
            #     textToWrite = b"Error decoding the topic message\n", ex
            #     exceptions.exceptionHandler(textToWrite)
            print("\033[96m=========================================\033[0m")
        elif msg.topic[0] == "r" and msg.topic[1:] in vectorIMEIs:
            print("My message")
            print("info: " + str(msg.info) + " | mid: " + str(msg.mid) +
                  " | qos: " + str(msg.qos) + " | retain: " + str(msg.retain) +
                  " | state: " + str(msg.state) + " | timestamp: " + str(msg.timestamp))
        else:
            defaultCase(msg)
    client.subscribe(topic)
    client.on_message = on_message

def run():
    directory = "logs"  # directory for the logs
    makingLogsDir(directory)
    while True:
        try:
            client = mqttConect.mqttConect(
                BROKER_INST, BROKER_IP, BROKER_PORT, BROKER_USR, BROKER_PASS)
            vectorIMEIs = loadImeis()
            # mysqlSet.insertMQTT_properties(866897040488373,"device","device","t866897040488373","r866897040488373")
            # user, password = mysqlGet.getUserAndPassFromImei(866897040488373)
            # connects to broker, deberiamos cambiar el nombre de la fnc
            subscribe(client, vectorIMEIs)
            #replyLite.pubTest(client)
            client.loop_forever()
        except KeyboardInterrupt:
            res = input("Ctrl-c was pressed. Do you really want to exit? y/n ")
            if res == 'y':
                exit(0)
        except Exception as ex:
            logFile = open("logs/backendErrors.txt", "ab")
            cDate = datetime.utcnow()
            stringDate = str(cDate.year) + "-" + str(cDate.month) + "-" + str(cDate.day) + \
                " " + str(cDate.hour) + ":" + str(cDate.minute) + \
                ":" + str(cDate.second)
            textToWrite = b"Principal thread error\n"+str.encode(str(ex))
            logFile.write(bytes(stringDate, 'ascii') +
                          bytes(" : ", 'ascii')+textToWrite)
            logFile.close()
            print(textToWrite.decode('ASCII'))

def main_run():
    threads = []
    x = threading.Thread(target=run)
    threads.append(x)
    for i in threads:
        i.start()


if __name__ == "__main__":
    main_run()
