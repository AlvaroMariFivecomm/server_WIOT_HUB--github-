import mysqlGet
import mysqlSet
import datetime
import requests
import json
from datetime import timedelta
import datetime

class jsonFormat:
    
    def __init__(self):
        json_data = {
            "5GLiteSn": "",
            "meterSn": "",
            "timestamp": "",
            "nbOfValidSlots": "",
            "slotDuration": "",
            "accumulatedVolume": [],
            "accumulatedReverseVolume": [],
            "currentFlow": [],
            "meterBatteryDaysLeft": [],
            "actualAmbientTemperature": [],
            "actualMediaTemperature": [],
            "alarms": [],
            "alarmDurations": [],
            "acousticNoise": [],
            "5gLiteConnectionData": [],
            "5gLiteBatteryActivated": "",
            "5gLiteBatteryLevel": ""
        }
        self.json_data = json_data
        self.url = ""
        self.bearer = ""
        self.headers = ""

    def send(self):
        json_send = json.dumps(self.json_data)
        self.bearer = "none"
        self.headers = {"accept": "application/json",
            "authorization": self.bearer, "content-type": "application/json"}
        response = requests.post(self.url, headers=self.headers, json=json_send)
        print(response)

    def set_value(self, key, value):
        self.json_data[key] = value
        
    def get_value(self, key):
        return self.json_data[key]
    
    def send_realTime(self, imei):
        try:
            data = mysqlGet.getDataToJSONRealTime(imei)
            self.json_data("accumulatedVolume",data["vol"])
            self.json_data("accumulatedReverseVolume",data["rev_vol"])
            self.json_data("currentFlow",data["flow"])
            self.json_data("meterBatteryDaysLeft",data["bat"])
            self.json_data("actualAmbientTemperature",data["tmp_amb"])
            self.json_data("actualMediaTemperature",data["tmp_med"])
            self.json_data("acousticNoise",data["acoustic"]) 
            self.json_data("timestamp",data['timestamp'])
            self.json_data("5gLiteBatteryActivated", "False")

            devProperties = mysqlGet.getDeviceProperties("imei", imei)
            self.json_data["5GLiteSn"] = devProperties["sn"]
            self.json_data["meterSn"] = mysqlGet.getArlarmProperty("meter_sn", "device_id", devProperties["id"])
            self.json_data["nbOfValidSlots"] = 1
            self.json_data["slotDuration"] = devProperties["reportTime"]
            self.json_data["alarms"] = list(setAlarms(mysqlGet.getAlarms(imei)).values())
            self.json_data["alarmDurations"] = [0] * 8
            self.json_data["5gLiteConnectionData"] = list(setCoverageRealTime(imei).values())
            self.json_data["5gLiteBatteryLevel"] = mysqlGet.getDeviceProperty("battery", imei)
            self.json_data["5gLiteBatteryLevel"] = False

        except Exception:  
            print("error")

    def send_JSON_timer(self, imei):
        try:
            dates = mysqlGet.getDatesTosend(imei)
            # reportTime = getDeviceProperty('reportTime',imei)
            reportTime = 86400
            fechas_dt = [datetime.datetime.strptime(fecha, '%Y-%m-%d %H:%M:%S') for fecha in dates]

            vectores_distintos = []
            vector_actual = []

            prev_date = None
            if dates:
                for date_str in dates:
                    date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                    if reportTime == 86400:
                        if prev_date is None:
                            vector_actual.append(date_str)
                        elif prev_date-date > timedelta(hours=1,minutes=10):
                            vectores_distintos.append(vector_actual)
                            vector_actual = []
                        else:
                            vector_actual.append(date_str)

                    elif reportTime == 43200:
                        if prev_date is None:
                            vector_actual.append(date_str)
                        elif date - prev_date > timedelta(minutes=32):
                            vectores_distintos.append(vector_actual)
                            vector_actual = []
                        else:
                            vector_actual.append(date_str)
                    elif reportTime == 21600:
                        if prev_date is None:
                            vector_actual.append(date_str)
                        elif date - prev_date > timedelta(minutes = 17):
                            vectores_distintos.append(vector_actual)
                            vector_actual = []
                        else:
                            vector_actual.append(date_str)
                    prev_date = date
                
                    # Agregar la última fecha al último vector actual
                vector_actual.append(fechas_dt[-1].strftime("%Y-%m-%d %H:%M:%S"))
                vectores_distintos.append(vector_actual)
            else:
                print('No hay datos para enviar')

            # Imprimir los vectores distintos
            for i, vector in enumerate(vectores_distintos):
                print(f'Vector {i+1}: {[fecha for fecha in vector]}')

            # Generamos un JSON por cada vector de fechas
            for vector in vectores_distintos:
                if len(vector) > 1:
                    js = jsonFormat()
                    data = mysqlGet.getDataToJSONnonRealTime(imei, vector[-2], vector[0])
                    js.set_value("accumulatedVolume",data["volume"])
                    js.set_value("accumulatedReverseVolume",data["reverse_volume"])
                    js.set_value("currentFlow",data["flow"])
                    js.set_value("meterBatteryDaysLeft",data["battery"])
                    js.set_value("actualAmbientTemperature",data["temp_amb"])
                    js.set_value("actualMediaTemperature",data["med_temp"])
                    js.set_value("acousticNoise",data["acoustic"])
                    js.set_value("timestamp", data['timestamp'][0].strftime("%Y-%m-%d %H:%M:%S")) 
                    js.set_value("5gLiteBatteryActivated", "False")
                    js.set_value("5gLiteBatteryActivated", "False")
                    
                    devProperties = mysqlGet.getDeviceProperties("imei", imei)
                    js.set_value("5GLiteSn", devProperties["sn"])
                    js.set_value("meterSn", mysqlGet.getArlarmProperty("meter_sn", "device_id", devProperties["id"]))
                    js.set_value("nbOfValidSlots",len(vector)/2)
                    js.set_value("slotDuration", devProperties["reportTime"])
                    js.set_value("alarms", list(setAlarms(mysqlGet.getAlarms(imei)).values()))
                    js.set_value("alarmDurations", [0] * 8)
                    js.set_value("5gLiteConnectionData", list(setCoverageRealTime(imei).values()))
                    js.set_value("5gLiteBatteryLevel", mysqlGet.getDeviceProperty("battery", imei))
                    js.set_value("5gLiteBatteryLevel", False)
                    print(js.json_data)
                    # js.send()

                    mysqlSet.updateSendMssgs(imei, dates)
                else:
                    print("Vector vacío")
        except Exception as ex:
            print(ex)

def setAlarms(alarms_db):
    alarms = {'dry':0, 'reverse':0, 'leak':0, 'burst':0,'tamper':0, 'lowBattery':0,
                'lowTemp':0, 'highTemp':0, 'V1AboveV4':0}
    if alarms_db:
        for alarm in alarms_db:
            alarms[alarm] = 1        
    return alarms

def setCoverageRealTime(imei):
    try:
        data = mysqlGet.getCoverageRealTime(imei)
        for d in data:
            if data[d] == None:
                data[d] = 0
    except Exception:
        print("Error getting coverage")    
    return data




                
