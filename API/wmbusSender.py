import sys
import datetime
import requests
import json
from datetime import timedelta
import datetime
import mysqlGet_5G
import mysqlSet_5G
class wmbusSender:
    
    def __init__(self, imei):

        json_data = {
            "sn": "",
            "imsi": "",
            "coverage":[],
            "data": [
            ]
        }
        self.json_data = json_data
        self.url = "http://192.168.0.141:8000"
        self.bearer = ""
        self.headers = ""
        self.imei = imei

    def send(self):
        self.setDeviceData()
        sensors = mysqlGet_5G.getWhiteListBySn("sensor_id", self.get_value("sn"))
        for s in sensors:
            self.setSensor(s)
        self.send_json()
        
    def set_value(self, key, value):
        self.json_data[key] = value
        
    def get_value(self, key):
        return self.json_data[key]
    
    def get_json(self):
        self.setDeviceData()
        
        sensors = mysqlGet_5G.getWhiteListBySn("sensor_id", self.get_value("sn"))
        for s in sensors:
            self.setSensor(s)
        return json.dumps(self.json_data)

    def send_json(self):
        json_send = json.dumps(self.json_data)
        print(json_send)
        # self.bearer = "none"
        self.headers = {"accept": "application/json",
            "authorization": self.bearer, "content-type": "application/json"}
        response = requests.post(self.url, headers=self.headers, json=json_send)
        print(response)

    def setSensor(self, sensor_id):
        ids = mysqlGet_5G.getPendingDataBySensorId(sensor_id)
        data_to_json = {"sensor_id": sensor_id, "timestamp": [], "volume": [], "pressure": [], "temperature": [], 
                        "flow": []}
        if len(ids) != 0:
            for id in ids:
                date = mysqlGet_5G.getData(id)
                data_to_json["timestamp"].append(date["timestamp"].strftime("%Y-%m-%d %H:%M:%S"))
                data_to_json["volume"].append(date["volume"])
                data_to_json["pressure"].append(date["pressure"])
                data_to_json["temperature"].append(date["temp"])
                data_to_json["flow"].append(date["flow"])
                mysqlSet_5G.updateDataProperty(id, "sent", 1)
            self.json_data['data'].append(data_to_json)

    def setDeviceData(self):
        try:
            data = mysqlGet_5G.getDeviceProperties("imei", self.imei)
            self.set_value("sn",data["sn"])
            self.set_value("imsi", data["imsi"])
            self.setCoverage()   
        except Exception as e:  
            print("error")
            print(e)

    def setCoverage(self):
        try:
            data = mysqlGet_5G.getCoverageRealTime(self.imei)
        except Exception:
            print("Error getting coverage")    
        self.set_value("coverage", data)


