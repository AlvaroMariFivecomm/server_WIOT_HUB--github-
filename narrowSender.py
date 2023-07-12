

import mysqlGet_narrow as mysqlGet
import mysqlSet_narrow as mysqlSet
import datetime
import requests
import json
from datetime import timedelta, datetime
import datetime

class narrowSender:


    def __init__(self, sn, fecha):

        json_data = {
            "current_whList_id": "",
            "data_send": {}            
        }
        self.json_data = json_data
        self.url = "http://192.168.0.141:8000"
        self.bearer = ""
        self.headers = ""
        self.sn = sn
        self.hour = 0
        self.fecha = fecha



    def get_json(self):
        self.setDeviceData()
        fechas = mysqlGet.getPendingSensorsId(self.sn, self.fecha)
        self.setData(self.divide_fechas(fechas))
        #print(json.dumps(self.json_data))
        return(json.dumps(self.json_data))

    def divide_fechas(self, datos):
        resultado = []
        acumulado = []
        for i, dato in enumerate(datos):
            if i == 0:
                acumulado.append([dato[0], dato[1], dato[2].strftime('%Y-%m-%d %H:%M:%S')])  # Agrega la primera fecha al acumulado
            else:
                fecha_actual = dato[2]
                fecha_anterior = datos[i-1][2]
                diferencia = fecha_actual - fecha_anterior
                
                if diferencia > timedelta(minutes=40):
                    resultado.append(acumulado)  # Agrega el acumulado actual al resultado
                    acumulado = []
                    aux = [dato[0], dato[1], dato[2].strftime('%Y-%m-%d %H:%M:%S')]  # Crea un nuevo acumulado con la fecha actual
                    acumulado.append(aux)
                else:
                    acumulado.append([dato[0], dato[1], dato[2].strftime('%Y-%m-%d %H:%M:%S')])  # Acumula la fecha actual al acumulado
        resultado.append(acumulado)  # Agrega el último acumulado al resultado
        return resultado
      
    def set_value(self, key, value):
        self.json_data[key] = value
        
    def get_value(self, key):
        return self.json_data[key]
    
    def setData(self, vector_fechas):
        try:
            if vector_fechas[0]:    
                if vector_fechas:
                    for fechas in vector_fechas:
                        if fechas:
                            self.json_data["data_send"][str(fechas[0][2])] = {
                                "data_whiteList": {},  # Nueva clave para almacenar todas las horas
                                "coverage": {},
                                "greyList": {}
                            }
                            for fecha in fechas:
                                data = mysqlGet.getData(fecha[0])
                                data_to_json = {"data": data["data"], "rssi": data["rssi"]}
                                hour = data["hour"]
                                sensor_id = data["data"][4:20]
                                self.json_data["data_send"][str(fechas[0][2])]["data_whiteList"].setdefault(hour, {})
                                self.json_data["data_send"][str(fechas[0][2])]["data_whiteList"][hour][sensor_id] = data_to_json
                            coverage = mysqlGet.getCoverageRealTime(self.sn)
                            self.json_data["data_send"][str(fechas[0][2])]["coverage"] = coverage
                            greyList = mysqlGet.getAllFromGreyListBySn(self.sn,fechas[0][2])
                            #Se añade greyList para esa hora y si no hay, para la anterior
                            if len(greyList) == 0:
                                datetime_obj = datetime.datetime.strptime(fechas[0][2], '%Y-%m-%d %H:%M:%S')
                                fecha_modificada = datetime_obj - timedelta(hours=1)
                                greyList = mysqlGet.getAllFromGreyListBySn(self.sn,fecha_modificada.strftime('%Y-%m-%d %H:%M:%S'))
                            self.json_data["data_send"][str(fechas[0][2])]["greyList"] = greyList
                        else:
                            #Se añade covertura y greyList si no hay datos
                            fecha = mysqlGet.getAllFromGreyListBySnNoDate(self.sn,self.fecha)
                            self.json_data["data_send"][str(fecha)] = {
                                "data_whiteList": "no data",  # Nueva clave para almacenar todas las horas
                                "coverage": {},
                                "greyList": {}
                            }
                            coverage = mysqlGet.getCoverageRealTime(self.sn)
                            self.json_data["data_send"][str(fecha)]["coverage"] = coverage
                            greyList = mysqlGet.getAllFromGreyListBySn(self.sn,fecha)
                            self.json_data["data_send"][str(fecha)]["greyList"] = greyList
            else:
                #Se añade covertura y greyList si no hay datos
                fecha = mysqlGet.getAllFromGreyListBySnNoDate(self.sn,self.fecha)
                self.json_data["data_send"][str(fecha)] = {
                    "data_whiteList": "no data",  # Nueva clave para almacenar todas las horas
                    "coverage": {},
                    "greyList": {}
                }
                coverage = mysqlGet.getCoverageRealTime(self.sn)
                self.json_data["data_send"][str(fecha)]["coverage"] = coverage
                greyList = mysqlGet.getAllFromGreyListBySn(self.sn,fecha)
                self.json_data["data_send"][str(fecha)]["greyList"] = greyList



        except Exception as e:
            print(e)

    def setDeviceData(self):
        try:
            whList = mysqlGet.getWhiteListBySn('sensor_id',self.sn)
            self.json_data["current_whList_id"] = whList
            # greyList = mysqlGet.getGreyListBySn('sensor_id', self.sn, self.fecha)
            # if not greyList:
            #     greyList = mysqlGet.getGreyListBySn('sensor_id', self.sn, datetime.datetime.now().strftime("%Y-%m-%d"))       
            #self.json_data["current_greyList_id"] = greyList
        except Exception as e:  
            print("error")
            print(e)


    def send(self):
        json_send = json.dumps(self.json_data)
        print(json_send)
        self.bearer = "none"
        self.headers = {"accept": "application/json",
            "authorization": self.bearer, "content-type": "application/json"}
        response = requests.post(self.url, headers=self.headers, json=json_send)
        print(response)
