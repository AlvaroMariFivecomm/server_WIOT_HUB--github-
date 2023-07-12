    # /* =====================================================
    #
    # * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved.
    #
    # * File_name jsonSender
    #
    # * Description:   handles the output of the json messages througth http
    #
    # * Author:  Alejandro
    #
    # * Date:  25-08-21
    #
    # * Version:  1.5
    #
    # * =================================================== */

from exceptions import exceptionHandler
from requests.structures import CaseInsensitiveDict
from datetime import datetime
import requests
import threading
import time
# from flask import Flask, jsonify, request
# from flasgger import Swagger


class JSON_Sender:

    def __init__(self, urlAguas, bearerAguas):
        self.url = "http://192.168.0.32:5000/"
        self.bearer = "none"
        self.headers = {"accept": "application/json",
            "authorization": self.bearer, "content-type": "application/json"}

    def get_url(self):
        return self.url

    def set_url(self, url):
        self.url = url

    def get_bearer(self):
        return self.bearer

    def set_bearer(self, bearer):
        self.bearer = bearer

    def get_headers(self):
        return self.headers

    def set_headers(self, new_headers):
        self.headers = new_headers

    def prepareData(kamstrup_vector):
        volume = []
        flow = []
        actual_amb_temperature = []
        actual_media_temperature = []
        meter_battery_days_left = []
        reverse = []
        acoustic_noise = []
        try:
            for iterator in kamstrup_vector:
                volume.append(iterator[0])
                reverse.append(iterator[1])
                flow.append(iterator[2])
                meter_battery_days_left.append(iterator[3])
                actual_amb_temperature.append(iterator[4])
                actual_media_temperature.append(iterator[5])
                # print(iterator[6])
                # acoustic_noise.append(iterator[6])
        except Exception as ex:
            print("prepareData", ex)
        return volume, reverse, flow, meter_battery_days_left, actual_amb_temperature, actual_media_temperature, acoustic_noise

    def prepareDataAlarm(kamstrup_vector):
        try:
            alarms = kamstrup_vector[1]
            duration = kamstrup_vector[0]
            for i in range(len(alarms)):
                    if (int(alarms[i])):
                        alarms[i] = "True"
                    else:
                        alarms[i] = "False"

        except Exception as ex:
            print("data Alarm error", ex)
        return alarms, duration

    def sendJsonToAll(self, lite_serial, kamstrup_serial, Ctimestampv, kamstrup_vector, connection_data, battery, alarm):
        v_ceros = [0] * 24
        # idrica
        urlAguas = JSON_Sender.get_url
        # urlAguas = "https://api01-pn5g.go-aigua.com/loadReadings"
        bearerAguas = JSON_Sender.get_bearer
        # bearerAguas = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRw"
        # bearerAguas += "czovL25pZmktcG41Zy5nby1haWd1YS5jb20vIiwiaWF0IjoxNjMxMDg3OTkx"
        # bearerAguas += "LCJleHAiOjI1Nzc3NzI3OTEsImF1ZCI6ImZpdmVjb21tIiwic3ViIjoiIn0."
        # bearerAguas += "pKj8Si9DCgzSzS5yN7OCeY3s6wjmQh-lvPb5o7zsu28"
        # headers["Authorization"] = "hhola"

        # send_json_basic(lite_serial,Ctimestampv,kamstrup_vector[0:24],connection_data,battery,\
        # alarm,urlAguas,bearerAguas)
        #    # # Neom
        # urlNeom = "https://"
        # bearer_neom = 'Bearer'
        # send_json_basic(lite_serial,Ctimestampv,vPulses,connection_data,battery,alarm,urlNeom,bearer_neom)
        # pruebas
        alarms = alarm + [0]*8

        sender = threading.Thread(target=self.send_json_kamstrup, args=(lite_serial,
        kamstrup_serial, Ctimestampv, kamstrup_vector[0:24],
        connection_data, battery, v_ceros, v_ceros,
        v_ceros, urlAguas, bearerAguas, v_ceros,
        v_ceros, v_ceros[0:3], alarms, v_ceros[0:7]))
        sender.start()

    def sendJsonKamstrupToAll(self, lite_serial, kamstrup_serial, Ctimestampv, kamstrup_vector, connection_data, battery, alarm):

        actual_media_temperature = []
        actual_amb_temperature = []
        volume, reverse, flow, meter_battery_days_left, actual_amb_temperature,\
        actual_media_temperature, acoustic_noise = self.prepareData(
            kamstrup_vector)
        alarms, alarm_durations = self.prepareDataAlarm(alarm)
        # idrica
        urlAguas = "https://api01-pn5g.go-aigua.com/loadReadings"
        bearerAguas = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRw"
        bearerAguas += "czovL25pZmktcG41Zy5nby1haWd1YS5jb20vIiwiaWF0IjoxNjMxMDg3OTkx"
        bearerAguas += "LCJleHAiOjI1Nzc3NzI3OTEsImF1ZCI6ImZpdmVjb21tIiwic3ViIjoiIn0."
        bearerAguas += "pKj8Si9DCgzSzS5yN7OCeY3s6wjmQh-lvPb5o7zsu28"
        # headers["Authorization"] = "hhola"
        self.send_json_kamstrup(lite_serial, kamstrup_serial, Ctimestampv, volume,
        connection_data, battery, reverse, flow,
        meter_battery_days_left, urlAguas, bearerAguas, actual_amb_temperature,
        actual_media_temperature, acoustic_noise, alarms, alarm_durations)

        # # Neom
        # urlNeom = "https://"
        # bea8rer_neom = 'Bearer'
        # send_json_basic(lite_serial,Ctimestampv,vPulses,connection_data,battery,alarm,urlNeom,bearer_neom)
        # pruebas

    def wmBus(vtmp):
        url = "https://api01-pn5g.go-aigua.com/loadReadings"
        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL25pZmktcG41Zy5nby1haWd1YS5jb20vIiwiaWF0IjoxNjMxMDg3OTkxLCJleHAiOjI1Nzc3NzI3OTEsImF1ZCI6ImZpdmVjb21tIiwic3ViIjoiIn0.pKj8Si9DCgzSzS5yN7OCeY3s6wjmQh-lvPb5o7zsu28"
        # headers["Authorization"] = "hhola"
        headers["Content-Type"] = "application/json"
        serialNumber =  "5GLITEA0000994122"
        Ctimestampv = time.time()
        vPulses = [0] * 24
        vPulses[0] = vtmp
        alarm = [0,0,0]
        connectionData = [0,0,0,0,0]
        string = f'"sn": "{serialNumber}","timestamp": "{Ctimestampv}","waterMeterReading": {vPulses[0:24]},"connectionData": {connectionData},"battery": {int(battery)},"alarm": {alarm}'
        data = "{"+string.replace("'", "")+"}"
        print(data)# coment to dont send JSON to the server post
        resp = requests.post(url, headers=headers, data=string)
        print(resp.status_code)

    def send_json_basic(serialNumber, Ctimestampv, vPulses, connection_data, battery, alarm_kamstrup_payload, url, bearer):
        try:

            bateryActivated=True
            if battery > 40000:  # if is higher the battery is not active
                bateryActivated=False
                battery=0


            string=f'"sn": "{serialNumber}","timestamp": "{Ctimestampv}",'
            string += f'"externalDataLength":{len(vPulses)},"waterMeterReading":'
            string += f'{vPulses[0:len(vPulses)]},"connectionData": {connection_data},'
            string += f'"batteryActivated":{bateryActivated},"battery": {int(battery)},'
            string += f'"alarm": {alarm_kamstrup_payload}'

            data="{"+string.replace("'", "")+"}"
            print(data)
            # coment to dont send JSON to the server post
            # resp = requests.post(url, headers=headers, data=data, verify='/path/to/public_key.pem')
            resp=requests.post(url, headers=headers, data=data,
                               verify='/path/to/public_key.pem')
            # coment to dont send JSON to the server get

            # added
            JsonFile=open("logs/backendSends.txt", "ab")
            cDate=datetime.utcnow()
            stringDate=str(cDate.year) + "-" + str(cDate.month) + "-" + str(cDate.day) + \
                           " " + str(cDate.hour) + ":" + \
                                     str(cDate.minute) + ":" + \
                                         str(cDate.second)
            JsonFile.write(bytes(stringDate, 'ascii')+bytes(" : ",
                           'ascii')+bytes(data, 'ascii')+bytes("\n", 'ascii'))
            JsonFile.close()
            # added
            print("Sended_ourEnd:")
            # print(resp.status_code)
        except Exception as ex:
            textToWrite=b"Error making the JSON\n"
            exceptionHandler(textToWrite)


    def send_json_kamstrup(self, lite_serial, kamstrup_serial, Ctimestampv, vPulses,\
        connection_data, battery, reverse, current_flow,\
        meter_battery_days_left, url, bearer, actual_ambient_temperature,\
        actual_media_temperature, acoustic_noise, alarms, alarm_durations):

        try:
            sender=JSON_Sender(self.url, self.bearer)
            headers=sender.get_headers()  # Obtener los encabezados actuales
            # Modificar el valor del encabezado "Accept"
            headers["accept"]="application/json"
            # Actualizar los encabezados en la instancia
            sender.set_headers(headers)

            # Modificar el valor del encabezado "Accept"
            headers["authorization"]=bearer
            # Actualizar los encabezados en la instancia
            sender.set_headers(headers)

            # Modificar el valor del encabezado "Accept"
            headers["content-Type"]="application/json"
            # Actualizar los encabezados en la instancia
            sender.set_headers(headers)

        except Exception as ex:
            textToWrite=b"Error setting the headers\n"
            exceptionHandler(textToWrite)
        try:
            bateryActivated=True
            if battery > 40000:  # if is higher the battery is not active
                bateryActivated=False
                battery=0
            nb_of_valid_slots=len(vPulses)
            if acoustic_noise == []:  # TO DO_ Delete when accoustic is updated
                acoustic_noise=[0, 0, 0, 0]
            string=f'"5GLiteSn":{lite_serial},"meterSn":{kamstrup_serial},'
            string += f'"timestamp":"{Ctimestampv}","nbOfValidSlots":{nb_of_valid_slots},'
            string += f'"accumulatedVolume": {vPulses[0:len(vPulses)]},'
            string += f'"accumulatedReverseVolume":{reverse},"currentFlow":{current_flow},'
            string += f'"meterBatteryDaysLeft":{meter_battery_days_left},'
            string += f'"actualAmbientTemperature":{actual_ambient_temperature},'
            string += f'"actualMediaTemperature":{actual_media_temperature},"alarms:"{str(alarms).replace("0","false")},'
            string += f'"alarmDurations":{alarm_durations}, "acousticNoise":{acoustic_noise},'
            string += f'"5gLiteConnectionData":{connection_data},'
            string += f'"5gLiteBatteryActivated":{bateryActivated},'
            string += f'"5gLiteBatteryLevel": {int(battery)}'

            data="{"+string.replace("'", "")+"}"
            print(data)
        except Exception as ex:
            textToWrite=b"Error making the json\n"
            exceptionHandler(textToWrite)
        try:
            # coment to dont send JSON to the server post
            resp=requests.post(url, headers=headers, data=data, timeout=0.1)
            # resp = requests.post(url, headers=headers, data=data, verify='/path/to/public_key.pem')
            # coment to dont send JSON to the server get
            # added
            JsonFile=open("logs/backendSends.txt", "ab")
            cDate=datetime.utcnow()
            stringDate=str(cDate.year) + "-" + str(cDate.month) + "-"
            stringDate += str(cDate.day) + " " + str(cDate.hour) + ":"
            stringDate += str(cDate.minute) + ":" + str(cDate.second)
            JsonFile.write(bytes(stringDate, 'ascii')+bytes(" : ",
                           'ascii')+bytes(data, 'ascii')+bytes("\n", 'ascii'))
            JsonFile.close()
            # added
            print("Sended_ourEnd:")
            print(resp.status_code)
        except Exception as ex:
            textToWrite=b"Error sending the JSON\n"
            exceptionHandler(textToWrite)

if __name__ == "__main__":
    url_pruebas="http://localhost:8000"
    bearer_neom='Bearer esteEsMiBearer'
    lite_serial='F0001'
    kamstrup_serial=12123123
    Ctimestampv=datetime.utcnow()
    vPulses=['12312323']
    connection_data=[33, 44, 55]
    battery=30000
    alarm_kamstrup_payload="none"
    reverse=['9']
    current_flow=['99999999']
    meter_battery_days_left=[9]
    actual_ambient_temperature=[-40]
    actual_media_temperature=[-55]
    acoustic_noise=[0, 0, 0]
    alarms=[0, 0, 0, 0]
    alarm_durations=[0, 0, 0, 0]
    JSON_Sender.send_json_kamstrup(lite_serial, kamstrup_serial, Ctimestampv, vPulses,\
    connection_data, battery, alarm_kamstrup_payload, reverse, current_flow,\
    meter_battery_days_left, url_pruebas, bearer_neom, actual_ambient_temperature,\
    actual_media_temperature, acoustic_noise, alarms, alarm_durations)

# swagger = Swagger(app)
# app.route('/send_json_options/<any:obj>', methods=['GET'])
