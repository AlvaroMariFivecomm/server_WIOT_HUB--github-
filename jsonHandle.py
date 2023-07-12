    # /* ===================================================== 
    #
    # * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved. 
    #
    # * File_name jsonHandle
    #
    # * Description:  handles the output of the json messages througth http
    #
    # * Author:  Alejandro
    #
    # * Date:  25-08-21
    #
    # * Version:  1.33
    #
    # * =================================================== */ 

from exceptions import exceptionHandler
from requests.structures import CaseInsensitiveDict
from datetime import datetime
import requests
import json

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
        print("prepareData",ex)
    return volume,reverse,flow,meter_battery_days_left,actual_amb_temperature,actual_media_temperature,acoustic_noise 

def prepareDataAlarm(kamstrup_vector):
    try:
        alarms = kamstrup_vector[1]
        duration = kamstrup_vector[0]
        for i in range(len(alarms)):
                if(int(alarms[i])):
                    alarms[i] ="True"
                else:
                    alarms[i]="False"

        
    except Exception as ex:
        print("data Alarm error",ex)
    return alarms,duration

def dataToJSON(coat, dataBaseObject):
    try:
        coat.append(dataBaseObject)

    except Exception as ex:
        print("data Alarm error",ex)
    return coat 