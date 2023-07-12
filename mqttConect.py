"""
    /* ===================================================== 
    * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved. 
    * File_name scriptRecepcionV33
    * Description:  The main file of the mqtt platform
    * Author:  Alejandro
    * Date:  25-08-21
    * Version:  1.33
    * =================================================== */ 
"""
from paho.mqtt import client as mqtt_client
import exceptions
import mqttConect

client_id = f'server-mqtt-{1}' 

def mqttConect(instance, ip, port, user, passwd):
    # function to obtain the mqtt client
    # input: instance -> string
    # output: volume2Liters -> integer
    broker = '192.168.0.39' #ip or domain of the broker
    port = 1883 #comunication port
    user = "device"
    passwd = "device"
    client = mqtt_client.Client(instance) #create new instance
    client.username_pw_set(user, passwd)
    client.connect(broker, port) #connect to broker
    return client
