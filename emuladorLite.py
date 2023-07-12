import paho.mqtt.client as paho
import time

def liteMia():
    broker="172.16.1.4"
    port=1883
    def on_publish(client,userdata,result):             #create function for callback
        print("data published \n")
        pass
    client1= paho.Client("emulasion")                           #create client object
    client1.on_publish = on_publish                          #assign function to callback
    client1.username_pw_set("device", "device")
    client1.connect(broker,port)                                 #establish connection
    valor = 0
    while True:
        try:    
            if valor >= 3606:
                valor = 0
            sendString = "sMeter;1;-85,-14,-53,9,130,-32768,-32768,-32768;23725797;"
            sendString += "3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A2902000"
            sendString += "57E02463E02001AFE012B2501001BE4012425010028023E001BF90000"
            valor += 1
            sendString += "501BF900601BF9AB73,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F3280442"
            sendString += "00000000004A290200057E02463E02001AFE012B2501001BE40124250"
            sendString += "10028023E001BF90000501BF900601BF9AB73,3F100044280442"
            valor += 1
            sendString += "0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A290200057E02463E02001AF"
            sendString += "E012B2501001BE4012425010028023E001BF90000501BF900601BF9AB"
            valor += 1
            sendString += "73,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A2902"
            sendString += "00057E02463E02001AFE012B2501001BE4012425010028023E001BF90"
            valor += 1
            sendString += "000501BF900601BF9AB73,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F3280"
            sendString += "44200000000004A290200057E02463E02001AFE012B2501001BE40124"
            sendString += "25010028023E001BF90000501BF900601BF9AB73,3F100044280442"
            valor += 1
            sendString += "0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A290200057E02463E02001AFE" 
            sendString += "012B2501001BE4012425010028023E001BF90000501BF900601BF9AB7"
            valor += 1
            sendString += "3,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A29020"
            sendString += "0057E02463E02001AFE012B2501001BE4012425010028023E001BF90"
            valor += 1
            sendString += "000501BF900601BF9AB73,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F3280"
            sendString += "44200000000004A290200057E02463E02001AFE012B2501001BE40124"
            sendString += "25010028023E001BF90000501BF900601BF9AB73,3F100044280442"
            valor += 1
            sendString += "0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A290200057E02463E02001AFE"
            sendString += "012B2501001BE4012425010028023E001BF90000501BF900601BF9AB7"
            valor += 1
            sendString += "3,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A29020"
            sendString += "0057E02463E02001AFE012B2501001BE4012425010028023E001BF900"
            valor += 1
            sendString += "00501BF900601BF9AB73,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F32804"
            sendString += "4200000000004A290200057E02463E02001AFE012B2501001BE401242"
            sendString += "5010028023E001BF90000501BF900601BF9AB73,3F100044280442"
            valor += 1
            sendString += "0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A290200057E02463E02001AFE"
            sendString += "012B2501001BE4012425010028023E001BF90000501BF900601BF9AB7"
            valor += 1
            sendString += "3,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A29020"
            sendString += "0057E02463E02001AFE012B2501001BE4012425010028023E001BF900"
            valor += 1
            sendString += "00501BF900601BF9AB73,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F32804"
            sendString += "4200000000004A290200057E02463E02001AFE012B2501001BE401242"
            sendString += "5010028023E001BF90000501BF900601BF9AB73,3F100044280442"
            valor += 1
            sendString += "0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A290200057E02463E02001AFE"
            sendString += "012B2501001BE4012425010028023E001BF90000501BF900601BF9AB7"
            valor += 1
            sendString += "3,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A29020"
            sendString +="0057E02463E02001AFE012B2501001BE4012425010028023E001BF90000"
            valor += 1
            sendString += "501BF900601BF9AB73,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F3280442"
            sendString += "00000000004A290200057E02463E02001AFE012B2501001BE40124250"
            sendString += "10028023E001BF90000501BF900601BF9AB73,3F100044280442"
            valor += 1
            sendString += "0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A290200057E02463E02001AFE"
            sendString += "012B2501001BE4012425010028023E001BF90000501BF900601BF9AB7"
            valor += 1
            sendString += "3,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A29020"
            sendString += "0057E02463E02001AFE012B2501001BE4012425010028023E001BF900"
            valor += 1
            sendString += "00501BF900601BF9AB73,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F32804"
            sendString +="4200000000004A290200057E02463E02001AFE012B2501001BE4012425"
            sendString += "010028023E001BF90000501BF900601BF9AB73,3F100044280442"
            valor += 1
            sendString += "0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A290200057E02463E02001AFE"
            sendString += "012B2501001BE4012425010028023E001BF90000501BF900601BF9AB7"
            valor += 1
            sendString += "3,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A29020"
            sendString += "0057E02463E02001AFE012B2501001BE4012425010028023E001BF900"
            valor += 1
            sendString += "00501BF900601BF9AB73,3F100044280442"+"0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F32804"
            sendString += "4200000000004A290200057E02463E02001AFE012B2501001BE401242"
            sendString += "5010028023E001BF90000501BF900601BF9AB73,3F100044280442"
            sendString += "0"*(8-len(hex(valor).replace('0x','')))+hex(valor).replace('0x','').replace('1b','1c')+"00"+"F328044200000000004A290200057E02463E02001AFE"
            sendString += "012B2501001BE4012425010028023E001BF90000501BF900601BF9AB7"
            sendString += "3;0200001C1;274;37242"           
            ret= client1.publish("t351",sendString)  #publish
            time.sleep(1)
            
        except Exception as ex:
            print("as",ex)