import exceptions
from mysqlConnector import *
import mysqlGet_5G
ip = "172.17.0.4"
db = 'WMB_5g_db'

def updateDevice_property(imei,column,value):
    try:
        tmp = mysqlConnect(id,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = "UPDATE DEVICE_PROPERTIES SET "+ column +" = %s where imei = %s"
        val = (value, imei) 
        myCursor.execute(sql, val)
        
    except Exception as ex:
        textToWrite = b"Error updating values on DEVICE_PROPERTIES\n",ex
        exceptions.exceptionHandler(textToWrite)
    
    mysqlClose(mydbConnector,myCursor)

def updateGreyListProperty(sensor_id, column, value):
    try:
        tmp = mysqlConnect(id)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = "UPDATE GREY_LIST SET " + column + " = %s where sensor_id = %s"
        val = (value, sensor_id) 
        myCursor.execute(sql, val)
        if myCursor.rowcount == 0:
            raise ValueError("No se ha actualizado ninguna fila")
    except Exception as ex:
        textToWrite = b"Error updating values on DEVICE_PROPERTIES\n",ex
        exceptions.exceptionHandler(textToWrite)
    
    mysqlClose(mydbConnector,myCursor)

def updateDataProperty(id,column,value):
    try:
        tmp = mysqlConnect(id)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = "UPDATE DATA SET " + column + " = %s where id = %s"
        val = (value, id) 
        myCursor.execute(sql, val)
        if myCursor.rowcount == 0:
            raise ValueError("No se ha actualizado ninguna fila")
        
    except Exception as ex:
        textToWrite = b"Error updating values on DEVICE_PROPERTIES\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)    

def insertNewDevice(imei, networked, reportTime):
    try:
        # Insertar los valores en device_properties
        tmp = mysqlConnect(id)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """INSERT INTO DEVICE_PROPERTIES (imei, networked, timestamp, reportTime) 
                SELECT %s, %s, NOW, %s, %s"""
        val = (imei, networked, reportTime)
        myCursor.execute(sql, val)

        
    except Exception as ex:
        textToWrite = b"Error inserting values on MQTT_PROPERTIES\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)

def insertNewWhiteList(sensor_id):
    sensor = mysqlGet_5G.getGreyListSensorById(sensor_id)

    try:
        tmp = mysqlConnect(id)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """INSERT INTO WHITE_LIST (manufacturer, model, vertical, device_id, sensor_id) 
                VALUES (%s, %s, %s, %s, %s)"""
        val = (sensor["manufacturer"], sensor["model"], sensor["vertical"], int(sensor["device_id"]), sensor_id)
        myCursor.execute(sql, val)

        
    except Exception as ex:
        textToWrite = b"Error inserting values on MQTT_PROPERTIES\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)

def insertMQTT_properties(imei, user, password, txTopic, rxTopic):
    try:
        # Insertar los valores en mqtt_properties
        tmp = mysqlConnect(id)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """INSERT INTO MQTT_PROPERTIES (txTopic, rxTopic, user, password, device_id) 
                SELECT %s, %s, %s, %s, dp.id FROM DEVICE_PROPERTIES dp WHERE dp.imei = %s"""
        val = (txTopic, rxTopic, user, password, imei)
        myCursor.execute(sql, val)

        
    except Exception as ex:
        textToWrite = b"Error inserting values on MQTT_PROPERTIES\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)


def insertDates(imei, timestamp, volume, preassure, temperature, flow, sensor_id ):
    try:
        # Obtener el ID de la fila en device_properties utilizando un JOIN
        tmp = mysqlConnect(id)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """INSERT INTO DATA (timestamp, volume, preassure, temperature, flow, sensor_id, device_id, received) 
                VALUES( %s, %s, %s, %s, %s, %s,
                        (SELECT dp.id FROM DEVICE_PROPERTIES dp WHERE dp.imei = %s),
                         NOW())"""
        val = (timestamp, volume, preassure, temperature, flow, sensor_id, imei)
        myCursor.execute(sql, val)

        
    except Exception as ex:
        textToWrite = b"Error inserting values on DATA\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)

def insertAlarm(imei, valores_alarmas):
    try:
        # Obtener el ID de la fila en device_properties utilizando un JOIN
        tmp = mysqlConnect(id)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        
        for clave, valor in valores_alarmas.items():
            if valor == 1:
                sql = """INSERT INTO ALARM_HISTORY (device_id, alarm_id, timestamp) 
                        VALUES (
                                (SELECT id FROM DEVICE_PROPERTIES WHERE imei = %s),
                                (SELECT id FROM ALARMS WHERE type = %s),
                                (NOW())
                                );"""
                val = (imei, clave)
                myCursor.execute(sql, val)

        
    except Exception as ex:
        textToWrite = b"Error inserting values on DATA\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)

def insertCoverage(imei, mode, valores_cobertura):
    try:
        # Obtener el ID de la fila en device_properties utilizando un JOIN
        tmp = mysqlConnect(id)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        if mode == '0':
            sql = """INSERT INTO COVERAGE (MCC, MNC, CELLID, PCID, BAND, NRDLBAND, RSRP, RSRQ, SINR, device_id, timestamp, mode) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s
                    ,(SELECT id FROM DEVICE_PROPERTIES WHERE imei = %s)
                    ,NOW()
                    ,%s
                    );"""
            val = (valores_cobertura["MCC"], valores_cobertura["MNC"], valores_cobertura["CELLID"], valores_cobertura["PCID"], 
                    valores_cobertura["BAND"], valores_cobertura["NRDLBAND"], valores_cobertura["RSRP"], valores_cobertura["RSRQ"], valores_cobertura["SINR"],
                    imei, mode)
            myCursor.execute(sql, val)

        elif mode == '1':
            sql = """INSERT INTO COVERAGE (MCC, MNC, CELLID, PCID, FREQBW, ULBW, DLBW, RSRP, RSRQ, SINR, TXPOWER, device_id, timestamp, mode) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ,(SELECT id FROM DEVICE_PROPERTIES WHERE imei = %s)
                    ,NOW()
                    ,%s
                    );"""
            val = (valores_cobertura["MCC"], valores_cobertura["MNC"], valores_cobertura["CELLID"], valores_cobertura["PCID"], 
                    valores_cobertura["FREQBW"], valores_cobertura["ULBW"], valores_cobertura["DLBW"], valores_cobertura["RSRP"], 
                    valores_cobertura["RSRQ"], valores_cobertura["SINR"], valores_cobertura["TXPOWER"], imei, mode)
            myCursor.execute(sql, val)

            if valores_cobertura["MCC1"] != None and valores_cobertura["MNC1"] != None:
                sql = """INSERT INTO COVERAGE (MCC, MNC, PCID, RSRP, SINR, RSRQ, BAND, NRDLBW, device_id, timestamp, mode) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s
                        ,(SELECT id FROM DEVICE_PROPERTIES WHERE imei = %s)
                        ,NOW()
                        ,%s
                        );"""
                val = (valores_cobertura["MCC1"], valores_cobertura["MNC1"], valores_cobertura["PCID1"], valores_cobertura["RSRP1"],
                        valores_cobertura["SINR1"], valores_cobertura["RSRQ1"], valores_cobertura["BAND1"], valores_cobertura["NRDLBW1"], 
                        imei, mode)
                myCursor.execute(sql, val)
        elif mode == '2':
            sql = """INSERT INTO COVERAGE (MCC, MNC, CELLID, PCID, FREQBW, ULBW, DLBW, RSRP, RSRQ, SINR, TXPOWER, device_id, timestamp, mode) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ,(SELECT id FROM DEVICE_PROPERTIES WHERE imei = %s)
                    ,NOW()
                    ,%s
                    );"""
            val = (valores_cobertura["MCC"], valores_cobertura["MNC"], valores_cobertura["CELLID"], valores_cobertura["PCID"], 
                    valores_cobertura["FREQBW"], valores_cobertura["ULBW"], valores_cobertura["DLBW"], valores_cobertura["RSRP"], valores_cobertura["RSRQ"], 
                    valores_cobertura["SINR"], valores_cobertura["TXPOWER"], imei, mode)
            myCursor.execute(sql, val)
        elif mode == '3':
            sql = """INSERT INTO COVERAGE (MCC, MNC, CELLID, RSCP, ECIO, device_id, timestamp, mode) 
                    VALUES (%s, %s, %s, %s, %s
                    ,(SELECT id FROM DEVICE_PROPERTIES WHERE imei = %s)
                    ,NOW()
                    ,%s
                    );"""
            val = (valores_cobertura["MCC"], valores_cobertura["MNC"], valores_cobertura["CELLID"], valores_cobertura["RSCP"], 
                    valores_cobertura["ECIO"], imei, mode)
            myCursor.execute(sql, val)

        
    except Exception as ex:
            print(ex)
            textToWrite = b"Error inserting values on COVERAGE\n",ex
            exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)