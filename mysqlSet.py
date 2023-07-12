import mysql.connector
import mysqlGet
from mysqlConnector import *

def updateDevice_property(imei,column,value):
    try:
        tmp = mysqlConnect()
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
        tmp = mysqlConnect()
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
        tmp = mysqlConnect()
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

def insertNewDevice(imei, timestamp, reportTime, fw, hw, signal_threshold, sn, networked):
    try:
        # Insertar los valores en device_properties
        tmp = mysqlConnect()
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """INSERT INTO DEVICE_PROPERTIES (imei, timestamp, reportTime, fw, hw, signal_threshold, sn, networked) values (%s,%s,%s,%s,%s,%s,%s,%s)"""
        val = (imei, timestamp, reportTime, fw, hw, signal_threshold, sn, networked)
        myCursor.execute(sql, val)

        
    except Exception as ex:
        textToWrite = b"Error inserting values on MQTT_PROPERTIES\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)

def insertNewWhiteList(sensor_id):
    sensor = mysqlGet.getGreyListSensorById(sensor_id)

    try:
        tmp = mysqlConnect()
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

def deleteWhiteList(sn):
    try:
        tmp = mysqlConnect()
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """DELETE FROM WHITE_LIST wl
                    WHERE device_id IN (
                    SELECT id
                    FROM DEVICE_PROPERTIES dp
                    WHERE dp.sn = %s
                    );"""
        val = (sn)
        myCursor.execute(sql, val)

        
    except Exception as ex:
        textToWrite = b"Error inserting values on MQTT_PROPERTIES\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)
def insertMQTT_properties(imei, user, password, txTopic, rxTopic):
    try:
        # Insertar los valores en mqtt_properties
        tmp = mysqlConnect()
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

def insertDates(imei, sensor_id, hora, data, rssi, backup):
    try:
        # Obtener el ID de la fila en device_properties utilizando un JOIN
        tmp = mysqlConnect()
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = ''
        if backup == '0':
            sql = """INSERT INTO DATA (hour, sensor_id, data, rssi, device_id, received) 
                    VALUES( %s, %s, %s, %s,
                            (SELECT dp.id FROM DEVICE_PROPERTIES dp WHERE dp.imei = %s),
                            CONVERT_TZ(NOW(), '+00:00', '+02:00'))"""
        elif backup == '1':
            sql = """INSERT INTO DATA (hour, sensor_id, data, rssi, device_id, received) 
                    VALUES( %s, %s, %s, %s,
                            (SELECT dp.id FROM DEVICE_PROPERTIES dp WHERE dp.imei = %s),
                            DATE_SUB(NOW(), INTERVAL 22 HOUR))"""
        elif backup == '2':
            sql = """INSERT INTO DATA (hour, sensor_id, data, rssi, device_id, received) 
                    VALUES( %s, %s, %s, %s,
                            (SELECT dp.id FROM DEVICE_PROPERTIES dp WHERE dp.imei = %s),
                            DATE_SUB(NOW(), INTERVAL 46 HOUR))"""
        val = (int(hora), sensor_id, data, rssi, imei)
        myCursor.execute(sql, val)

        
    except Exception as ex:
        textToWrite = b"Error inserting values on DATA\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)

def insertSensorToGraylist(imei, average, rssi, sensor_id):
    try:
        tmp = mysqlConnect()
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """INSERT INTO GREY_LIST (average, rssi, sensor_id, device_id, timestamp,
                 manufacturer, model, vertical) 
                VALUES (%s, %s, %s, 
                (SELECT dp.id FROM DEVICE_PROPERTIES dp WHERE dp.imei = %s),
                CONVERT_TZ(NOW(), '+00:00', '+02:00'),
                %s,
                %s,
                %s);"""
        val = (average, rssi, sensor_id, imei, sensor_id[:4], sensor_id[-4:-2], sensor_id[-2:])
        myCursor.execute(sql, val)
        
        print("INSERT sensor" + str(sensor_id) + "executed successfully!")

        
    except Exception as ex:
        textToWrite = b"Error inserting values on DATA\n",ex
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)

def insertCoverage(imei, valores_cobertura):
    try:
        # Obtener el ID de la fila en device_properties utilizando un JOIN
        tmp = mysqlConnect()
        myCursor = tmp[1]
        mydbConnector = tmp[0]

        sql = """INSERT INTO COVERAGE (Cc, Nc, RSRP, RSRQ, TAC, Id_cov, EARFCN, PWD, PAGING, CID, BAND, BW, device_id, timestamp) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ,(SELECT id FROM DEVICE_PROPERTIES WHERE imei = %s)
                ,NOW()
                );"""
        val = (valores_cobertura["Cc"], valores_cobertura["Nc"], valores_cobertura["RSRP"], valores_cobertura["RSRQ"], 
                valores_cobertura["TAC"], valores_cobertura["Id"], valores_cobertura["EARFCN"], valores_cobertura["PWR"],
                valores_cobertura["PAGING"], valores_cobertura["CID"], valores_cobertura["BAND"], valores_cobertura["BW"],
                imei)
        myCursor.execute(sql, val)
        
    except Exception as ex:
            print(ex)
            textToWrite = b"Error inserting values on COVERAGE\n",ex
            exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)