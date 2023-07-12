import exceptions
from mysqlConnector import *
ip = "172.17.0.3"
db = "narrow_db"


def getAllIMEIs():
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        myCursor.execute("SELECT imei FROM DEVICE_PROPERTIES;")
        imeis = []
        for row in myCursor.fetchall():
            imei = str(row[0])
            if len(imei) < 15:
                imei = "0" + imei
            imeis.append(imei)
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining IMEIs from DEVICE_PROPERTIES\n"
        exceptions.exceptionHandler(textToWrite)
        value = textToWrite

    mysqlClose(mydbConnector,myCursor)
    return imeis

def getDeviceProperties(column, value):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql =( """select imei, reportTime, fw, hw, signal_threshold, wmb_mode, wmb_measurement_interval, 
              wmb_measurement_window, apn, sn, imsi, last_mssg_send, networked, decrypt, wake_up, id, 
              filter_vertical, filter_manufacturer, filter_model
              from DEVICE_PROPERTIES WHERE """ + column + " = %s;")
        val = (value,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        properties = {"imei": result[0], "reportTime": result[1], "fw": result[2], "hw": result[3], "signal_threshold": result[4],
                       "wmb_mode": result[5], "wmb_measurement_interval": result[6], "wmb_measurement_window": result[7], "apn": result[8],
                         "sn": result[9], "imsi": result[10], "last_mssg_send": result[11], "networked": result[12], "decrypt":result[13],
                         "wake_up": result[14], "id": result[15], "filter_vertical": result[16], 
                         "filter_manufacturer": result[17], "filter_model":result[18]}
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining properties from DEVICE_PROPERTIES\n"
        exceptions.exceptionHandler(textToWrite)
        properties = textToWrite

    mysqlClose(mydbConnector,myCursor)
    return properties

def getGreyListSensorProperties(column, value):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql =( """select device_id, average, sensor_id, rssi FROM GREY_LIST WHERE """ + column + " = %s;")
        val = (value,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        properties = {"device_id": result[0], "nveces": result[1], "sensor_id": result[2], "rssi": result[3] }
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining properties from DEVICE_PROPERTIES\n"
        exceptions.exceptionHandler(textToWrite)
        properties = textToWrite

    mysqlClose(mydbConnector,myCursor)
    return properties

def getUserAndPassFromImei(imei):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """ SELECT mp.user, mp.password 
                FROM MQTT_PROPERTIES mp 
                JOIN DEVICE_PROPERTIES dp ON mp.device_id = dp.id 
                WHERE dp.imei = %s 
                AND mp.id = (
                SELECT MAX(id) 
                FROM MQTT_PROPERTIES mp2 
                WHERE mp2.device_id = dp.id
                );"""
        val = (imei,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        user = result[0]
        password = result[1]

    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining user and pass from DEVICE_PROPERTIES\n"
        exceptions.exceptionHandler(textToWrite)
        id = textToWrite

    mysqlClose(mydbConnector,myCursor)

    return user, password

def getBasicDataFromSn(id):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """SELECT volume, preassure, temperature, flow from DATA where sensor_id = %s"""
        val = (id,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        
        data = {"volume":result[0], "preassure":result[1], "temp":result[2], "flow": result[3]}

    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining user and pass from DEVICE_PROPERTIES\n"
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)

    return data


def getDevicePropertiesFromSn(column,sn):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """SELECT """ + column + """ FROM DEVICE_PROPERTIES WHERE sn = %s;"""
        val = (sn,)
        myCursor.execute(sql, val)
        data = myCursor.fetchone()

    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining user and pass from DEVICE_PROPERTIES\n"
        exceptions.exceptionHandler(textToWrite)
        data = textToWrite

    mysqlClose(mydbConnector,myCursor)
    return data
    
def getWhiteListSensorById(id):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql =( "select manufacturer, model, vertical, device_id from WHITE_LIST WHERE sensor_id = %s;")
        val = (id,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        properties = {"manufacturer": result[0], "model": result[1], "vertical": result[2], "device_id": result[3]}
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining properties from WHITELIST\n"
        exceptions.exceptionHandler(textToWrite)
        properties = textToWrite
    mysqlClose(mydbConnector,myCursor)
    return properties


def getGreyListSensorById(id):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql =( """select manufacturer, model, vertical, device_id 
              from GREY_LIST 
              WHERE sensor_id = %s
              ORDER BY id DESC
              LIMIT 1;""")
        val = (id,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        properties = {"manufacturer": result[0], "model": result[1], "vertical": result[2], "device_id": result[3]}
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining properties from WHITELIST\n"
        exceptions.exceptionHandler(textToWrite)
        properties = textToWrite
    mysqlClose(mydbConnector,myCursor)
    return properties

def getWhiteListBySn(column,sn):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = ("SELECT " + column + " FROM WHITE_LIST wl JOIN DEVICE_PROPERTIES dp ON wl.device_id = dp.id WHERE dp.sn = %s;")
        val = (sn,)
        myCursor.execute(sql, val)
        data = []
        for row in myCursor.fetchall():
                #print(row)
                data.append(row)
        ids = [device_id[0] for device_id in data]
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining White List\n"
        exceptions.exceptionHandler(textToWrite)
        id = textToWrite
    mysqlClose(mydbConnector, myCursor)
    return ids

def getGreyListBySn(sn):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = ("""SELECT gl.sensor_id, gl.rssi, gl.average, gl.`timestamp`
                FROM narrow_db.GREY_LIST gl 
                JOIN narrow_db.DEVICE_PROPERTIES dp 
                ON gl.device_id = dp.id 
                WHERE DATE_FORMAT(gl.`timestamp`, '%Y-%m-%d %H') =(
                	SELECT DATE_FORMAT(gr.`timestamp`, '%Y-%m-%d %H')
                	FROM narrow_db.GREY_LIST gr
                	JOIN narrow_db.DEVICE_PROPERTIES dp 
                	ON gr.device_id = dp.id
                	where dp.sn = %s
                    ORDER BY gl.`timestamp` DESC
                    LIMIT 1
                );""")
        val = (sn,)
        myCursor.execute(sql, val)
        result = myCursor.fetchall()

        lista_resultados = []
        for i, row in enumerate(result):
            sensor_id = row[0]
            rssi = row[1]
            average = row[2]
            timestamp = row[3]
            lista_resultados.append((sensor_id, rssi, average, timestamp.strftime('%Y-%m-%d %H:%M:%S')))
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining Grey List\n"
        exceptions.exceptionHandler(textToWrite)
        lista_resultados = textToWrite
    mysqlClose(mydbConnector, myCursor)
    return lista_resultados

def getAllFromGreyListBySn(sn, fecha):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = ("""SELECT gl.average, gl.sensor_id, gl.rssi FROM GREY_LIST gl JOIN DEVICE_PROPERTIES dp ON gl.device_id = dp.id 
               WHERE dp.sn = %s AND DATE_FORMAT(gl.timestamp, '%Y-%m-%d %H:00:00') = DATE_FORMAT(%s, '%Y-%m-%d %H:00:00');""")
        val = (sn,fecha)
        myCursor.execute(sql, val)
        result = myCursor.fetchall()
        data = {}
        if result:
            for row in result:
                average, sensor_id, rssi = row
                data[sensor_id] = {"nveces": average, "rssi": rssi}
        print(data)
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining Grey List\n"
        exceptions.exceptionHandler(textToWrite)
        data = textToWrite

    mysqlClose(mydbConnector, myCursor)
    return data

def getTxTime(sn):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql =( """select d.timestamp from `DATA` d JOIN DEVICE_PROPERTIES dp ON d.device_id = dp.id 
                    WHERE dp.sn = %s
                    order by `timestamp` desc""")
        val = (sn,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining properties from WHITELIST\n"
        exceptions.exceptionHandler(textToWrite)
        properties = textToWrite
    mysqlClose(mydbConnector,myCursor)
    return result[0]

def getCoverageRealTime(sn):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """SELECT * FROM COVERAGE c 
                    JOIN DEVICE_PROPERTIES dp ON c.device_id = dp.id
                    WHERE dp.sn = %s"""
        val = (sn,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()

        # Obtener los nombres de las columnas
        column_names = [desc[0] for desc in myCursor.description]

        # Crear un diccionario con los resultados
        result_dict = {}
        if result != None:
            for indice, nombre in enumerate(column_names):
                if indice >0 and indice <12:
                    result_dict[column_names[indice]] = result[indice]
        print(result_dict)
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining coverage\n"
        exceptions.exceptionHandler(textToWrite)
        result_dict = textToWrite
    mysqlClose(mydbConnector,myCursor)
    return result_dict

def getPendingDataBySensorId(sensor_id, fecha):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """select id from `DATA` d where sensor_id = %s AND sent = 0 AND DATE(timestamp) = %s order by timestamp"""
        val = (sensor_id, fecha)
        myCursor.execute(sql, val)
        data = []
        for row in myCursor.fetchall():
                data.append(row)
        ids = [data_id[0] for data_id in data]
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining data id\n"
        exceptions.exceptionHandler(textToWrite)
        id = textToWrite
    mysqlClose(mydbConnector, myCursor)
    return id

def getPendingSensorsId(sn, fecha):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """select d.id, d.sensor_id, d.received from `DATA` d JOIN DEVICE_PROPERTIES dp ON dp.id = d.device_id where d.sent = 0 AND dp.sn = %s AND DATE(d.received) = %s order by d.received, d.sensor_id"""
        val = (sn, fecha)
        myCursor.execute(sql, val)
        data = []
        for row in myCursor.fetchall():
                data.append(row)
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining data id\n"
        exceptions.exceptionHandler(textToWrite)
        id = textToWrite
    mysqlClose(mydbConnector, myCursor)
    return data

def getDataBySensorIdAndHour(sensor_id, hour, fecha):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """SELECT device_id, sent, received, data, id, rssi from DATA where sensor_id = %s AND hour = %s AND sent = 0 AND DATE(received) = %s"""
        val = (sensor_id, hour, fecha)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        
        data = {"device_id":result[0], "sent":result[1], "received":result[2], "data":result[3], "id":result[4],
                "rssi":result[5]}

    except Exception as ex:        
        textToWrite = b"Error obtaining data in hour\n" + str(hour)
        exceptions.exceptionHandler(textToWrite)
        data = {"data":"", "rssi":""}

    mysqlClose(mydbConnector,myCursor)
    return data

def getData(id):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """SELECT id, device_id, hour, sensor_id, sent, received, data, rssi from DATA where id = %s"""
        val = (id,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        
        data = {"id":result[0], "device_id":result[1], "hour":result[2], "sensor_id":result[3], "sent":result[4], "received":result[5], "data":result[6], "rssi":result[7]}
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining user and pass from DEVICE_PROPERTIES\n"
        exceptions.exceptionHandler(textToWrite)

    mysqlClose(mydbConnector,myCursor)
    return data

def getPendingDevices(time):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """select distinct dp.imei from DEVICE_PROPERTIES dp JOIN `DATA` d ON dp.id = d.device_id WHERE CURRENT_TIMESTAMP() - d.received  >= %s AND d.sent = 0"""
        val = (time, )
        myCursor.execute(sql, val)
        data = []
        for row in myCursor.fetchall():
                data.append(row)
        imeis = [imei[0] for imei in data]
    except Exception as ex:
            print(ex)        
            textToWrite = b"Error obtaining data id\n"
            exceptions.exceptionHandler(textToWrite)
            id = textToWrite
    mysqlClose(mydbConnector, myCursor)
    return imeis

def getAllFromGreyListBySnNoDate(sn, fecha):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = ("""SELECT gl.average, gl.sensor_id, gl.rssi, gl.timestamp FROM GREY_LIST gl JOIN DEVICE_PROPERTIES dp ON gl.device_id = dp.id 
               WHERE dp.sn = %s AND DATE(gl.timestamp) = DATE(%s) ORDER BY gl.timestamp DESC;""")
        val = (sn,fecha)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        if result:
            data = result[3]
        else:
            data = None
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining Grey List\n"
        exceptions.exceptionHandler(textToWrite)
        data = textToWrite

    mysqlClose(mydbConnector, myCursor)
    return data