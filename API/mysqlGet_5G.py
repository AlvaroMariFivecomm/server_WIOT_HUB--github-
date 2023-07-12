from mysqlConnector import *
import exceptions
ip = "172.17.0.2"
db = 'wmb_db'

def getAllIMEIs():
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        myCursor.execute( "select imei from DEVICE_PROPERTIES;")
        imeis = []
        for row in myCursor.fetchall():
            imeis.append(str(row[0]))
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
              wmb_measurement_window, apn, sn, imsi, last_mssg_send, networked, decrypt, wake_up, id, filter_model from DEVICE_PROPERTIES WHERE """ + column + " = %s;")
        val = (value,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        properties = {"imei": result[0], "reportTime": result[1], "fw": result[2], "hw": result[3], "signal_threshold": result[4],
                       "wmb_mode": result[5], "wmb_measurement_interval": result[6], "wmb_measurement_window": result[7], "apn": result[8],
                         "sn": result[9], "imsi": result[10], "last_mssg_send": result[11], "networked": result[12], "decrypt":result[13],
                         "wake_up": result[14], "id": result[15], "filter_model": result[16]}
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
        textToWrite = b"Error obtaining battery from DEVICE_PROPERTIES\n"
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
        sql =( "select manufacturer, model, vertical, device_id from GREY_LIST WHERE sensor_id = %s;")
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

def getGreyListBySn(column,sn):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = ("SELECT " + column + " FROM GREY_LIST wl JOIN DEVICE_PROPERTIES dp ON wl.device_id = dp.id WHERE dp.sn = %s;")
        val = (sn,)
        myCursor.execute(sql, val)
        data = []
        for row in myCursor.fetchall():
                print(row)
                data.append(row)
        ids = [device_id[0] for device_id in data]
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining Grey List\n"
        exceptions.exceptionHandler(textToWrite)
        id = textToWrite
    mysqlClose(mydbConnector, myCursor)
    return ids

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

def getCoverageRealTime(imei):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """SELECT TXPOWER, SINR, RSCP, CELLID FROM COVERAGE c 
                    JOIN DEVICE_PROPERTIES dp ON c.device_id = dp.id
                    WHERE dp.imei = %s"""
        val = (imei,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        if result is not None:
            result = {
                "TXPOWER": result[0] if result[0] is not None else None,
                "SINR": result[1] if result[1] is not None else None,
                "RSCP": result[2] if result[2] is not None else None,
                "CELLID": result[3] if result[3] is not None else None
            }
        else:
            result = {
                "TXPOWER": None,
                "SINR": None,
                "RSCP": None,
                "CELLID": None
            }
    except Exception as ex:
        print(ex)        
        textToWrite = b"Error obtaining coverage\n"
        exceptions.exceptionHandler(textToWrite)
        result = textToWrite
    mysqlClose(mydbConnector,myCursor)
    return result

def getPendingDataBySensorId(sensor_id):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """select id from `DATA` d where sensor_id = %s AND sent = 0 order by timestamp"""
        val = (sensor_id,)
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
    return ids

def getData(id):
    try:
        tmp = mysqlConnect(ip,db)
        myCursor = tmp[1]
        mydbConnector = tmp[0]
        sql = """SELECT volume, preassure, temperature, flow, timestamp from DATA where id = %s"""
        val = (id,)
        myCursor.execute(sql, val)
        result = myCursor.fetchone()
        
        data = {"volume":result[0], "pressure":result[1], "temp":result[2], "flow": result[3], "timestamp": result[4]}

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
