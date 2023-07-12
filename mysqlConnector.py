# /* =====================================================
# * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY
#   VERTICALS S.L. All rights reserved.
# * File_name mysqlConnector
# * Description:  MySQL DB Connector
# * Author:  Alejandro
# * Date:  25-08-21
# * Version:  1.33
# * =================================================== */

import exceptions
from mysql.connector import connection

# mydbConnector = mysql.connector.connect(
#     host="localhost",
#     user="suscriptorPython",
#     password="Rw3X3sZeH6JRXAXPP9pz",
#     database="n8htgo8_fivecomm")


def mysqlConnect():
    # function to obtain the mysql connector and the pointer
    # input: void
    # output: mydbConnector -> connector, myCursor -> cursor
    try:
        #print("connecting to mysql")
        mydbConnector = connection.MySQLConnection(
            host="172.17.0.3", 
            user="root", 
            password="Fivecomm", 
            database="narrow_db")
        myCursor = mydbConnector.cursor(buffered=True)
    except exceptions as ex:
        textToWrite = b"Error obtaining the connector\n"
        exceptions.exceptionHandler(textToWrite)
    return mydbConnector, myCursor


def mysqlClose(mydbConnector, myCursor):
    # function to close connection with mysql      no vestas en el servidor tu docker tiene u
    # input: mydbConnector -> connector to mysql, myCursor -> cursor to the bbdd
    # output: void
    try:
        mydbConnector.commit()
        myCursor.close()
        mydbConnector.close()

    except:
        textToWrite = b"Error closing the connector\n"
        exceptions.exceptionHandler(textToWrite)
