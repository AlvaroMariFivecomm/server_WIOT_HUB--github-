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
from datetime import datetime
from calendar import monthrange

def obtainStringFromDatetime(cTime):
    # function to a correct string for mysql from a datetime
    # input: cTime -> datetime
    # output: stringDate -> string
    if cTime.month < 10:
        month = "0"+str(cTime.month)
    else:
        month = str(cTime.month)    

    if cTime.day < 10:
        day = "0"+str(cTime.day)
    else:
        day = str(cTime.day)

    if cTime.hour < 10:
        hour = "0"+str(cTime.hour)
    else:
        hour = str(cTime.hour)
    
    if cTime.minute < 10:
        minute = "0"+str(cTime.minute)
    else:
        minute = str(cTime.minute)

    if cTime.second < 10:
        second = "0"+str(cTime.second)
    else:
        second = str(cTime.second)    

    stringDate = str(cTime.year) + "-" + month + "-" + day + " " + hour + ":00:00"

    return stringDate

def obtainSeparatedDateFromDatetime(cTime):
    # function to a correct string for mysql from a datetime
    # input: cTime -> datetime
    # output: stringDate -> string
    year = str(cTime.year)
    if cTime.month < 10:
        month = "0"+str(cTime.month)
    else:
        month = str(cTime.month)    

    if cTime.day < 10:
        day = "0"+str(cTime.day)
    else:
        day = str(cTime.day)

    if cTime.hour < 10:
        hour = "0"+str(cTime.hour)
    else:
        hour = str(cTime.hour)
    
    if cTime.minute < 10:
        minute = "0"+str(cTime.minute)
    else:
        minute = str(cTime.minute)

    if cTime.second < 10:
        second = "0"+str(cTime.second)
    else:
        second = str(cTime.second)    

   
    return year,month,day,hour,minute,second

def obtainSQLDateFromDatetime(year,month,day,hour):
     # inserting minutes and seconds before the hour
     return str(year) + "-" + month + "-" + day + " " + hour + ":%%:%%"

def obtainWakeUp():

    cDate = datetime.utcnow()

    if cDate.month == 12 and cDate.day == monthrange(cDate.year, cDate.month)[1]:
        cWakeUp = datetime.timestamp(cDate.replace(year = cDate.year+1,month=1,hour=12,minute=30,day=1))
    else:
        if cDate.day == monthrange(cDate.year, cDate.month)[1]:
            cWakeUp = datetime.timestamp(cDate.replace(month=cDate.month+1,hour=12,minute=30,day=1))
        else:
            cWakeUp = datetime.timestamp(cDate.replace(hour=12,minute=30,day=cDate.day+1))
    
    return cWakeUp

def stringFromDatetime(cDate):
    return str(cDate.year) + "-" + str(cDate.month) + "-" + str(cDate.day) + " " + str(cDate.hour) + ":" + str(cDate.minute) + ":" + str(cDate.second)

def obtainStringFromDatetimeWithMinute(cTime):
    # function to a correct string for mysql from a datetime
    # input: cTime -> datetime
    # output: stringDate -> string
    if cTime.month < 10:
        month = "0"+str(cTime.month)
    else:
        month = str(cTime.month)    

    if cTime.day < 10:
        day = "0"+str(cTime.day)
    else:
        day = str(cTime.day)

    if cTime.hour < 10:
        hour = "0"+str(cTime.hour)
    else:
        hour = str(cTime.hour)
    
    if cTime.minute < 10:
        minute = "0"+str(cTime.minute)
    else:
        minute = str(cTime.minute)

    if cTime.second < 10:
        second = "0"+str(cTime.second)
    else:
        second = str(cTime.second)    

    stringDate = str(cTime.year) + "-" + month + "-" + day + " " + hour + ":"+minute+":00"

    return stringDate