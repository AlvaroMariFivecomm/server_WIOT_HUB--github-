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
import exceptions
import math
import mysqlGet
import mysqlSet

changeSpeed = 0.05        

def batteryConversion(battery_linear,id):
    try:
        reference_value = 36 
        zero_value = 34
        aux_battery = math.exp(-(reference_value-battery_linear))
        diff_value = math.exp(-(reference_value-zero_value))

        percentile = ((aux_battery-diff_value)/(1-diff_value))*100
        param = mysqlGet.getBatteryFromGatewayId(id)
        if param == None:
            balanceCounter = 0
            maxValue = 36000
            minValue = 100
        else:

            balanceCounter = param
            maxValue = 3600
            minValue = 0
            
        if isinstance(balanceCounter, type(None)):
            balanceCounter = 0
            maxValue = battery_linear
            minValue = battery_linear
            mysqlSet.insertBatteryParameters(id,battery_linear,0,balanceCounter)
            mysqlSet.insertBatteryParameters(id,battery_linear,1,balanceCounter)

        dst = int((maxValue-minValue)/2)
        if percentile >= dst:
            y = maxValue
            balanceCounter = balanceCounter + 1
        else:
            y = minValue
            balanceCounter = balanceCounter - 1
       
        newValue = (1 - changeSpeed) * y + changeSpeed * percentile

        if percentile >= dst:
            maxValue = newValue
            
        else:
            minValue = newValue

        mysqlSet.insertBatteryParameters(id,newValue,percentile >= dst,balanceCounter)

        if balanceCounter > 0:
            result = maxValue
        else:
            result = minValue
            
        if result > 100:
            result = 100
    except Exception as ex:
        print("mi casa")
        print(ex)
        textToWrite = b"Error converting the battery\n"
        exceptions.exceptionHandler(textToWrite)

    return result
