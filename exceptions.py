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

def exceptionHandler(textToWrite):
    # function to handle exceptions and writing the error in to a file
    # input: textToWrite -> string
    # output: void
    logFile = open("logs/backendErrors.txt", "ab")
    cDate = datetime.utcnow()           
    stringDate = str(cDate.year) + "-" + str(cDate.month) + "-" + str(cDate.day) + " " + str(cDate.hour) + ":" + str(cDate.minute) + ":" + str(cDate.second)
    logFile.write(bytes(stringDate,'ascii')+bytes(" : ",'ascii')+textToWrite)
    logFile.close()
    print(textToWrite.decode('ASCII'))