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
def diameterToVolume(diameter):
    # function to obtain the volume of liters from the diameter
    # input: diameter -> object(should be integer)
    # output: volume2Liters -> integer
    # to do: should be done with ddbb access
    diameter = int(diameter)
    if diameter == 15:
        volume2Liters = 3125
    elif diameter == 20:
        volume2Liters = 5000
    elif diameter == 25:
        volume2Liters = 7875
    elif diameter == 80:
        volume2Liters = 125000
    elif diameter == 100:
        volume2Liters = 200000
    else:
        volume2Liters = 3125

    return volume2Liters
