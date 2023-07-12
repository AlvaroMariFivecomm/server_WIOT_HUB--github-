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

def signal_values(signal, sigtec):
    try:
        G5_RSRP = 0
        G5_RSRQ = 0
        G5_RSSI = 0
        G5_SINR = 0
        G5_TX_POW = 0
        G4_RSRP = 0
        G4_RSRQ = 0
        G4_SINR = 0
        G4_RSSI = 0
        G4_TX_POW = 0
        G3_RSCP = 0
        G3_ecio = 0

        tmp = signal.split(",")

        if sigtec == 0: # 5g SA
            G5_RSRP = tmp[0]
            G5_RSRQ = tmp[1]
            G5_RSSI = tmp[2]
            G5_SINR = tmp[3]
            G5_TX_POW = tmp[4]
        elif int(sigtec) == 1: # 5g NSA
            G5_RSRP = tmp[0]
            G5_RSRQ = tmp[1]
            G5_RSSI = tmp[2]
            G5_SINR = tmp[3]
            G5_TX_POW = tmp[4]
            G4_RSRP = tmp[5]  
        elif int(sigtec) == 2:# 4g 
            G4_RSRP = tmp[0]
            G4_RSRQ = tmp[1]
            G4_SINR = tmp[2]
            G4_RSSI = tmp[3]
            G4_TX_POW = tmp[4]
        elif int(sigtec) == 3: # 3g
            G3_RSCP = tmp[0]
            G3_ecio = tmp[1]
    except:
        textToWrite = b"Error handling the signal values\n"
        exceptions.exceptionHandler(textToWrite)
        
    return G5_RSRP, G5_RSRQ,G5_RSSI, G5_SINR, G5_TX_POW, G4_RSRP, G4_RSRQ, G4_SINR, G4_RSSI, G4_TX_POW, G3_RSCP, G3_ecio
