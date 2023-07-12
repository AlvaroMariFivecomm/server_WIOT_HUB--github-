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

def vectorRxDo(msg):
    # logic function when the topic is a trasmsion topic
    # input: void
    # output: void
    print("lectura placa")
    print(msg.payload)
    print(msg.topic)     
