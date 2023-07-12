    # /* ===================================================== 
    #
    # * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved. 
    #
    # * File_name kamstrup
    #
    # * Description:  The file that handles the kamstrup payload
    #
    # * Author:  Alejandro
    #
    # * Date:  27-10-22
    #
    # * Version:  0.8
    #
    # * =================================================== */
from operator import countOf
import crcmod 

def bit_not(n, numbits=8):
    return (1 << numbits) - 1 - n

def unStuffing(resp_aux):
    size_resp = len(resp_aux)
    resp_auxReturn = ""
    sum = 0
    i = 0
 
    while i < size_resp:
        hexa_to_use = ''.join(format(resp_aux[i],'02x'))
        if hexa_to_use == '1b':
            sum += 1

            i += 1

            resp_auxReturn += ''.join(format(bit_not(resp_aux[i]),'02x'))

            i += 1
        else:
            resp_auxReturn += str(hexa_to_use)
            i += 1
        # j = 1+i

        # while j < size_resp:
        #     resp_auxReturn.append(resp_aux[j + 1])
        #     j += 1

    return sum,resp_auxReturn


def checkCRC(message, size2CRC):

    CRCresp = 0
    size2CRC = len(message)-4
    CRCresp = message[size2CRC:]
    CRCresp = int(str(CRCresp),16) << 8
    CRCresp = int(str(CRCresp),16) | int(message[int(size2CRC) + 1],16)

    crc16 = crcmod.predefined.Crc('crc-16')
    crc16.update(str.encode(message[0:len(message)-4]))
    # print(hex(crc16.crcValue))
    if CRCresp == hex(crc16.crcValue):
        res = True
    else: 
        res = False

    return res



def getRegister(unstuffed_array) : # algo pasa aqui
    nReg = 6
    ndx = 2
    reg_value = 0 
    values = []

    meter_resp = unstuffed_array[:len(unstuffed_array)-4]+'0000'
    byte_meter_resp =  bytearray.fromhex(meter_resp)
    for i in range(nReg):
        regID = int(''.join(format(byte_meter_resp[ndx],'02x')),16)
        ndx += 1

        regID <<= 8

        regID = regID | int(''.join(format(byte_meter_resp[ndx],'02x')),16)

        ndx += 1
 

        Unit = int(''.join(format(byte_meter_resp[ndx],'02x')),16)
        ndx += 1

        NoB = int(''.join(format(byte_meter_resp[ndx],'02x')),16)
        ndx += 1
       
        SiEx = int(''.join(format(byte_meter_resp[ndx],'02x')),16)
        ndx += 1

        reg_value = 0 
        j = 0
        while j < (int(NoB)):
            reg_value <<= 8 
            reg_value = reg_value | int(''.join(format(byte_meter_resp[ndx],'02x')),16)
            ndx += 1
            j += 1

        Exp = SiEx & 0x3F 

        SE = (SiEx >> 6) & 0x01 

        SI = SiEx >> 7 

        auxInt = pow(10, Exp) 

        auxInt += 0
        
        values.append(pow(-1,SI)*reg_value*pow(10,pow(-1,SE)*Exp))

    return values 

def processData(resp_aux):
    try:
        rtmp =  bytearray.fromhex(resp_aux) # to decode = ''.join(format(x,'02x') for x in a
        stuffed_bytes, unstuffed_array= unStuffing(rtmp)

        num2CRC = len(resp_aux)/2 - stuffed_bytes - 4

        if(num2CRC<0):
            num2CRC = 1
        
        numbers = getRegister(unstuffed_array) # The noise number is not here
    except Exception as ex:
        print("ProcessData",ex)
    return numbers

def process_alarm(alarm):

    alarm_bits =  bin(int(alarm,16)) 
    alarm_bits = ('0'*(32-(len(alarm_bits)-2)))+alarm_bits[2:]
    alarms = []
    alarms_array = []
    alarms_time = []
    myNumber =''
    for i in range(32):
        if i<21:
            if len(myNumber)< 3:
                myNumber += alarm_bits[i]
            else:
                
                alarms_time.append(int(myNumber,2))
                myNumber = alarm_bits[i]
        elif i == 21:
            alarms_time.append(int(myNumber,2))  
            alarms_array.append(alarm_bits[i])  
        else:        
            alarms_array.append(alarm_bits[i])
    alarms.append(alarms_time)
    alarms.append(alarms_array)
    print(alarms)
    return alarms

if __name__ == '__main__':
    # resp_aux = "3F1001CA330200001C1BF2583F1001CA330200001C1BF258"
    # resp_aux = "0200001C1"
    # 100000E0
    # print(process_alarm(resp_aux))
    resp_aux = "3F1000442804420000000100F328044200000000004A290200057E02463E02001AFE012B2501001BE4012425010028023E001BF90000501BF900601BF9AB73"
    rtmp =  bytearray.fromhex(resp_aux) # to decode = ''.join(format(x,'02x') for x in a
    stuffed_bytes, unstuffed_array= unStuffing(rtmp)
    numbers = getRegister(unstuffed_array)
    print(numbers)
    #     numbers = getRegister(unstuffed_array)
    #     print(numbers)
    # else:
    #     numbers = 0


 
