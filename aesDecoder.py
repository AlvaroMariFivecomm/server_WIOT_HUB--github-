# /* ===================================================== 
# * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        
#    VERTICALS S.L. All rights reserved. 
# * File_name: aesDecoder.py
# * Description:  AES frame decoder (CBC)
# * Author:  {pablo.trelis, miriam.ortiz}@fivecomm.eu
# * Date:  28-2-23
# * Version:  1.0
# * =================================================== */ 
import binascii
from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import pad
import mysqlSet
import datetime

HEADER_LENGTH = 15  # Header length in bytes
ACCNUM_INDEX = [22, 24]  # Accesss number index in header (1 byte)
NUM_MEASURES = 4  # Number of measures in the frame (volume, flow, temp, pressure)
# KEY = "e7ac09ce9e902d3aaf23eb592aa01973"
# KEY = "0E9FACC6A0E1EF1C3B27A49BAC216535"
# KEY = "356521AC9BA4273B1CEFE1A0C6AC9F0E"

def meassureToFloat(code: str, div: int) -> float:
    v_aux = []
    aux_str = ""
    for i in code:
        if len(aux_str) == 2:
            v_aux.append(aux_str)
            aux_str = ""
        aux_str += i
    v_aux.append(aux_str)
    v_aux = v_aux[::-1]
    value = "".join(str(v) for v in v_aux)
    return int(value, 16)/div

def aesSingleDecoder(data,keyHex):
    dt = data[-110:]
    clean_frames = dt[:20] + dt[24:56] + dt[60:92] + dt[96:106]
     
    header = clean_frames[:HEADER_LENGTH*2] 
    clean_frames = clean_frames[HEADER_LENGTH*2:] 
    # print(clean_frames)
    data = pad(binascii.unhexlify(clean_frames), AES.block_size)
    iv = binascii.unhexlify(ivCalc(header))
    key = binascii.unhexlify(keyHex)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted_data = cipher.decrypt(data)
    dec_array = ""
    for i in decrypted_data:
        if i < 16:
            dec_array += "0" + str(hex(i)).split("x")[1]
        else:
            dec_array += str(hex(i)).split("x")[1]
    print("decrypted data: " + dec_array)  # This is the decrypted data
    return dataInterpreter(dec_array)
    # return dec_array

ivCalc = lambda header: header[4:20] + 8 * header[ACCNUM_INDEX[0]:ACCNUM_INDEX[1]]

def dataInterpreter(data):
    enc_verif = data[:4]
    print("Decryption successful") if enc_verif == "2f2f" else print(
        "Decryption failed")
    pointer = 4
    measures = []
    current_byte = data[pointer:pointer+2]
    meassures_count = 0
    while meassures_count < NUM_MEASURES:
        measure_obj = {"units": "", "value": ""}
        measure_obj["units"] = str(data[pointer+2:pointer+4])
        measure_obj["value"] = str(data[pointer+4:pointer+4+2*int(current_byte,16)])
        measures.append(measure_obj)
        pointer += (4 + 2 * int(current_byte,16))
        current_byte = data[pointer:pointer+2]
        meassures_count += 1
    return measures


if __name__ == "__main__":


    with open("/root/server_WMB/wmb_server.txt") as f:
        lines = f.readlines()

        for line in lines:
            if "Raw frame:" in line:
                raw_frames = line.split(":")[1].strip()
            elif "Clean frame:" in line:
                clean_frames = line.split(":")[1].strip()

                print("Raw frames:", raw_frames)
                print("Clean frames:", clean_frames)


                msgDec = aesSingleDecoder(clean_frames, KEY)
                meassures = []
                for i in msgDec:
                    my_bytes = i["value"]
                    if i["units"] == "13" or i["units"] == "3b":
                        div = 1000
                    else:
                        div = 10
                    meassures.append(meassureToFloat(my_bytes, div))
                sensor_id = clean_frames[-102:-94]
                print(meassures)
                mysqlSet.insertDates(866897040458269,datetime.datetime.fromtimestamp(int(raw_frames[2:10],16)).strftime("%Y-%m-%d %H:%M:%S"), meassures[0], meassures[1],
                                                meassures[2], meassures[3], sensor_id)
                print(datetime.datetime.fromtimestamp(int(raw_frames[2:10],16)).strftime("%Y-%m-%d %H:%M:%S"))