# /* =====================================================
# * Copyright (c) 2022, Fivecomm - 5G COMMUNICATIONS FOR FUTURE INDUSTRY        VERTICALS S.L. All rights reserved.
# * File_name replyLite.py
# * Description: Frame sender to Lite
# * Author:  Miriam Ortiz <miriam.ortiz@fivecomm.eu>
# * Date:  2023-03-10
# * Version:  0.1
# * =================================================== */

import json

def frameType(resp, args):
    # args [0]: IMEI
    # args [1]: user(WZILM) | reportTime(120) | command('AT+QENG="servingcell"' | 'CMD;1;3')
    # args [2]: pass(U0Sxn) | granularity(60)
    # args [3]: signalThreshold(0)

    if resp == "whitelist":
        whitelist_values = ','.join(str(value) for value in args[1:])

        return ["r" + args[0], "whitelist;" + whitelist_values + ",}"]
    elif resp == "empty_whitelist":
        return ["r" + args, "whitelist;"]
    
    elif resp == "a1":
        return ["r" + args[0], {
            "user": args[1],
            "pass": args[2],
            "topic1": "t" + args[0],
            "topic2": "r" + args[0]
        }]
    elif resp == "sigtech":
        return ["r" + args[0], {
            "reportTime": str(args[1]),
            "granularity": str(args[2]),
            "wmbMode": str(args[3]),
            "update": str(args[4]),
            "allowedManufacturers": str(args[5]),
            "allowedTypes": str(args[6])
        }]
    elif resp == "command":
        return ["r" + args[0], {"command": args[1]}]
    elif resp == "NOTwhitelist":
        return ["r" + args[0], {"whiteList": "not white list in db"}]
    else:
        raise TypeError('The type is not supported!')

def send(resp, args, client):
    try: 
        msg = frameType(resp, args)
        msg[1] = json.dumps(msg[1], ensure_ascii=False)
        result = client.publish(msg[0], msg[1])
        if result.rc != 0 or not result.is_published():
            print("Publish error")
    except Exception as e:
        print("Publish error: " + str(e))
    return client

    
def pubTest(client):
    res = client.publish("testing", "HelloWorld")
# Example calls 
#send("a1",['IMEI', 'WZILM', 'U0Sxn'])
#send("sigtech",['IMEI', 120, 60])
#send("command",['IMEI', 'AT+QENG="servingcell"', None])
#send("adsf1",['IMEI', 'WZILM', 'U0Sxn'])
