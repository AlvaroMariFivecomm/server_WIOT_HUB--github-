import json

# 00 46 25 1E44AE4C929005406807A0697A2D301005C3B5AE0DB7BE0D3B016622A7F822D4338BE4DDC5 01 46 25 1E44AE4C929005406807A0697A1E30100584E16A2DE07A30FAA0DCFD2B4B20D93C770217970246251E44AE4C929005406807A0697A0E30100563908CF6B031EA58F1D9383EDB1679E337F414C80345251E44AE4C929005406807A0697AFE301005A3A5AB65C756CA1DFF396D86DFB13B49F4A3DE8A0446251E44AE4C929005406807A0697AED30100551B56461ED20DC7BE1178AF004039103BCECB5D10546251E44AE4C929005406807A0697ADE30100586636A584C04AB49223B2DD35308EFB7320CF3730645251E44AE4C929005406807A0697ACD301005799292D7C1C0A530AAE892ADF25FB560672C49C90745251E44AE4C929005406807A0697ABD3010053CE8CCB587B0CFDB85598D3D11BBFDB4F463293E0845251E44AE4C929005406807A0697AAD301005BCBFDA69CC1291F5025F022D321FCF26DB01FF8F0945251E44AE4C929005406807A0697A9D301005C81A2B6942B34682EB3D94DF273C68BBB66CFEF80A45251E44AE4C929005406807A0697A8D301005D6ACEABBB0BFB3A771A0C064E9C6F423CB4E976A0B45251E44AE4C929005406807A0697A7D30100589B13FA6DD39D4978EAD4A3B2F7F72BB37B943630C45251E44AE4C929005406807A0697A6D301005C2E32B8E6CBD1DBF9F2EA01C9215714E9DCDB7C40D45251E44AE4C929005406807A0697A5D301005367CB0A2026D6F8087A419BFE21FC42C8820B2BD0E45251E44AE4C929005406807A0697A4E3010050F237109CC23D7AC0B136DB92EAA6F7D71821F6D0F45251E44AE4C929005406807A0697A3D3010057AF29BE284463FDB5624862ABF53B3EA3C691BF91045251E44AE4C929005406807A0697A2D301005C3B5AE0DB7BE0D3B016622A7F822D4338BE4DDC51145251E44AE4C929005406807A0697A1D301005E993E03331E7C60EE0B308B2985DC7C4D9D45E861246251E44AE4C929005406807A0697A0D3010051169296B1705E29D65817F698C6D7183343759591344251E44AE4C929005406807A0697AFD301005FC1E33EA8F15672117A0E4A455D9F8CB8823BCBF1445251E44AE4C929005406807A0697AED30100551B56461ED20DC7BE1178AF004039103BCECB5D11545251E44AE4C929005406807A0697ADD3010057531BF924FCFD6AC3DE628D29376FA5E5CE20A601645251E44AE4C929005406807A0697ACD301005799292D7C1C0A530AAE892ADF25FB560672C49C91748251E44AE4C929005406807A0697ABD3010053CE8CCB587B0CFDB85598D3D11BBFDB4F463293E

def parse_trama(trama):
    trama_binary = bin(int(trama, 16))[2:].zfill(len(trama)*4)
    datos_json = {}
    i = 0
    
    while i < len(trama_binary):
        hora = trama[i:i+2]
        if hora != '':
            rssi = int(trama[i+2:i+4], 16)
            longitud = int(trama[i+4:i+6],16)
            dato = trama[i+6:i+6+longitud*2]

            datos_json[f"hora{str(int(hora, 16)).zfill(2)}"] = {  
                        "RSSI": rssi,
                        "datos": dato
                    }
        i += 6 + longitud*2

    return json.dumps(datos_json, indent=4)

msg = "0046251E44AE4C929005406807A0697A2D301005C3B5AE0DB7BE0D3B016622A7F822D4338BE4DDC50146251E44AE4C929005406807A0697A1E30100584E16A2DE07A30FAA0DCFD2B4B20D93C770217970246251E44AE4C929005406807A0697A0E30100563908CF6B031EA58F1D9383EDB1679E337F414C80345251E44AE4C929005406807A0697AFE301005A3A5AB65C756CA1DFF396D86DFB13B49F4A3DE8A0446251E44AE4C929005406807A0697AED30100551B56461ED20DC7BE1178AF004039103BCECB5D10546251E44AE4C929005406807A0697ADE30100586636A584C04AB49223B2DD35308EFB7320CF3730645251E44AE4C929005406807A0697ACD301005799292D7C1C0A530AAE892ADF25FB560672C49C90745251E44AE4C929005406807A0697ABD3010053CE8CCB587B0CFDB85598D3D11BBFDB4F463293E0845251E44AE4C929005406807A0697AAD301005BCBFDA69CC1291F5025F022D321FCF26DB01FF8F0945251E44AE4C929005406807A0697A9D301005C81A2B6942B34682EB3D94DF273C68BBB66CFEF80A45251E44AE4C929005406807A0697A8D301005D6ACEABBB0BFB3A771A0C064E9C6F423CB4E976A0B45251E44AE4C929005406807A0697A7D30100589B13FA6DD39D4978EAD4A3B2F7F72BB37B943630C45251E44AE4C929005406807A0697A6D301005C2E32B8E6CBD1DBF9F2EA01C9215714E9DCDB7C40D45251E44AE4C929005406807A0697A5D301005367CB0A2026D6F8087A419BFE21FC42C8820B2BD0E45251E44AE4C929005406807A0697A4E3010050F237109CC23D7AC0B136DB92EAA6F7D71821F6D0F45251E44AE4C929005406807A0697A3D3010057AF29BE284463FDB5624862ABF53B3EA3C691BF91045251E44AE4C929005406807A0697A2D301005C3B5AE0DB7BE0D3B016622A7F822D4338BE4DDC51145251E44AE4C929005406807A0697A1D301005E993E03331E7C60EE0B308B2985DC7C4D9D45E861246251E44AE4C929005406807A0697A0D3010051169296B1705E29D65817F698C6D7183343759591344251E44AE4C929005406807A0697AFD301005FC1E33EA8F15672117A0E4A455D9F8CB8823BCBF1445251E44AE4C929005406807A0697AED30100551B56461ED20DC7BE1178AF004039103BCECB5D11545251E44AE4C929005406807A0697ADD3010057531BF924FCFD6AC3DE628D29376FA5E5CE20A601645251E44AE4C929005406807A0697ACD301005799292D7C1C0A530AAE892ADF25FB560672C49C91748251E44AE4C929005406807A0697ABD3010053CE8CCB587B0CFDB85598D3D11BBFDB4F463293E"
trama_json = parse_trama(msg)
# print(trama_parseada)
print(trama_json)
for hora, valores in json.loads(trama_json).items():
    rssi = valores['RSSI']
    datos = valores['datos']