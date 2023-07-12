import mysqlGet
import mysqlSet
import datetime
import requests
import json
from datetime import timedelta
import datetime

class JsonMobile:

    def __init__(self, success="", message="", data={}):
        self.json_data = {
            "success":success,
            "message":message,
            "data":data
        }

    def set_value(self, key, value):
        self.json_data[key] = value
        
    def get_value(self, key):
        return self.json_data[key]
    
    def get_json(self):
        return json.dumps(self.json_data)