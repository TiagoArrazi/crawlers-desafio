from subprocess import Popen, PIPE
from sys import argv
from datetime import datetime
import json


def get_current_timestamp():

    return '-'.join(['0' + str(t)
                     if t in list(range(1,10))
                     else str(t)
                     for t in list(datetime.now().timetuple())[:-6]])


index_json = json.dumps({

    "settings": {
        "number_of_shards": 1, 
        "number_of_replicas": 0
        }
    }
)

index_creation = "curl -XPUT 'localhost:9200/crypto_{}/' -H 'Content-Type: application/json' -d '{}'".format(get_current_timestamp(), index_json)


Popen(index_creation, shell=True).communicate()
