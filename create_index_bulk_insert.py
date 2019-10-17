from subprocess import call
from datetime import datetime
import json


def get_current_timestamp(date_string):

    return '-'.join([str(t).zfill(2)
                     for t in list(date_string.timetuple())[:-6]])

curr_date = datetime.now()


##=============================================== INDEX CREATION =======================================================##

index_json = json.dumps({

    "settings": {
        "number_of_shards": 1, 
        "number_of_replicas": 0
        },
    "mapping": {
        "properties": {
            "code": {"type": "integer"},
            "name": {"type": "string"},
            "priceUSD": {"type": "double"},
            "priceBRL": {"type": "double"},
            "priceBTC": {"type": "double"},
            "change24H": {"type": "string"},
            "volume24H": {"type": "string"},
            "timestamp": {"type": "string"}
            }
        }
    }
)

index_json_alias = json.dumps({
    
    "actions": [
        {"add": {
            "index": 'crypto_{}'.format(get_timestamp(curr_date)),
            "alias": "crypto"
            }}
        ]
    
    })

call("curl -XPUT 'localhost:9200/crypto_{}/' -H 'Content-Type: application/json' -d '{}'"
        .format(get_current_timestamp(curr_date), index_json), shell=True)
call("curl -XPOST 'localhost:9200/_aliases' -H 'Content-Type: application/json' -d '{}'"
        .format(index_json_alias), shell=True)

##=============================================== BULK INSERT =======================================================##

chdir(path.expanduser("~/tiagoArrazi/processados_json/processado_data.json/"))
json_list = glob.glob('*.json')
my_json = list()

for js in json_list:
    with open(js, 'r') as fjs:
        for line in fjs.readlines():
            my_json.append(json.dumps({"create": {}}))
            my_json.append(json.dumps(json.loads(line)))

my_json_data = json.dumps('\n'.join(my_json))

bulk_insert = "curl -XPOST localhost:9200/crypto_{}/_bulk -H 'Content-Type: application/json' -d '{}'".format(get_timestamp(curr_date), my_json_data)
call(bulk_insert, shell=True)

