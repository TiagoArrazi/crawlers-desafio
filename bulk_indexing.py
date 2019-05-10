from subprocess import Popen, PIPE

def get_timestamp(date=datetime.now()):
    return ''.join(['0' + str(t)
                     if t in list(range(1,10))
                     else str(t)
                     for t in list(date.timetuple())[:-6]])


path = "{}/tiagoArrazi/processados_json/processado_data.json/".format(Popen("pwd", shell=True, stdout=PIPE).communicate())
json_file = Popen("ls {}| grep part*.json".format(path), shell=True, stdout=PIPE).communicate()
my_json = ""

with open(json_file, 'r') as js:
    for line in js:
        my_json = "{\"index\":{}}\n" + line + "\n"

bulk_insert = "curl -XPOST localhost:9200/crypto_{}/_bulk -d '{}'".format(get_timestamp(), my_json)

Popen(bulk_insert, shell=True).communicate()

