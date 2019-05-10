from subprocess import Popen, PIPE


where_it_should_be = "{}/tiagoArrazi/processados_json".format(Popen("pwd", shell=True, stdout=PIPE).communicate())
where_it_came_from = "/user/tiagoArrazi/output/transferidos/processado_data.json/*.json"

Popen("hdfs dfs -get {} {}".format(where_it_came_from, where_it_should_be), shell=True).communicate()


