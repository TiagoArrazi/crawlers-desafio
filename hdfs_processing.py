from subprocess import call
from datetime import datetime
from os import chdir, path


def get_current_timestamp(date_string, n):

    return ''.join(['0' + str(t)
                     if t in list(range(1,10))
                     else str(t)
                     for t in list(date_string.timetuple())[:-n]])


curr_date = datetime.now()

##========================================== MOVING FILES TO HDFS =========================================##

crypto_filename = path.expanduser("~/tiagoArrazi/crawler_crypto/consolidados/crypto_{}.csv".format(get_current_timestamp(curr_date, 4)))
dolar_filename = path.expanduser("~/tiagoArrazi/crawler_dolar/dolar_{}.csv".format(get_current_timestamp(curr_date, 6))

zipname = path.expanduser("~/tiagoArrazi/crawler_dolar/transferidos/transferidos_{}.zip".format(get_current_timestamp(curr_date, 6)))

call("hdfs dfs -put {} /user/tiagoArrazi/input".format(crypto_filename), shell=True)
call("hdfs dfs -put {} /user/tiagoArrazi/input".format(dolar_filename), shell=True)
call("zip -m {} {} {}".format(zipname, crypto_filename, dolar_filename), shell=True)

##========================================== CALLING SPARK PROCESS =========================================##

dolar_filename = '/user/tiagoArrazi/input/dolar_{}.csv'.format(get_current_timestamp(curr_date, 4))
crypto_filename = '/user/tiagoArrazi/input/crypto_{}.csv'.format(get_current_timestamp(curr_date, 6))
jar_file = 'hdfs://localhost:8020/user/tiagoArrazi/spark_process_2.11-0.1.0-SNAPSHOT.jar'

call("spark-submit --deploy-mode cluster --class Main --master yarn-cluster {} {} {}".format(jar_file, crypto_filename, dolar_filename),
      shell=True)

##========================================== GET JSON FROM HDFS =========================================##

fs_dir = path.expanduser("~/tiagoArrazi/processados_json"))
hdfs_dir = "/user/tiagoArrazi/output/transferidos/processado_data.json/*.json"
hdfs_dir_to_delete = "/user/tiagoArrazi/output/transferidos/*"

call("hdfs dfs -get {} {}".format(hdfs_dir, fs_dir), shell=True)
call("hdfs dfs -rm -R {}".format(hdfs_dir_to_delete), shell=True)


