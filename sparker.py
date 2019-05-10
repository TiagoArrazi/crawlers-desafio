from subprocess import Popen
from datetime import datetime


def get_current_timestamp():

    return ''.join(['0' + str(t)
                     if t in list(range(1,10))
                     else str(t)
                     for t in list(datetime.now().timetuple())[:-6]])



dolar_filename = '/user/tiagoArrazi/input/dolar_{}.csv'.format(get_current_timestamp())
crypto_filename = '/user/tiagoArrazi/input/crypto_{}.csv'.format(get_current_timestamp())
jar_file = 'spark_process_2.11-0.1.0-SNAPSHOT.jar'

Popen("spark-submit {} {} {}".format(jar_file, crypto_filename, dolar_filename), shell=True).communicate()
