from subprocess import Popen, PIPE


def get_current_timestamp():

    return ''.join(['0' + str(t)
                     if t in list(range(1,10))
                     else str(t)
                     for t in list(datetime.datetime.now().timetuple())[:-6]])


crypto_filename = "{}/tiagoArrazi/crawler_crypto/consolidados/crypto_{}.csv".format(Popen('pwd', stdout=PIPE).communicate()[0][:-1].decode('utf-8'), get_current_timestamp())
dolar_filename = "{}/tiagoArrazi/crawler_dolar/dolar_{}.csv".format(Popen('pwd', stdout=PIPE).communicate()[0][:-1].decode('utf-8'), get_current_timestamp())

zipname = "{}/tiagoArrazi/crawler_dolar/transferidos/transferidos_{}.zip".format(Popen('pwd', stdout=PIPE).communicate()[0][:-1].decode('utf-8'), get_current_timestamp())

moving_dolar_to_hdfs = Popen("hdfs dfs -put /user/tiagoArrazi/input", shell=True)
moving_crypto_to_hdfs = Popen("hdfs dfs -put /user/tiagoArrazi/input", shell=True)
compress = Popen("zip -m {} {} {}".format(zipname, crypto_filename, dolar_filename), shell=True)

moving_dolar_to_hdfs.communicate()
moving_crypto_to_hdfs.communicate()
compress.communicate()
