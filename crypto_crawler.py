from os import stat, chdir, path
from bs4 import BeautifulSoup
from csv import writer
from datetime import datetime
from requests import ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError, get
from subprocess import call, PIPE, Popen 


def get_timestamp(date=datetime.now()):
    return ''.join([str(t).zfill(2)
                    for t in list(date.timetuple())[:-4]])

chdir(path.expanduser('~/tiagoArrazi/crawler_crypto'))

crypto_Url = "https://m.investing.com/crypto/"

try:
    requestString = get(url = crypto_Url, headers = {'User-Agent':'curl/7.52.1'})

except (ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError):
    print('Couldn\'t connect, quitting!')
    exit(-1)

soup = BeautifulSoup(requestString.text, "html.parser")
content = soup.findAll('tr')
date = get_timestamp(datetime.strptime(requestString.headers['Date'][:-4], '%a, %d %b %Y %H:%M:%S'))
filename = "crypto_{}.csv".format(get_timestamp())

with open(filename, "w+") as f:

        w = writer(f, delimiter = ";")

        if stat(filename).st_size == 0:
            w.writerow(['code', 'name', 'priceUSD', 'change24H', 
                        'change7D', 'symbol', 'priceBTC', 'marketCap', 
                        'volume24H', 'totalVolume', 'timestamp'])

        for c in content[1:]:

            l = c.get_text().replace('\t', '').split('\n')
            L = list(filter(None, l))

            w.writerow([L[0],L[1],L[2],L[3],L[4],L[5],L[6],L[7],L[8],L[9], date])


call("sed -i 's/,//g' {}".format(filename), shell=True)
