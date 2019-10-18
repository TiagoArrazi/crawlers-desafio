from bs4 import BeautifulSoup
from requests import ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError, get
import sys
import csv
import os
import datetime
from subprocess import Popen, PIPE

def value(soup):

    tag = soup.find("span", class_ = "pid-2103-last")
    return tag.get_text().strip()


def currency(soup):

    tag = soup.find("h1", class_ = "instrumentH1inlineblock")
    return tag.get_text().strip()


def variation(soup):

    tagVar = soup.find("i", class_ = "pid-2103-pc")
    return tagVar.get_text().strip()


def percentage(soup):

    tagPerc = soup.find("i", class_ = "pid-2103-pcp")
    return tagPerc.get_text().strip()


def get_timestamp(requestString):

    date = requestString.headers["Date"][:-4]
    timestamp = ''.join([str(t).zfill(2)
                         for t in list(datetime.datetime.strptime(date, "%a, %d %b %Y %H:%M:%S").timetuple())[:-6]])
    return timestamp


def get_curr_timestamp():

    return ''.join([str(t).zfill(2)
                    for t in list(datetime.datetime.now().timetuple())[:-6]])


if __name__ == '__main__':

    url = 'https://m.investing.com/currencies/usd-brl'
    os.chdir('~/tiagoArrazi/crawler_dolar')
    filename = "dolar_{}.csv".format(get_curr_timestamp())

    try:
        requestString = get(url=url, headers={'User-Agent':'curl/7.52.1'})
        soup = BeautifulSoup(requestString.text, "html.parser")

        with open(filename, 'w+') as f:
            w = csv.writer(f, delimiter=';')

            if os.stat(filename).st_size == 0:
                w.writerow(['currency', 'value', 'change', 'perc', 'timestamp'])

            w.writerow([currency(soup), value(soup), 
                        variation(soup), percentage(soup), 
                        get_timestamp(requestString)])

    except (ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError):
        print("Couldn't connect, quitting!")
        sys.exit()



