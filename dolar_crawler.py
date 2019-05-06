from bs4 import BeautifulSoup
from requests import ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError, get
import sys
import csv
import os
import datetime
from subprocess import Popen, PIPE

def beautifulValueScraper(soup):

    tag = soup.find("span", class_ = "pid-2103-last")
    return tag.get_text().strip()


def beautifulCurrencyScraper(soup):

    tag = soup.find("h1", class_ = "instrumentH1inlineblock")
    return tag.get_text().strip()


def beautifulVariationScraper(soup):

    tagVar = soup.find("i", class_ = "pid-2103-pc")
    return tagVar.get_text().strip()


def beautifulPercentageScraper(soup):

    tagPerc = soup.find("i", class_ = "pid-2103-pcp")
    return tagPerc.get_text().strip()


def getTimestamp(requestString):

    date = requestString.headers["Date"][:-4]
    timestamp = ''.join(['0' + str(t) 
                          if t in list(range(1,10)) 
                          else str(t) 
                          for t in list(datetime.datetime.strptime(date, "%a, %d %b %Y %H:%M:%S").timetuple())[:-6]])
    return timestamp


def getCurrentTimestamp():

    return ''.join(['0' + str(t) 
                     if t in list(range(1,10)) 
                     else str(t) 
                     for t in list(datetime.datetime.now().timetuple())[:-6]])


if __name__ == '__main__':

    url = 'https://m.investing.com/currencies/usd-brl'
    filename = "{}/tiagoArrazi/crawler_dolar/dolar_{}.csv".format(Popen('pwd', stdout=PIPE).communicate()[0][:-1].decode('utf-8'), getCurrentTimestamp())

    try:
        requestString = get(url = url, headers = {'User-Agent':'curl/7.52.1'})
        soup = BeautifulSoup(requestString.text, "html.parser")

        with open(filename, 'a+') as f:
            w = csv.writer(f, delimiter=';')

            if os.stat(filename).st_size == 0:
                w.writerow(['currency', 'value', 'change', 'perc', 'timestamp'])

            w.writerow([beautifulCurrencyScraper(soup), beautifulValueScraper(soup), 
                        beautifulVariationScraper(soup), beautifulPercentageScraper(soup), 
                        getTimestamp(requestString)])

    except (ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError):
        print("Couldn't connect, quitting!")
        sys.exit()



