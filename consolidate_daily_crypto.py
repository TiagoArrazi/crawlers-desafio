import glob
import subprocess
import pandas as pd
import os

from datetime import datetime

def get_timestamp(date_string):
    return ''.join([str(t).zfill(2) 
                    for t in date_string.timetuple()[:-6]])


os.chdir(os.path.expanduser('~/tiagoArrazi/crawler_crypto'))
filename = 'consolidate_crypto_{}.csv'.format(get_timestamp(datetime.now()))
all_data = glob.glob('*.csv')

try:
    df = pd.concat([pd.read_csv(f) for f all_data], ignore_index=True)

except ValueError:
    df = pd.Dataframe()

os.chdir(os.path.expanduser('~/tiagoArrazi/crawler_crypto/consolidados'))

df.to_csv(filename, sep=';', index=False)

subprocess.call("sed -i 's/\"//g' {}".format(filename), shell=True)

os.chdir(os.path.expanduser('~/tiagoArrazi/crawler_crypto'))

subprocess.call('zip -u ../processados/processados.zip {}'.format(' '.join(all_data)), shell=True)

