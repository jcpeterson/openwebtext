import glob
import os
import argparse
import pandas as pd
from urlparse import urlparse, parse_qsl
from urllib import unquote_plus

class Url(object):
    '''A url object that can be compared with other url orbjects
    without regard to the vagaries of encoding, escaping, and ordering
    of parameters in query strings.'''

    def __init__(self, url):
        parts = urlparse(url)
        _query = frozenset(parse_qsl(parts.query))
        _path = unquote_plus(parts.path)
        parts = parts._replace(query=_query, path=_path)
        self.parts = parts

    def __eq__(self, other):
        return self.parts == other.parts

    def __hash__(self):
        return hash(self.parts)

parser = argparse.ArgumentParser()
parser.add_argument('--input_glob', type=str, default='*.txt')
args = parser.parse_args()

seen = {}
output = {}

filepaths = glob.glob(args.input_glob)
basenames = [os.path.splitext(x)[0] for x in filepaths]
year_month = [x.split('_')[-1][0:8] for x in basenames]
year = [int(x.split('-')[0]) for x in year_month]
month = [int(x.split('-')[1]) for x in year_month]
pathTable = pd.DataFrame({'filepaths':filepaths, 'year':year, 'month':month})
pathTable = pathTable.sort_values(by=['year','month'])

filepaths = pathTable['filepaths']

for filepath in filepaths:
    output[filepath] = []

    with open(filepath) as f:
        for url in f:
            normalized_url = Url(url)
            if normalized_url in seen:
                # skip it b/c already seen
                pass
            else:
                # keep it
                seen[normalized_url] = filepath

for key,value in seen.items():
    output[value].append(key)

for path,url_list in output.items():
    #better renaming logic
    output_path = path.replace('goodlinks.txt','deduped.txt')
    with open(ouptut_path, 'wb') as f:
        for url in url_list:
            f.write(url)
