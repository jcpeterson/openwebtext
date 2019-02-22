import os, glob
from os.path import join, splitext
import argparse
import pandas as pd
from urllib.parse import urlparse, parse_qsl
from urllib.parse import unquote_plus
import html

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
        self.original_url = url

    def __eq__(self, other):
        #test.parts.netloc + test.parts.path
        return ((self.parts.netloc  == other.parts.netloc) and 
                (self.parts.path  == other.parts.path))

    def __hash__(self):
        return hash(self.parts)

parser = argparse.ArgumentParser()
parser.add_argument('--input_dir', type=str, default='url_dumps')
parser.add_argument('--input_glob', type=str, default='*.txt')
args = parser.parse_args()

seen = {}
output = {}

filepaths = glob.glob(join(args.input_dir, args.input_glob))
basenames = [splitext(x)[0] for x in filepaths]
year_month = [x.split('_')[-1][0:7] for x in basenames]
year = [int(x.split('-')[0]) for x in year_month]
month = [int(x.split('-')[1]) for x in year_month]
pathTable = pd.DataFrame({'filepaths':filepaths, 
                          'year':year, 
                          'month':month})
pathTable = pathTable.sort_values(by=['year','month'])

filepaths = pathTable['filepaths']

for filepath in filepaths:
    output[filepath] = []
    print('Processing', filepath, end=' ')

    with open(filepath) as f:
        for url in f:
            try:
                normalized_url = Url(html.unescape(url))
                if normalized_url in seen:
                    # skip it b/c already seen
                    pass
                else:
                   # keep it
                   seen[normalized_url] = filepath
            except:
                print('\nProblem parsing the URL for '+url)
    print('-- done!')

for key, value in seen.items():
    output[value].append(key)

output_folder = args.input_dir + '_deduped'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

for path, url_list in output.items():
    # better renaming logic
    output_path = path.replace('goodlinks.txt','deduped.txt')
    output_path = output_path.replace(args.input_dir, output_folder)
    print('Saving', output_path, end=' ')
    with open(output_path, 'w') as f:
        for url in url_list:
            f.write(url.original_url)
    print('-- done!')