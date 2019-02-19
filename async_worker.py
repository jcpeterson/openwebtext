from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import react
from requests_threads import AsyncSession
import os
import sys
import bs4
import time
import pandas as pd
import argparse
import pickle
import numpy as np
import unicodedata	
import warnings

parser = argparse.ArgumentParser()
parser.add_argument('--url_file', type=str)
parser.add_argument('--num_urls', type=int, default=1000)
args = parser.parse_args()

session = AsyncSession(n=10)

url_table = pd.read_table(args.url_file).tail(args.num_urls) 	
url_table.columns =['url']
url_table['retrieval_index'] = range(url_table.shape[0])

metadata_collection = []
text_collection = []

def process(result):

	print('Processing return data for '+result.url)

	t1 = time.time()
	metadata = {'url': result.url}
	metadata['status'] = result.status_code

	if result.status_code == 200:
		metadata['size'] = len(result.content) / 1000. / 1000.
		soup = bs4.BeautifulSoup(result.content, 'lxml')			
		text = {}
		text['p'], p_count = findAndFilterTag('p', soup)		

	else:
		text = {}
		p_count = 0

	metadata['count'] = np.sum(p_count)
	metadata['elapsed'] = time.time() - t1

	metadata_collection.append(metadata)
	text_collection.append(text)
	return({'succcess':1})


def findAndFilterTag(tag, soup):
	'''tag specific filter logic'''

	candidates = soup.find_all(tag)
	candidates = [unicodedata.normalize("NFKD", x.string) for x in candidates if x.string is not None]

	if tag == 'p':
		candidates = [y.strip() for y in candidates if len(y.split(' ')) >= 4]
		count = np.sum(len(y.split(' ')) for y in candidates) 
	else:
		raise NotImplementedError

	return(candidates, count)

@inlineCallbacks
def main(reactor):
    t1 = time.time()
    responses = []
    for record in url_table.to_records('dict'):
        responses.append(session.get(record['url'], verify=False))
        

    for response in responses:
        try:
            r = yield response
            process(r)
        except:
            print('issue with '+record['url'])

    metadata_df = pd.DataFrame(metadata_collection)
    pickle.dump( metadata_df, open( "metadata_df.p", "wb" ) )
    pickle.dump( text_collection, open( "text_collection.p", "wb" ) )

    total_time = time.time() - t1
    print('Total time: '+str(np.round(total_time, 3))+' seconds')

    total_transferred = np.nansum(metadata_df['size'])
    print('Total transferred: '+str(np.round(total_transferred, 3))+' MB')	

    total_words = np.nansum(metadata_df['count'])
    print('Total tokens: '+str(np.round(total_words, 3))+' tokens')	
    
    import pdb
    pdb.set_trace()

if __name__ == '__main__':
    react(main)




