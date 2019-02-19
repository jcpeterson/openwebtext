# simple getter scraper 

import os
import sys
import bs4
import requests
import time
import pandas as pd
import argparse
import pickle
import numpy as np
import unicodedata
# read the input file list

parser = argparse.ArgumentParser()
parser.add_argument('--url_file', type=str)
parser.add_argument('--num_urls', type=int, default=1000)
args = parser.parse_args()


def scrape(url):
	''' main request and status code handling'''
	t1 = time.time()
	metadata = {'url': url}
	try:
		result = requests.get(url, timeout=5)
	except:
		print('Timeout!')
		metadata['status'] = 'timeout'
		text = {}		
		return(metadata, text)

	metadata['status'] = result.status_code
	print(result.status_code)

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
	#print(rdf)
	return(metadata, text)


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



if __name__ == '__main__':

	url_table = pd.read_table(args.url_file).tail(args.num_urls) 	
	url_table.columns =['url']
	url_table['retrieval_index'] = range(url_table.shape[0])

	metadata_collection = []
	text_collection = []

	for record in url_table.to_records('dict'):
		print('Getting url #'+str(record['retrieval_index'])+': '+ record['url'])
		metadata, text  = scrape(record['url'])
		metadata_collection.append(metadata)
		text_collection.append(text)



	metadata_df = pd.DataFrame(metadata_collection)
	
	pickle.dump( metadata_df, open( "metadata_df.p", "wb" ) )
	pickle.dump( text_collection, open( "text_collection.p", "wb" ) )

	
	# a wee bit of reporting
	total_time = np.sum(metadata_df.elapsed)
	print('Total time: '+str(np.round(total_time, 3))+' seconds')

	total_transferred = np.nansum(metadata_df['size'])
	print('Total transferred: '+str(np.round(total_transferred, 3))+' MB')	

	total_words = np.nansum(metadata_df['count'])
	print('Total tokens: '+str(np.round(total_words, 3))+' tokens')	
		
	print('Status Code count')
	metadata_df['status'].value_counts()

	import pdb
	pdb.set_trace()	


#[X] estimate the size of the html
#[X] get the total duration 
#[ ] could add a word count
#[X]  \xa0, \ and text encoding