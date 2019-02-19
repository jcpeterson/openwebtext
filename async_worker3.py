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
import async_worker_helper

parser = argparse.ArgumentParser()
parser.add_argument('--url_file', type=str)
parser.add_argument('--num_urls', type=int, default=-1)
parser.add_argument('--num_threads', type=int, default=1000)
args = parser.parse_args()

@inlineCallbacks
def main(reactor):
    t1 = time.time()
    responses = []

    session = AsyncSession(n=args.num_threads)

    for record in work_unit.to_records('dict'):
        responses.append(session.get(record['url'], verify=False))        

    for response in responses:
        try:
            r = yield response
            async_worker_helper.process(r)
        except:
            print('issue with '+record['url'])

    return(metadata_collection, text_collection)
    

if __name__ == '__main__':
	
	if args.num_urls > 0:
		url_table = pd.read_table(args.url_file).head(args.num_urls) 	
	else:
		url_table = pd.read_table(args.url_file)
	url_table.columns =['url']
	url_table['retrieval_index'] = range(url_table.shape[0])

	work_units = async_worker_helper.splitDataFrameIntoSmaller(url_table, 100)	

	for i in range(2):
		# can I use scope like this when using an async library
		print('Processing work unit '+str(i))
		work_unit = work_units[i]
		metadata_collection = []
		text_collection = []
		react(main)

		# write out 
		import pdb
		pdb.set_trace()
	

	metadata_df = pd.DataFrame(metadata_collection)
	pickle.dump( metadata_df, open( "metadata_df.p", "wb" ) )
	pickle.dump( text_collection, open( "text_collection.p", "wb" ) )

	total_time = time.time() - t1
	print('Total time: '+str(np.round(total_time, 3))+' seconds')

	total_transferred = np.nansum(metadata_df['size'])
	print('Total transferred: '+str(np.round(total_transferred, 3))+' MB')	

	total_words = np.nansum(metadata_df['count'])
	print('Total tokens: '+str(np.round(total_words, 3))+' tokens')	


#[ ] What is the core usage pattern for async before joblib?
#[ ] Divide the work into m 10k work units
#[ ] save output to a compressed format
#[ ] Core-level parallelization with joblibss
#[ ] Each core should be consuming 10k units



