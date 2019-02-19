def splitDataFrameIntoSmaller(df, chunkSize = 10000): 
    listOfDf = list()
    numberChunks = len(df) // chunkSize + 1
    for i in range(numberChunks):
        listOfDf.append(df[i*chunkSize:(i+1)*chunkSize])
    return listOfDf


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