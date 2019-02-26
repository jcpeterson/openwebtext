# OpenWebText

[Joshua Peterson](http://joshpeterson.io), [Stephan Meylan](https://stephanmeylan.com), & David Bourgin

Open clone of [OpenAI's unreleased WebText dataset](https://blog.openai.com/better-language-models/) scraper (started via [this reddit post](https://www.reddit.com/r/MachineLearning/comments/aqzjv1/d_open_alternative_reddit_scraper_inspired_by/)). It mines URLs from pre-downloaded (monthly) pushshift.io submission dumps instead of the API for speed.

All scraped data can currently be found [here](https://mega.nz/#F!EZZD0YwJ!9_PlEQzdMVLaNdKv_ICNVQ). 
* The URLs folder contains text dumps of all outbound reddit links with at least 3 karma (NSFW currently not included). After removal of non-html files/media and URL de-duplication, there are just over 23 million links spanning 2005 to 2018, a reduction from 150GB in the original pushdump archives to 2GB.
* The HTML folder contains most of the raw html downloads from each of the links above with style and script tags removed. The rest of this data is currently being uploaded.

Original OpenAI project links:
* Blog Post [(Better Language Models and Their Implications)](https://blog.openai.com/better-language-models/)
* Paper [(Language Models are Unsupervised Multitask Learners)](https://d4mucfpksywv.cloudfront.net/better-language-models/language_models_are_unsupervised_multitask_learners.pdf)
* Code [(https://github.com/openai/gpt-2)](https://github.com/openai/gpt-2)

An alternative scraper based on the pushshift.io API can be found [here](https://github.com/eukaryote31/openwebtext)

### Dependencies
If you use pipenv (`pip install --user pipenv`), cd to the project root and run
```
pipenv install 
pipenv shell
```
Otherwise, just run the following in a new virtual environment
```
pip3 install -r requirements.txt
```

### To Extract/Clean URLs Yourself
Pushshift dumps must be downloaded from [here](https://files.pushshift.io/reddit/submissions/) (auto-downloader to be added soon). Two examples are included in the repo in the "pushshift_dumps" folder. Then, extract good URLs using:
```
python extract_urls.py --single_file RS_v2_2005-06.xz
```
To process multiple pushshift files, specify year ranges:
```
python extract_urls.py --year_start 2016 --year_end 2018
```
To change the karma threshold:
```
python extract_urls.py --single_file RS_v2_2005-06.xz --min_karma 4
```
To de-duplicate the extracted URLs, provide a directory of all URL dumps:
```
python deduplicate_urls.py --input_dir url_dumps
```

### To Scrape HTML
This is done one month at a time given the time/bandwidth required. `n_procs` is the number of cores to use for parallelization and should be at least `20-40` for fastest results. The script will output results in chunks of size `chunk_size`. If `timeout` is not set, or is set to `-1`, the downloader may hang on large files.
```
python download.py url_dumps_deduped/RS_20XX-XX.xz.deduped.txt --n_procs 100 --scraper raw --chunk_size 100000 --compress --timeout 30
```