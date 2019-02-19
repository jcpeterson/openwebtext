# OpenWebText
Open clone of OpenAI's unreleased WebText dataset scraper (started via [this reddit post](https://www.reddit.com/r/MachineLearning/comments/aqzjv1/d_open_alternative_reddit_scraper_inspired_by/)). It mines URLs from pre-downloaded (monthly) pushshift.io submission dumps instead of the API for speed.

Currently, only the 3+ karma links (30 million from 2006-2018 posts) are available for download [here](https://mega.nz/fm/9BRTBABA)(2GB uncompressed down from the approx. 150GB total set of compressed submissions), but the scraped content will be posted very soon. The URLs are already filtered to exclude audio/video/nsfw/etc.

Original OpenAI project links:
* Blog Post [(Better Language Models and Their Implications)](https://blog.openai.com/better-language-models/)
* Paper [(Language Models are Unsupervised Multitask Learners)](https://d4mucfpksywv.cloudfront.net/better-language-models/language_models_are_unsupervised_multitask_learners.pdf)
* Code [(https://github.com/openai/gpt-2)](https://github.com/openai/gpt-2)

An alternative scraper based on the pushshift.io API can be found [here](https://github.com/eukaryote31/openwebtext)

### Dependencies
```
pip install tldextract
```

### To Scrape URLs Yourself
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


