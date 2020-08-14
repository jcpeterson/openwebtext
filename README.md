# OpenWebText

[Joshua Peterson](https://twitter.com/joshuacpeterson), [Stephan Meylan](https://stephanmeylan.com), & [David Bourgin](https://http://dvaidbourign.com)

Open clone of OpenAI's unreleased WebText dataset ([blog](https://blog.openai.com/better-language-models/), [paper](https://d4mucfpksywv.cloudfront.net/better-language-models/language_models_are_unsupervised_multitask_learners.pdf), [code](https://github.com/openai/gpt-2)) scraper used to train GPT-2. The current result is just over 23 million URLs and over 10 million HTML pages.

This implementation mines and intelligently de-duplicates +3 karma URLs from pre-downloaded (monthly) pushshift.io Reddit submission dumps (which is much faster than making successive calls to the web API), downloads raw HTML, and extracts text. To save time, you can use the pre-filtered URL lists [here](https://mega.nz/#F!EZZD0YwJ!9_PlEQzdMVLaNdKv_ICNVQ), which reduce the 140GB of pushshift data to down to the 2GB of URLs actually needed for content scraping. There's also an initial utility for tokenizing and we are looking to add BPE encoding soon. This code base is functional but in active development so please feel free to post issues or suggest improvements (pull requests welcome).

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
You can download the pre-filtered URLs [here](https://mega.nz/#F!EZZD0YwJ!9_PlEQzdMVLaNdKv_ICNVQ), but if you want to re-filter them yourself, perhaps with different filtering criteria, follow these instructions. Pushshift dumps must first be downloaded using `fetch_urls.py` (thanks to [simonfall](https://github.com/simonfall)), or manually from [here](https://files.pushshift.io/reddit/submissions/). Two example dumps are included in the repo in the "pushshift_dumps" folder. Next, extract good URLs using:
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
The output of both `extract_urls.py` and `deduplicate_urls.py` are text files given that all 23 million "good" URLs only comprise 2GB.

### To Scrape HTML (or Text Directly)
This is done one month at a time given the compute/bandwidth required. `n_procs` is the number of cores to use for parallelization and should be at least `20-40` for fastest results. The script will output results in chunks of size `chunk_size`. If `timeout` is not set, or is set to `-1`, the downloader may hang on large files.

To scrape raw HTML for later processing and text extraction, set `--scraper` to `raw` as shown below. The downloaded HTML is stripped of script/style tags and stored in compressed archives using LZMA compression, along with a small amount of meta.
```
python download.py url_dumps_deduped/RS_20XX-XX.xz.deduped.txt --n_procs 100 --scraper raw --chunk_size 100000 --compress --timeout 30
```
To scrape text content directly and save disk space (but without the option to re-extract with different parameters later), set `--scraper` to `newspaper` to extract text using the Python [newspaper](https://github.com/codelucas/newspaper) package. For more careful extraction, set `--scraper` to `bs4` ([Beautiful Soup 4](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)), which will extact text for all `<p>` tags on the page.

### To Extract Text from HTML (After Download)
```
python extract_text.py --html_archive scraped/RS_20XX-XX-X_data.xz --n_procs 100 
```
This currently uses [newspaper](https://github.com/codelucas/newspaper) and outputs txt files.

### Tokenization
The original WebText didn't use tokenization, but if you need it use:
```
python tokenize_text.py --input_glob "parsed/*.txt" --output_dir tokenized
```
This will be improved and parallelized soon.

### BPE Encoding
Coming soon...

### Original OpenAI project links
* Blog Post [(Better Language Models and Their Implications)](https://blog.openai.com/better-language-models/)
* Paper [(Language Models are Unsupervised Multitask Learners)](https://d4mucfpksywv.cloudfront.net/better-language-models/language_models_are_unsupervised_multitask_learners.pdf)
* Code [(https://github.com/openai/gpt-2)](https://github.com/openai/gpt-2)

### Other Implmentations
An alternative scraper based on the pushshift.io API and fork of the download code above can be found [here](https://github.com/eukaryote31/openwebtext)
