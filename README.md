# openwebtext
Open clone of OpenAI's unreleased WebText dataset scraper (started via [this reddit post](https://www.reddit.com/r/MachineLearning/comments/aqzjv1/d_open_alternative_reddit_scraper_inspired_by/)). It mines URLs from pre-downloaded (monthly) pushshift.io submission dumps instead of the API for speed.

Currently, only the 3+ karma links (14 million from 2016-2018 posts) are available for download [here](https://mega.nz/fm/9BRTBABA), but the scraped content will be posted soon. The URLs are already filtered to exclude audio/video/nsfw/etc.

Original OpenAI project links:
* Blog Post [(Better Language Models and Their Implications)](https://blog.openai.com/better-language-models/)
* Paper [(Language Models are Unsupervised Multitask Learners)](https://d4mucfpksywv.cloudfront.net/better-language-models/language_models_are_unsupervised_multitask_learners.pdf)
* Code [(https://github.com/openai/gpt-2)](https://github.com/openai/gpt-2).

An alternative scraper based on the pushshift.io API can be found [here](https://github.com/eukaryote31/openwebtext)
