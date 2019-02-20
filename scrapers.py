import time
# for backward compatibility
from six.moves.urllib.request import urlopen

import bs4
import newspaper
import lxml
from lxml.html.clean import Cleaner
from async_worker_helper import findAndFilterTag

def raw_scraper(url):
    t1 = time.time()
    try:
        cleaner = Cleaner()
        cleaner.javascript = True
        cleaner.style = True
        html = lxml.html.tostring(lxml.html.parse(url))
        html = cleaner.clean_html(html)
    except:
        return None, None

    metadata = {
        "url": url,
        "elapsed": time.time() - t1,
        "scraper": "raw",
    }
    return html, metadata


def newspaper_scraper(url):
    t1 = time.time()

    # try:
    #     article = newspaper.Article(url)
    #     article.download()
    #     article.parse()
    #     text = article.text
    #     count = len(text.split())
    # except: #newspaper.article.ArticleException:
    #     return None, None

    article = newspaper.Article(url)
    article.download()
    article.parse()
    text = article.text
    count = len(text.split())

    metadata = {
        "url": url,
        "word_count": count,
        "elapsed": time.time() - t1,
        "scraper": "newspaper",
    }
    return text, metadata

def bs4_scraper(url):
    t1 = time.time()

    try:
        result = urlopen(url)
        raw = result.read()        
        size = len(raw) / 1000.0 / 1000.0
        soup = bs4.BeautifulSoup(raw, "lxml")
        text, count = findAndFilterTag("p", soup)
        # DDB: keep text as a single string for consistency with
        # newspaper_scraper
        text = " ".join(text)
    except:
        return None, None

    metadata = {
        "url": url,
        "status": result.status,
        "word_count": count,
        "elapsed": time.time() - t1,
        "size": size,
        "scraper": "bs4",
    }
    return text, metadata
