import time
# for backward compatibility
from six.moves.urllib.request import urlopen

import unicodedata
import bs4
import newspaper
# import lxml
from lxml.html.clean import Cleaner
from htmlmin import minify

def findAndFilterTag(tag, soup):
    '''tag specific filter logic'''

    candidates = soup.find_all(tag)
    candidates = [unicodedata.normalize("NFKD", x.string) for x in candidates if x.string is not None]

    if tag == 'p':
        candidates = [y.strip() for y in candidates if len(y.split(' ')) >= 4]
        count = sum(len(y.split(' ')) for y in candidates) 
    else:
        raise NotImplementedError

    return(candidates, count)

def raw_scraper(url):
    t1 = time.time()

    try:
        cleaner = Cleaner()
        cleaner.javascript = True
        cleaner.style = True
        article = newspaper.Article(url, fetch_images=False)
        article.download()
        html = minify(article.html)
        html = cleaner.clean_html(html)
        article.parse()
    except:
        return None, None
    if article.text == '':
        return None, None

    metadata = {
        "url": url,
        "elapsed": time.time() - t1,
        "scraper": "raw",
    }
    return html, metadata

def newspaper_scraper(url):
    t1 = time.time()

    try:
        article = newspaper.Article(url, fetch_images=False)
        article.download()
        article.parse()
        text = article.text
        count = len(text.split())
    except: #newspaper.article.ArticleException:
        return None, None

    metadata = {
        "url": url,
        "word_count": count,
        "elapsed": time.time() - t1,
        "scraper": "newspaper",
    }
    return text, metadata

def bs4_scraper(url):
    t1 = time.time()

    # slow!
    # try:
    #     result = urlopen(url)
    #     raw = result.read()        
    #     size = len(raw) / 1000.0 / 1000.0
    #     soup = bs4.BeautifulSoup(raw, "lxml")
    #     text, count = findAndFilterTag("p", soup)
    #     # DDB: keep text as a single string for consistency with
    #     # newspaper_scraper
    #     text = " ".join(text)
    # except:
    #     return None, None

    try:
        article = newspaper.Article(url, fetch_images=False)
        article.download()
        html = article.html
        size = len(html) / 1000.0 / 1000.0
        soup = bs4.BeautifulSoup(html, "lxml")
        text, count = findAndFilterTag("p", soup)
        # DDB: keep text as a single string for consistency with
        # newspaper_scraper
        text = " ".join(text)
    except:
        return None, None

    metadata = {
        "url": url,
        # "status": result.status,
        "word_count": count,
        "elapsed": time.time() - t1,
        # "size": size,
        "scraper": "bs4",
    }
    return text, metadata
