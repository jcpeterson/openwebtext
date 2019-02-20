import os, argparse, time, json, glob
import mimetypes
import multiprocessing as mp

import bs4
import newspaper

from async_worker_helper import findAndFilterTag

# for backward compatibility
from six.moves.urllib.request import urlopen

parser = argparse.ArgumentParser()
parser.add_argument("url_file", type=str)
parser.add_argument("--n_threads", type=int, default=1)
parser.add_argument("--max_urls", type=int, default=-1)
parser.add_argument("--scraper", type=str, default="bs4")
args = parser.parse_args()


def init_output_dirs():
    if not os.path.exists("data/parsed"):
        os.makedirs("data/parsed")

    if not os.path.exists("data/raw"):
        os.makedirs("data/raw")

    if not os.path.exists("data/meta"):
        os.makedirs("data/meta")


# https://stackoverflow.com/questions/21515098/how-to-check-the-url-is-either-web-page-link-or-file-link-in-python
def get_link_type(link, strict=True):
    link_type, _ = mimetypes.guess_type(link)
    if link_type is None and strict:
        # this will return None if the url is invalid, at the expense of also
        # loading the freakin url... :-/
        try:
            u = urlopen(link)
            link_type = u.headers["Content-Type"]
        except:
            link_type = None
    return link_type


def newspaper_scraper(url):
    text, count = {}, 0
    raw, status = None, None
    t1 = time.time()

    try:
        article = newspaper.Article(url)
        article.download()
        article.parse()
        text = article.text
        count = len(text.split())
    except newspaper.article.ArticleException:
        return None, None, None

    # DDB: for consistency with bs4_scraper. can probably remove
    try:
        result = urlopen(url)
        status = result.status
        if status == 200:
            raw = result.read()
            size = len(raw) / 1000.0 / 1000.0
    except:
        pass

    metadata = {
        "url": url,
        "status": status,
        "word_count": count,
        "elapsed": time.time() - t1,
        "size": size,
        "scraper": "newspaper",
    }
    return text, metadata, raw


def bs4_scraper(url):
    t1 = time.time()
    try:
        result = urlopen(url)
    except:
        return None, None, None

    text, count, size = None, 0, None
    if result.status == 200:
        raw = result.read()
        size = len(raw) / 1000.0 / 1000.0
        soup = bs4.BeautifulSoup(raw, "lxml")
        text, count = findAndFilterTag("p", soup)

        # DDB: keep text as a single string for consistency with
        # newspaper_scraper
        text = " ".join(text)

    metadata = {
        "url": result.url,
        "status": result.status,
        "word_count": count,
        "elapsed": time.time() - t1,
        "size": size,
        "scraper": "bs4",
    }

    return text, metadata, raw


def download(url_entry, scraper="bs4"):
    uid, url = url_entry
    url = url.strip()
    link_type = get_link_type(url)
    print("Link type: {}".format(link_type))

    if link_type is None or "text/html" not in link_type:
        return

    if args.scraper == "bs4":
        text, meta, raw = bs4_scraper(url)
    elif args.scraper == "newspaper":
        text, meta, raw = newspaper_scraper(url)

    if text is None or text.strip() == "":
        return

    fid = "{:07d}-{}".format(uid, hash(url.encode()))
    parsed_fp = "data/parsed/{}.txt".format(fid)
    raw_fp = "data/raw/{}.txt".format(fid)
    meta_fp = "data/meta/{}.json".format(fid)

    with open(parsed_fp, "w") as out:
        out.write(text)
    with open(raw_fp, "wb") as out:
        out.write(raw)
    with open(meta_fp, "w") as out:
        json.dump(meta, out)

    return


def load_urls(completed_fids):
    with open(args.url_file) as fh:
        url_entries = [
            (fid, url) for (fid, url) in enumerate(fh) if fid not in completed_fids
        ]
        if args.max_urls != -1:
            url_entries = url_entries[: args.max_urls]
    return url_entries


def get_completed_fids():
    parsed_fid, raw_fid, meta_fid = set(), set(), set()
    for ff in glob.glob("data/parsed/*.txt"):
        parsed_fid.add(int(os.path.split(ff)[-1].split("-")[0]))
    for ff in glob.glob("data/raw/*.txt"):
        raw_fid.add(int(os.path.split(ff)[-1].split("-")[0]))
    for ff in glob.glob("data/meta/*.json"):
        meta_fid.add(int(os.path.split(ff)[-1].split("-")[0]))
    return parsed_fid.intersection(raw_fid).intersection(meta_fid)


if __name__ == "__main__":
    init_output_dirs()
    completed_fids = get_completed_fids()
    url_entries = load_urls(completed_fids)

    print("Downloading...")
    t1 = time.time()
    p = mp.Pool(args.n_threads)  # num of download threads
    list(p.imap(download, url_entries))
    total_time = time.time() - t1

    print("Total time: ", str(int(total_time)), " seconds")
    print("Done!")
