import os, argparse, time, json, glob
import mimetypes
import multiprocessing as mp

# for backward compatibility
# from six.moves.urllib.request import urlopen

from scrapers import *

parser = argparse.ArgumentParser()
parser.add_argument("url_file", type=str)
parser.add_argument("--save_output", action='store_true', default=False)
parser.add_argument("--n_threads", type=int, default=1)
parser.add_argument("--max_urls", type=int, default=-1)
parser.add_argument("--scraper", type=str, default="newspaper")
args = parser.parse_args()

def init_output_dirs():
    # create 'data/parsed' etc...
    subdirs = ['parsed','meta']
    for subdir in subdirs:
        path = os.path.join('data', subdir)
        if not os.path.exists(path):
            os.makedirs(path)

def get_completed_fids():
    parsed_fid, meta_fid = set(), set()
    for ff in glob.glob("data/parsed/*.txt"):
        parsed_fid.add(int(os.path.split(ff)[-1].split("-")[0]))
    for ff in glob.glob("data/meta/*.json"):
        meta_fid.add(int(os.path.split(ff)[-1].split("-")[0]))
    return parsed_fid.intersection(meta_fid)

def load_urls(completed_fids):
    with open(args.url_file) as fh:
        url_entries = [
            (fid, url) for (fid, url) in enumerate(fh) if fid not in completed_fids
        ]
        if args.max_urls != -1:
            url_entries = url_entries[: args.max_urls]
    return url_entries

def vet_link(link):
    # checks link type and status
    # returns if a non-200 status code or
    # the link points to a non-html file
    try:
        info = urlopen(link)
        link_type = info.headers["Content-Type"]
        link_status = info.status
    except:
        link_type = None

    # we want "text/html" only!
    is_good_link = False
    try:
        if ('text/html' in link_type and 
            link_status == 200):
            is_good_link = True
    except:
        pass

    return is_good_link, link_type

def download(url_entry, scraper=args.scraper, 
             save_output=args.save_output):

    uid, url = url_entry
    url = url.strip()
    
    # is_good_link, link_type = vet_link(url)

    # if not is_good_link:
    #     return

    # choose scraper and scrape
    if scraper == "bs4":
        scrape = bs4_scraper
    elif scraper == "newspaper":
        scrape = newspaper_scraper
    elif scraper == "raw":
        scrape = raw_scraper
    text, meta = scrape(url)

    if text is None or text.strip() == "":
        return

    if args.save_output:
        fid = "{:07d}-{}".format(uid, hash(url.encode()))
        parsed_fp = "data/parsed/{}.txt".format(fid)
        meta_fp = "data/meta/{}.json".format(fid)

        with open(parsed_fp, "w") as out:
            out.write(text)
        with open(meta_fp, "w") as out:
            json.dump(meta, out)

    return text

if __name__ == "__main__":
    if args.save_output:
        init_output_dirs()
    completed_fids = get_completed_fids()
    url_entries = load_urls(completed_fids)

    # set up worker pool
    p = mp.Pool(args.n_threads)

    print("Downloading...")
    t1 = time.time()

    # iterating will be needed to dump larger files later
    data = []
    for result in p.imap(download, url_entries):
        # problem links return None instead of content
        if result != None:
            data.append(result)

    total_time = time.time() - t1

    print("Total time: ", str(total_time), " seconds")
    print("Done!")