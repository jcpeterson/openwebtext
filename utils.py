import bz2
try:                # python3
    import lzma
except ImportError: # python2
    from backports import lzma

def get_decompresser(fn):
    if '.bz2' in fn:
        decompress = bz2.BZ2File
    elif '.xz' in fn:
        decompress =  lzma.open
    return decompress

# the below is adapted from:
# https://github.com/eukaryote31/openwebtext/blob/master/filter.py

import tldextract

# domains that aren't scraper friendly. do not include subdomains!
exclude_domains = set([

    # image, video, and music hosting sites
    'imgur.com',
    'redd.it',
    'gfycat.com',
    'giphy.com',
    'reddituploads.com',
    'redditmedia.com',
    'twimg.com',
    'sli.mg',
    'magaimg.net',
    'flickr.com',
    'imgflip.com',
    'youtube.com',
    'youtu.be',
    'youtubedoubler.com',
    'vimeo.com',
    'twitch.tv',
    'streamable.com',
    'bandcamp.com',
    'soundcloud.com',
    'video.google.com',
    'instagram.com',
    'deviantart.com',
    'itunes.apple.com',
    'pornhub.com',
    'pinterest.com',

    # not scraper friendly
    'reddit.com',
    'gyazo.com',
    'github.com',
    'xkcd.com',
    'twitter.com',
    'spotify.com',
    'facebook.com',
    'gunprime.com',
    'strawpoll.me',
    'voyagefusion.com',
    'rollingstone.com',
    'google.com',
    'timeanddate.com',
    'walmart.com',
    'roanoke.com',
    'spotrac.com',
    'discord.gg',

    # other non-text content
    'ebay.com',

    # remove these?
    # 'reverb.com',

    # original paper excluded wikipedia
    'wikipedia.org',

    # lots of top posts for this one
    'battleforthenet.com',
])

exclude_extensions = (
    '.png',
    '.jpg',
    '.jpeg',
    '.gif',
    '.gifv',
    '.pdf',
    '.mp4',
    '.mp3',
    '.ogv',
    '.webm',
    '.doc',
    '.docx',
    '.log',
    '.csv',
    '.dat',
    '.iso',
    '.bin',
    '.exe',
    '.apk',
    '.jar',
    '.app',
    '.ppt',
    '.pps',
    '.pptx',
    '.xml',
    '.gz',
    '.xz',
    '.bz2',
    '.tgz',
    '.tar',
    '.zip',
    '.wma',
    '.mov',
    '.wmv',
    '.3gp',
    '.svg',
)

def is_bad_url(url):
    ext = tldextract.extract(url)
    domain = '.'.join([x for x in ext if x])
    basedomain = '.'.join(ext[-2:])

    if basedomain in exclude_domains or \
       domain in exclude_domains:
        return True

    if url.split('?')[0].endswith(exclude_extensions):
        return True

    return False