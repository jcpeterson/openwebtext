    import bz2

try:                # python3
    import lzma
except ImportError: # python2
    from backports import lzma

def get_decompresser(fn):
    if '.bz2' in fn:
        decompress = bz2.BZ2File
    elif '.xz' in fn:
        decompress = lzma.open
    elif '.zst' in fn:
        print('zst not implemented yet!')
        exit()
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
    'twimg.com', # twitter images
    'erome.com',
    'magaimg.net',
    # after V1 (V2)
    'flic.kr', # flickr short image urls


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

    # product websites
    'ebay.com',
    'hotdealstar.com',
    'edealinfo.com',

    # software repos
    'aapks.com',
    'dlapkandroid.org',

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
    # V2 run
    '.swf',
    '.avi',
    '.exe',
    '.css',
    '.mid',
    '.midi',
    '.mpg',
    '.mpeg',
    '.mkv',
    '.qt',
    '.ram',
    '.rar',
    '.tiff',
    '.txt',
    '.wav',
    '.aif',
    '.ogg',
    '.aif',
    '.7z',
    '.deb',
    '.tar',
    '.tar.gz',
    '.pkg',
    '.dmg',
    '.sql',
    '.jar',
    '.py',
    '.otf',
    '.ttf',
    '.bmp',
    '.ai',
    '.psd',
    '.js',
)

def is_bad_url(url):
    ext = tldextract.extract(url)
    domain = '.'.join([x for x in ext if x])
    basedomain = '.'.join(ext[-2:])

    if basedomain in exclude_domains or \
       domain in exclude_domains:
        return True

    # V1 first full URL extraction used:
    # if url.split('?')[0].endswith(exclude_extensions):
    #     return True
    # note: some lower case 'JPG' links still got through

    # V2 include upper extensions
    if url.split('?')[0].lower().endswith(exclude_extensions):
        return True

    # V2 first chars are always http:// or https://
    # need at least three more chars for real url
    if len(url) < 10:
        return True

    return False