from __future__ import print_function

import os, glob, argparse, tarfile, pickle

parser = argparse.ArgumentParser()
parser.add_argument("--html_dir", type=str, default="scraped")
parser.add_argument("--save_pickle", type=str, default="stats.p")
args = parser.parse_args()

pickle_needs_update = False

# try/except is safer than isfile()
try:
    saved = pickle.load(open(args.save_pickle, 'rb'))
    print('Previously calculated stats loaded successfully.')
except:
    saved = {}
    print('No previous stats found. Computing from scratch.')
    pickle_needs_update = True

# grab paths for all html data files
query = os.path.join(args.html_dir, '*.xz')
paths = glob.glob(query)
paths = sorted([p for p in paths if 'data' in p])

total_count = 0
for path in paths:

    if path in saved.keys():
        print('Already have stats for {}; loading...'.format(path))
        count = saved[path]
    else:
        print('No stats found for {}; computing...'.format(path))
        with tarfile.open(path) as tf:
            count = len(tf.getnames())
        saved[path] = count
        pickle_needs_update = True

    total_count += count
    print('-- {} contains {} files'.format(path, count))

print('\nTotal Document Count:', total_count, '\n')

# save updated pickle if needed
if pickle_needs_update:
    pickle.dump(saved, open(args.save_pickle, 'wb'))
    print('Updated stats saved to {}'.format(args.save_pickle))
else:
    print('Nothing new to compute or save.')