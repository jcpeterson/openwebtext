import os, sys, json, argparse
from collections import Counter

from utils import *

parser = argparse.ArgumentParser()
parser.add_argument('--psdir', type=str, default='pushshift_dumps')
parser.add_argument('--year_start', type=int, default=2018)
parser.add_argument('--year_end', type=int, default=2018)
parser.add_argument('--min_karma', type=int, default=3)
args = parser.parse_args()

filenames = []
years = range(args.year_start, args.year_end+1)
years = [str(year) for year in years]
for fn in os.listdir(args.psdir):
    for year in years:
        if year in fn: 
            filenames.append(fn)
filenames = sorted(filenames)
print('Processing the following files', 
      filenames)

good_links = []
for fn in filenames:
    
    hit_count = 0
    total_count = 0

    path = os.path.join(args.psdir, fn)
    decompress = get_decompresser(fn)

    with decompress(path, "r") as psfile:
        with open(fn+'.goodlinks.txt', 'w') as outfile:

            for line in psfile:

                j = json.loads(line)

                # only take the good links
                if (not is_bad_url(j['url'])) and \
                   (j['score'] > args.min_karma-1) and \
                   (not j['over_18']):

                    outfile.write(j['url'] + '\n')

                    hit_count += 1
                    if hit_count % 10000==0:
                        print(hit_count, total_count, hit_count/float(total_count))
                        outfile.flush()
                        os.system('xz -zkf '+fn+'.goodlinks.txt')
                total_count += 1

print(hit_count, total_count, hit_count/float(total_count))
