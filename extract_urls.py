import os, sys, json, argparse

from utils import *

parser = argparse.ArgumentParser()
parser.add_argument('--psdir', type=str, default='pushshift_dumps')
parser.add_argument('--outdir', type=str, default='url_dumps')
parser.add_argument('--year_start', type=int, default=2018)
parser.add_argument('--year_end', type=int, default=2018)
parser.add_argument('--single_file', type=str, default=None)
parser.add_argument('--min_karma', type=int, default=3)
parser.add_argument('--compress', action='store_true', default=False)
args = parser.parse_args()

# for processing many pushshift dumps at once
filenames = []
if args.single_file is None:
    years = range(args.year_start, args.year_end+1)
    years = [str(year) for year in years]
    for fn in os.listdir(args.psdir):
        for year in years:
            if year in fn: 
                filenames.append(fn)
    filenames = sorted(filenames)
    print('Processing the following files', 
          filenames)
else:
    # args.single_file overrides the year range
    filenames.append(args.single_file)

if not os.path.exists(args.outdir):
    os.makedirs(args.outdir)

# extract all good links and save
good_links = []
for fn in filenames:
    
    hit_count = 0
    total_count = 0

    path = os.path.join(args.psdir, fn)
    decompress = get_decompresser(fn)

    with decompress(path, "r") as psfile:
        out_path = os.path.join(args.outdir, fn+'.goodlinks.txt')
        with open(out_path, 'w') as outfile:

            for line in psfile:

                j = json.loads(line)

                # only take the good links
                if (not is_bad_url(j['url'])) and \
                   (j['score'] > args.min_karma-1) and \
                   (not j['over_18']):

                    # save the good url
                    outfile.write(j['url'] + '\n')

                    # some basic logging
                    hit_count += 1
                    if hit_count % 10000==0:
                        print(hit_count, total_count, hit_count/float(total_count))
                        # flush the output every now and then
                        outfile.flush()
                total_count += 1

# save a compressed version too
if args.compress:
    os.system('xz -zkf '+out_path)

print(hit_count, total_count, hit_count/float(total_count))
print('Done!')
