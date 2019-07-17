from __future__ import print_function
from __future__ import division

import argparse, time, tarfile
from glob import glob
from hashlib import md5
import multiprocessing as mpl
import os.path as op
import pathlib as pl

import newspaper
from tqdm import tqdm

from utils import mkdir, chunks, extract_month


parser = argparse.ArgumentParser()
parser.add_argument("--html_archive", type=str, default="scraped/RS_2017-04-4_data.xz")
parser.add_argument("--chunk_size", type=int, default=100)
parser.add_argument("--n_procs", type=int, default=100)
parser.add_argument("--output_dir", type=str, default="parsed")
args = parser.parse_args()


def parse_file(filename):
    with open(filename, "rt") as f:
        html = f.read()
        url_hash = md5(html.encode("utf-8")).hexdigest()
        article = newspaper.Article(url=url_hash, fetch_images=False)
        article.set_html(html)
        article.parse()
        return filename, article.text


def save_parsed_file(filename, text, out_dir):
    txt_fp = out_dir / filename.name
    with open(txt_fp, "wt") as handle:
        handle.write(text)


def get_processed_files(out_dir):
    parsed = glob(op.join(out_dir, "*.txt"))
    return set([op.split(f)[-1] for f in parsed])


def parse_archive(archive_fp, out_dir, n_procs, chunk_size=100):
    tmp_data_dir = pl.Path(archive_fp).with_suffix(".tmp")

    # extract tar first
    if not tmp_data_dir.exists():
        tar = tarfile.open(archive_fp)
        tar.extractall(tmp_data_dir)
        tar.close()

    # get files to process
    processed_files = set(get_processed_files(out_dir))
    num_total_files = len([_ for _ in tmp_data_dir.iterdir()])
    num_remaining_files = num_total_files - len(processed_files)
    print("{}/{} files already processed.".format(len(processed_files), num_total_files))

    def file_gen():
        for filename in tmp_data_dir.iterdir():
            if filename.name not in processed_files and filename.is_file():
                yield filename

    out_dir = pl.Path(out_dir)
    unparsable = 0

    if n_procs == 1:
        for filename in tqdm(file_gen(), total=num_remaining_files):
            filename, text = parse_file(filename)

            if not text:
                unparsable += 1
                continue

            save_parsed_file(filename, text, out_dir)
    else:
        with mpl.Pool(n_procs) as pool:
            for filename, text in tqdm(pool.imap(parse_file, file_gen(), chunksize=chunk_size), total=num_remaining_files):
                if not text:
                    unparsable += 1
                    continue

                save_parsed_file(filename, text, out_dir)
    print("Could not parse {} files".format(unparsable))


if __name__ == "__main__":
    month = extract_month(args.html_archive)
    out_dir = mkdir(op.join(args.output_dir, month))
    parse_archive(args.html_archive, out_dir, args.n_procs, args.chunk_size)
    print("Done!")
