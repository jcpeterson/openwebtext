import spacy
import io
import argparse
import glob
import os
import tqdm
import contextlib
from pytorch_pretrained_bert import GPT2Tokenizer
import numpy as np
from multiprocessing import Pool, current_process
from functools import partial
import itertools

def every(it, n):
    """every(ABCDEFG, 2) --> AB CD EF G"""                                      
    toexit = False
    while not toexit:
        batch = []
        for i in range(n):
            try:
                batch.append(next(it))
            except StopIteration:
                toexit = True
        if not batch:
            break
        yield batch


def tokenizeGpt2Spawn(args, nproc=None, **kwargs):
    # Make sure output dir exists.
    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)
    extraction_file_paths = glob.iglob(args.input_glob)
    if nproc == 1:
        for batch in every(extraction_file_paths, 6):
            print(tokenizeGpt2(batch, args, **kwargs))
        return
    with Pool() as pool:
        out = pool.imap_unordered(
            partial(tokenizeGpt2, args=args, **kwargs),
            every(extraction_file_paths, args.file_bs))
        omitted, total = zip(*out)
        print(f'\n\nSkipped {sum(omitted)}/{sum(total)} files')


def tokenizeGpt2(extraction_file_paths, args, min_length=20):
    """Tokenize text using GPT-2's pretrained BPE encoder.

    Saves as compressed npz files that can be loaded using `with np.load('filename.npz') as a: a['arr_0']`.
    Omit files smaller than min_length tokens, which  are likely low quality.
    """
    tokenizer = GPT2Tokenizer.from_pretrained('gpt2')
    EOT = tokenizer.encoder['<|endoftext|>']
    omitted_files = 0
    combined = []
    p = current_process()
    index = p._identity[0] if p._identity else 0
    bar = tqdm.tqdm(extraction_file_paths, position=index, desc=f'proc {index}')
    for extraction_file_path in bar:
        _, filename = os.path.split(extraction_file_path)
        text_file = os.path.join(
            args.output_dir, filename.replace('.txt', '.tokenized.npz'))
        with io.open(extraction_file_path, 'r', encoding='utf-8') as fi:
            # Suppress warnings about length.
            with open(os.devnull, "w") as f, contextlib.redirect_stderr(f):
                # Safe to concat by adding EOT.
                out = tokenizer.encode(fi.read()) + [EOT]
            if len(out) < min_length:
                omitted_files += 1
                continue
            combined += out
        if len(combined) > args.combine:
            np.savez_compressed(text_file, combined)
            combined = []
    # Save the rest.
    if combined:
        np.savez_compressed(text_file, combined)

    return omitted_files, bar.total


def tokenizeSpacy(args):
    nlp = spacy.load('en')

    extraction_file_paths = glob.glob(args.input_glob)

    for extraction_file_path in extraction_file_paths:

        path, filename = os.path.split(extraction_file_path)
        text_file = os.path.join(
            args.output_dir, filename.replace('.txt', '.tokenized.txt'))

        fi = io.open(extraction_file_path, 'r', encoding='utf-8')
        fo = io.open(text_file, 'w', encoding='utf-8')

        omitted_line_count = 0
        for line in fi:
            if len(line) > 1:
                doc = nlp(line)
                fo.write(' '.join([x.text for x in doc]))
            else:
                omitted_line_count += 1

        fi.close()
        fo.close()

        print('Omitting '+str(omitted_line_count) + ' empty lines')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_glob', type=str, default='*.txt')
    parser.add_argument('--output_dir', type=str, default='tokenized')
    parser.add_argument('--tokenizer', type=str,
                        default='spacy', choices=['spacy', 'gpt2'])
    parser.add_argument('--combine', type=int, default=1e8, help="min tokens per file in gpt2 mode")
    parser.add_argument('--file_bs', type=int, default=10000, help="files per batch in gpt2 mode")

    args = parser.parse_args()

    if args.tokenizer == 'spacy':
        tokenizeSpacy(args)
    elif args.tokenizer == 'gpt2':
        tokenizeGpt2Spawn(args)
