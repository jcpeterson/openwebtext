import spacy
import io
import argparse
import glob
import os
import tqdm
import contextlib
from pytorch_pretrained_bert import GPT2Tokenizer
import numpy as np
from multiprocessing import Pool
from functools import partial


def batch_iter(l, bs):
    chunks = (len(l) - 1) // bs + 1
    for i in range(chunks):
        yield l[i*bs:(i+1)*bs]


def tokenizeGpt2Spawn(args, nproc=None, **kwargs):
    # Make sure output dir exists.
    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)
    extraction_file_paths = glob.glob(args.input_glob)

    if nproc == 1:
        print(tokenizeGpt2(extraction_file_paths, args, **kwargs))
        return
    with Pool() as pool:
        omitted_files = 0
        tqdm_bar = tqdm.tqdm(pool.imap_unordered(
            partial(tokenizeGpt2, args=args, **kwargs),
            batch_iter(extraction_file_paths, len(extraction_file_paths)//pool._processes)),
            total=len(extraction_file_paths))
        for o in tqdm_bar:
            omitted_files += o
            tqdm_bar.set_description(f'omit: {omitted_files}')
        print(
            f'Tokenized {tqdm_bar.total - omitted_files}/{tqdm_bar.total} files')


def tokenizeGpt2(extraction_file_paths, args, min_length=20, combine=100000):
    """Tokenize text using GPT-2's pretrained BPE encoder.

    Saves as compressed npz files that can be loaded using `with np.load('filename.npz') as a: a['arr_0']`.
    Omit files smaller than min_length tokens, which  are likely low quality.
    """
    tokenizer = GPT2Tokenizer.from_pretrained('gpt2')

    EOT = tokenizer.encoder['<|endoftext|>']
    omitted_files = 0
    combined = []
    for extraction_file_path in extraction_file_paths:
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
        if len(combined) > combine:
            np.savez_compressed(text_file, combined)
            combined = []
    # Save the rest.
    if combined:
        np.savez_compressed(text_file, combined)

    return omitted_files


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
    args = parser.parse_args()

    if args.tokenizer == 'spacy':
        tokenizeSpacy(args)
    elif args.tokenizer == 'gpt2':
        tokenizeGpt2Spawn(args)
