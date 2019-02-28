import spacy
import io
import argparse
import glob
import os

parser = argparse.ArgumentParser()
parser.add_argument('--input_glob', type=str, default='*.txt')
parser.add_argument('--output_dir', type=str, default='tokenized')
args = parser.parse_args()

nlp = spacy.load('en')

extraction_file_paths = glob.glob(args.input_glob)

for extraction_file_path in extraction_file_paths:

    path, filename = os.path.split(extraction_file_path)    
    text_file = os.path.join(args.output_dir, filename.replace('.txt','.tokenized.txt'))

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

    print('Omitting '+str(omitted_line_count)+ ' empty lines')
