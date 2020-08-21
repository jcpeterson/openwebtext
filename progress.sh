#!/bin/bash
echo "print('{:.2f} minutes'.format(int(" $(date +%s%N) - $(cat start-time.txt) ") / 1e9 / 60));" "print(int(" $(du -s raw_dumps | cut -f1) "), 'bytes\n', int(" $(ls -1 raw_dumps | wc -l) "), 'files')" | python3
