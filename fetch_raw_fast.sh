#!/bin/bash

export MAX_THREADS=100

main() {
	mkdir -p raw_dumps && cd url_dumps
	cat $(ls -d $PWD/* | grep ".deduped.txt") | sed -E "p;s/[\/:]/-/g" | paste -d ' ' - - | xargs -n 2 -P $MAX_THREADS -l bash -c 'curl -o ../raw_dumps/$1-$(date | tr " " "-" | tr ":" "_").html $0'
}

# Execute main function
main
