#!/bin/bash
mkdir raw_dump && cd raw_dump
cat $(ls ../urldumps | grep ".deduped.txt") | xargs curl $1 -o $1-$(date +"%T").html
