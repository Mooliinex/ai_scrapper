#!/usr/bin/env bash
set -e
SINCE=2015-05-01
UNTIL=$(date +%F)
OUTRAW=data/raw
OUTCLEAN=data/clean/corpus.csv

python harvest.py --since $SINCE --until $UNTIL --config config.yaml --out $OUTRAW
python clean_merge.py --config config.yaml --raw_dir $OUTRAW --out $OUTCLEAN --extract-text
