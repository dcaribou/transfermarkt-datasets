#!/bin/bash

python acquire.py --asset all

(mkdir -p data/prep && cd prep && python prep.py --ignore-checks --raw-files-location ../data/raw && cp stage/* ../data/prep)
