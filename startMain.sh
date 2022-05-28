#!/bin/bash

python3 main.py -c "Files/D3N_201704.txt" \
                -u "172.17.0.1" \
                -p 27017 \
                -s "dsn_dsa" \
                -t "dsn_dpa" \
                -m "dsn_dmt"
