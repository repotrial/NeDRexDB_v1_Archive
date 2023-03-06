#!/bin/bash -eu

bash import/import.sh
bash wait-for-it.sh localhost:7687 --timeout=30 -- bash create_table.sh &
