#!/bin/bash -eu

bash /var/lib/neo4j/import/import.sh
bash /var/lib/neo4j/create_table.sh &
