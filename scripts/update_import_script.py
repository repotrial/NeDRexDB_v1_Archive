#!/bin/python3

import sys

IMPORT_SCRIPT = sys.argv[1]
WD = sys.argv[2]
NEO4J_ADMIN_PATH = sys.argv[3]

with open(IMPORT_SCRIPT, 'r') as fh:
    IMPORT_CALL = "\n".join(fh.readlines())

IMPORT_CALL = IMPORT_CALL.replace(WD, "/var/lib/neo4j/import/")
IMPORT_CALL = IMPORT_CALL.replace("bin/neo4j-admin", NEO4J_ADMIN_PATH)

with open(IMPORT_SCRIPT, 'w') as fh:
    fh.write(IMPORT_CALL)
