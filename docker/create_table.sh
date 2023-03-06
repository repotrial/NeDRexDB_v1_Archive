#!/bin/bash -eu
sleep 15
echo "Starting to create the database"
cypher-shell -u neo4j -p neo4jpassword "create database nedrex"
echo "Database created!"