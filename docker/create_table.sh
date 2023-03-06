sleep 15
echo "Starting to create the database"
cypher-shell -u $NEO4J_USER -p $NEO4J_PASSWORD "create database nedrex"
echo "Database created!"