# NeDRexDB_v1 Archived
A quick way to set up a NeDRexDB_v1 instance using BioCypher and docker.

## ⚙️ Installation (local, for docker see below)
1. Clone this repository.
```{bash}
git clone git@github.com:repotrial/NeDRexDB_v1_Archive.git
cd NeDRexDB_v1_Archive
```
2. Configure the fields of the resulting knowledge graph. Can be found in scripts.nedrex_script.py

## 🐳 Run Docker

This repo also contains a `docker compose` workflow to create the NeDRexDB_v1 
using BioCypher and load it into a dockerised Neo4j instance
automatically. To run it, simply execute `docker compose up -d` (or `docker-compose up -d`) in the root 
directory of the project. This will build and start up a single (detached) docker
container with a Neo4j instance that contains the NeDRexDB_v1 knowledge graph built by
BioCypher as the DB `nedrex`, which you can connect to and browse at 
localhost:7474 (don't forget to switch the DB to `nedrex` instead of the 
standard `neo4j`). Authentication is set to `neo4j/neo4jpassword` by default
and can be modified in the `docker-variables.env` file.
