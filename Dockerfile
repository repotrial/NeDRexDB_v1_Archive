FROM andimajore/biocyper_base:python3.10 as setup-stage

RUN apt install unzip

WORKDIR /usr/app/data/nedrex_files/
RUN wget https://wolken.zbh.uni-hamburg.de/index.php/s/d6ScMNd2WHFeJtP/download/nedrex.zip --no-check-certificate && unzip nedrex.zip && rm nedrex.zip
WORKDIR /usr/app/

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false && poetry install
COPY . ./

RUN python3 scripts/nedrex_script.py

FROM neo4j:4.4-enterprise as deploy-stage
COPY --from=setup-stage /usr/app/biocypher-out/ /var/lib/neo4j/import/
COPY docker/* ./
RUN cat biocypher_entrypoint_patch.sh | cat - /startup/docker-entrypoint.sh > docker-entrypoint.sh && mv docker-entrypoint.sh /startup/ && chmod +x /startup/docker-entrypoint.sh