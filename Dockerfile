FROM andimajore/miniconda3_kinetic as setup-stage

WORKDIR /usr/

RUN apt update && apt update --fix-missing && apt upgrade -y
RUN apt update && apt install -y curl unzip libcurl4-openssl-dev libssl-dev python3-dev libnss3 libnss3-dev build-essential rsync python3-pip wget git

RUN conda install python=3.9
RUN pip install --upgrade pip wheel setuptools

RUN wget https://download.java.net/java/GA/jdk15.0.1/51f4f36ad4ef43e39d0dfdbaf6549e32/9/GPL/openjdk-15.0.1_linux-x64_bin.tar.gz
RUN tar -xzf openjdk-15.0.1_linux-x64_bin.tar.gz
RUN rm openjdk-15.0.1_linux-x64_bin.tar.gz
ENV JAVA_HOME=/usr/jdk-15.0.1

RUN pip install poetry

WORKDIR /usr/app/data/nedrex_files/
RUN wget https://wolken.zbh.uni-hamburg.de/index.php/s/d6ScMNd2WHFeJtP/download/nedrex.zip --no-check-certificate && unzip nedrex.zip && rm nedrex.zip
WORKDIR /usr/app/

COPY pyproject.toml ./
RUN poetry config virtualenvs.create false && poetry install
COPY . ./

RUN python3 scripts/target_disease_script.py
RUN python3 scripts/update_import_script.py "/usr/app/biocypher-out/nedrex/neo4j-admin-import-call.sh" "/usr/app/biocypher-out/" "neo4j-admin"


FROM neo4j:4.4-enterprise as import-stage
COPY --from=setup-stage /usr/app/biocypher-out/ /var/lib/neo4j/import/
COPY docker/create_table.sh docker/import_entrypoint.sh ./
COPY docker/import.sh import/
RUN cut import_entrypoint.sh | cut - /startup/docker-entrypoint.sh > docker-entrypoint.sh && mv docker-entrypoint.sh /startup/ && chmod +x /startup/docker-entrypoint.sh
